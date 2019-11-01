/*
 * @author	Adrian Mroczek
 * @version 1.0.8
 * @github	https://github.com/amroczeK/hs2-thrift
 * @npm		https://www.npmjs.com/package/hs2-thrift
 */

"use strict";
const thrift = require("thrift"),
	service = require("../lib/gen-nodejs/TCLIService"),
	serviceTypes = require("../lib/gen-nodejs/TCLIService_types"),
	bunyan = require("bunyan");

var connection, client;
const transport = thrift.TBufferedTransport;
const protocol = thrift.TBinaryProtocol;

/* Create bunyan logger for logging */
const logger = bunyan.createLogger({
	name: "HiveServer2ThriftClient",
	streams: [
		{
			level: "info",
			path: "./node_modules/hs2-thrift/logs/hive-thrift-logs.log"
		},
		{
			level: "error",
			path: "./node_modules/hs2-thrift/logs/hive-thrift-logs.log"
		}
	]
});

/* HiveServer2ThriftClient class default constructor */
function HiveServer2ThriftClient() {}

/*
 *	Connect to database and create thrift client
 *	@param config - the server configuration
 *	@return a promise with session or error
 */
HiveServer2ThriftClient.prototype.connect = function connect(config) {
	logger.info("Attempting to connect to: " + config.host + ":" + config.port);

	return new Promise((resolve, reject) => {
		/* Create a connection and thrift client */
		establishConnection(config)
			.then(session => {
				resolve(session);
			})
			.catch(error => {
				reject(error);
			});
	});
};

/*
 *	Disconnect from the server and close thrift session
 *	@param session - the current thrift session
 *	@return a promise with response or error
 */
HiveServer2ThriftClient.prototype.disconnect = function disconnect(session) {
	return new Promise((resolve, reject) => {
		closeConnection(session)
			.then(response => {
				resolve(response);
			})
			.catch(error => {
				reject(error);
			});
	});
};

/*
 *	Execute a select statement
 *	@param session - the current thrift session
 *	@param statement - SQL statement/query
 *	@return a promise with result or error
 */
HiveServer2ThriftClient.prototype.query = function executeStatement(
	session,
	statement
) {
	return new Promise((resolve, reject) => {
		executeQuery(session, statement)
			.then(result => {
				resolve(result);
			})
			.catch(error => {
				reject(error);
			});
	});
};

/*
 *	Create a connection, session and execute SQL query
 *	@param session - the current thrift session
 *	@param statement - SQL statement/query
 *	@return a promise with response or error
 */
HiveServer2ThriftClient.prototype.connectAndQuery = function connectAndQuery(
	config,
	statement
) {
	logger.info("Sending SQL Query : " + statement);
	return new Promise((resolve, reject) => {
		establishConnection(config)
			.then(session => {
				executeQuery(session, statement)
					.then(result => {
						switch (config.retain_session) {
							case true:
								logger.info("Connection and session remains alive.");
								break;
							default:
								closeConnection(session)
									.then(() => {
										resolve(result);
									})
									.catch(error => {
										reject(error);
									});
						}
					})
					.catch(error => {
						closeConnection(session)
									.then(() => {
										reject(error)
									})
									.catch(error => {
										reject(error);
									});
					});
			})
			.catch(error => {
				reject(error);
			});
	});
};

/*
 *	Connect to database and create thrift client
 *	@param config - the server configuration
 *	@return a promise with session or error
 */
const establishConnection = config => {
	return new Promise((resolve, reject) => {
		/* Create a connection and thrift client */
		connection = thrift.createConnection(config.host, config.port, {
			transport: transport,
			protocol: protocol
		});

		client = thrift.createClient(service, connection);

		/* Handle connection errors */
		connection.on("error", function(error) {
			logger.error(
				"Failed to make a thrift connection : " + JSON.stringify(error)
			);
			if (error) {
				reject(error);
			}
		});

		/* Handle connection success */
		connection.on("connect", function() {
			logger.info(
				"Connection initialised for " + config.host + ":" + config.port
			);
			openSessionThrift(config)
				.then(response => {
					logger.info("Session opened successfully.");
					resolve(response.sessionHandle);
				})
				.catch(error => {
					logger.error(
						"Failed to make a connection with server : " +
							JSON.stringify(error)
					);
					reject(error);
				});
		});
	});
};

/*
 *	Close connection to the server and close thrift session
 *	@param session - the current thrift session
 *	@return a promise with response or error
 */
const closeConnection = session => {
	return new Promise((resolve, reject) => {
		closeSessionThrift(session)
			.then(response => {
				logger.info("Thrift session closed successfully.");
				connection.on("end", function(error) {
					if (error) {
						logger.error(
							"Disconnecting connection encountered an error : " +
								JSON.stringify(error)
						);
						reject(error);
					}
				});
				connection.end();
				logger.info("Connection disconnected successfully.");
				resolve(response);
			})
			.catch(error => {
				logger.error(
					"Failed to close thrift session : " + JSON.stringify(error)
				);
				reject(error);
			});
	});
};

/*
 * 	Execute query against database
 *	@param session - the current thrift session
 *	@param statement - SQL statement/query
 *	@return a promise with result or error
 */
const executeQuery = (session, statement) => {
	return new Promise((resolve, reject) => {
		logger.info("Executing SQL Query : " + statement);
		executeStatementThrift(session, statement)
			.then(response => {
				getResults(response.operationHandle)
					.then(result => {
						resolve(result);
					})
					.catch(error => {
						reject(error);
					});
			})
			.catch(error => {
				logger.error(
					"Failed to execute statement with error : " +
						JSON.stringify(error)
				);
				reject(error);
			});
	});
};

/*
 * 	Open Hive/Thrift session
 *	@param config - the server configuration
 *	@return a promise with response or error
 */
const openSessionThrift = config => {
	return new Promise((resolve, reject) => {
		var protocol = setProtocolVersion(config);
		var openSessReq = new serviceTypes.TOpenSessionReq();
		openSessReq.username = config.username;
		openSessReq.password = config.password;
		openSessReq.client_protocol = protocol;
		client.OpenSession(openSessReq, function(error, response) {
			if (!error) {
				resolve(response);
			} else {
				reject(error);
			}
		});
	});
};

/*
 *	Set protocol to appropriate HiveServer2 Protocol Version
 *	@param config - the server configuration
 *	@return protocol for HiveServer2 Thrift Service version
 */
const setProtocolVersion = config => {
	var protocol;
	switch (config.protocol_ver) {
		case 1:
			protocol =
				serviceTypes.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1;
			break;
		case 2:
			protocol =
				serviceTypes.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V2;
			break;
		case 3:
			protocol =
				serviceTypes.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V3;
			break;
		case 4:
			protocol =
				serviceTypes.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V4;
			break;
		case 5:
			protocol =
				serviceTypes.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V5;
			break;
		case 6:
			protocol =
				serviceTypes.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6;
			break;
		case 7:
			protocol =
				serviceTypes.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V7;
			break;
		case 8:
			protocol =
				serviceTypes.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8;
			break;
		case 9:
			protocol =
				serviceTypes.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V9;
			break;
		case 10:
			protocol =
				serviceTypes.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10;
			break;
		case 11:
			protocol =
				serviceTypes.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V11;
			break;
		default:
			protocol =
				serviceTypes.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V5;
	}
	return protocol;
};

/*
 *	Close Hive/Thrift session
 *	@param session - the current thrift session
 *	@return a promise with response or error
 */
const closeSessionThrift = session => {
	return new Promise((resolve, reject) => {
		var closeSessReq = new serviceTypes.TCloseSessionReq();
		closeSessReq.sessionHandle = session;
		client.CloseSession(closeSessReq, function(error, response) {
			if (!error) {
				resolve(response);
			} else {
				reject(error);
			}
		});
	});
};

/*
 *	Execute SQL Statement
 *	@param session - the current thrift session
 *	@param statement - SQL statement/query
 *	@return a promise with response or error
 */
const executeStatementThrift = (session, statement) => {
	return new Promise((resolve, reject) => {
		var request = new serviceTypes.TExecuteStatementReq();
		request.sessionHandle = session;
		request.statement = statement;
		request.runAsync = false;
		client.ExecuteStatement(request, function(error, response) {
			if (!error) {
				resolve(response);
			} else {
				reject(error);
			}
		});
	});
};

/*
 *  Execute GetResultSetMetadata
 *	@param operation - the current operation
 *	@return a promise with response or error
 */
const getResultSetMetadataThrift = operation => {
	return new Promise((resolve, reject) => {
		var request = new serviceTypes.TGetResultSetMetadataReq();
		request.operationHandle = operation;
		client.GetResultSetMetadata(request, function(error, response) {
			if (!error) {
				resolve(response);
			} else {
				reject(error);
			}
		});
	});
};

/*
 *	Execute FetchResults action on operation result
 *	@param operation - the current operation
 *	@return a promise with response or error
 */
const fetchResultsThrift = operation => {
	return new Promise((resolve, reject) => {
		var request = new serviceTypes.TFetchResultsReq();
		request.operationHandle = operation;
		request.orientation = serviceTypes.TFetchOrientation.FETCH_NEXT;
		request.maxRows = 2000; // Hardcoded maximum number of rows able to be returned
		client.FetchResults(request, function(error, response) {
			if (!error) {
				resolve(response);
			} else {
				reject(error);
			}
		});
	});
};

/*
 *	Retrieve and parse the response
 *	@param operation - the current operation
 *	@return a promise with response (result) or error
 */
const getResults = operation => {
	logger.info("Getting results from SQL query.");
	return new Promise((resolve, reject) => {
		getResultSetMetadataThrift(operation)
			.then(responseMeta => {
				logger.info(
					"Get ResultSet Metadata Response = " +
						JSON.stringify(responseMeta)
				);
				fetchResultsThrift(operation)
					.then(responseFetch => {
						logger.info(
							"Fetch Results Response = " +
								JSON.stringify(responseFetch)
						);
						var columnNames = responseMeta.schema.columns.map(
							column => column.columnName
						);
						/* Format result returning columns and rows */
						const result = responseFetch.results.rows.map(row =>
							row.colVals.map((col, c) => ({
								[columnNames[c]]: Object.values(col).filter(
									val => val
								)[0].value
							}))
						);
						resolve(result);
					})
					.catch(error => {
						logger.error("\nError fetching results : " + error);
						reject(error);
					});
			})
			.catch(error => {
				logger.error("\nError getting ResultSetMetadata : " + error);
				reject(error);
			});
	});
};

module.exports = new HiveServer2ThriftClient();
