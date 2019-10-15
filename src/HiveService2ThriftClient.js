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
	name: "HiveService2ThriftClient",
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

/* HiveService2ThriftClient class default constructor */
function HiveService2ThriftClient() {}

/* Connect to database and create thrift client
  - callback : callback(error, session) function
  - error : error
  - session : opened session
*/
HiveService2ThriftClient.prototype.connect = function connect(config, callback) {
	logger.info(
		"Attempting to connect to" + config.host + "on port " + config.port
	);

	/* Create a connection and thrift client */
	connection = thrift.createConnection(config.host, config.port, {
		transport: transport,
		protocol: protocol
	});

	client = thrift.createClient(service, connection);

	/* Handle connection errors */
	connection.on("error", function(error) {
		logger.error("Failed to make a thrift connection : " + error);
		callback(error, null);
	});

	/* Handle connection success */
	connection.on("connect", function() {
		logger.info(
			"Connection initialised for " + config.host + ":" + config.port
		);

		openSessionThrift(config, function(error, response, protocol) {
			if (error) {
				logger.error(
					"Failed to open session (connection) on the server against which operations may be executed : " +
						error
				);
			} else {
				logger.info(
					"Session opened with HiveServer2 protocol value : " +
						protocol
				);
			}
			callback(error, response.sessionHandle);
		});
	});
};

/* Disconnect from database
  - session : The opened session
  - callback : callback(status) function
    - status : status
*/
HiveService2ThriftClient.prototype.disconnect = function disconnect(
	session,
	callback
) {
	/*Closing hive session*/
	closeSessionThrift(session, function(status) {
		if (status) {
			logger.error(
				"Failed to close thrift session : " + JSON.stringify(status)
			);
		} else {
			logger.info("Thrift session closed.");
		}

		/*Handle disconnect success*/
		connection.on("end", function(error) {
			if (error) {
				logger.error(
					"Disconnecting connection encountered an error : " + error
				);
			} else {
				logger.info("Connection disconnected successfully.");
			}
		});

		/*Closing thrift connection*/
		connection.end();
		logger.info("Connection disconnected successfully.");
		callback(status);
	});
};

/* Execute a select statement
  - session : The operation to fetch rows from
  - statement : The statement to be executed
  - callback : callback(error, result) function
    - error : error
    - result : result of the executeStatementThrift
*/
HiveService2ThriftClient.prototype.query = function query(
	session,
	selectStatement,
	callback
) {
	logger.info("SQL Query : " + selectStatement);

	executeStatementThrift(session, selectStatement, function(error, response) {
		if (error) {
			logger.error("SQL Query error = " + JSON.stringify(error));
			callback(error, response);
		} else {
			logger.info("SQL Query sent successfully, getting results.");
			getResults(response.operationHandle, function(error, response) {
				callback(error, response);
			});
		}
	});
};

/* Open Hive session */
function openSessionThrift(config, callback) {
	var protocol = serviceTypes.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V5;
	var openSessReq = new serviceTypes.TOpenSessionReq();
	openSessReq.username = config.username;
	openSessReq.password = config.password;
	openSessReq.client_protocol = protocol;
	client.OpenSession(openSessReq, function(error, response) {
		callback(error, response, protocol);
	});
}

/* Close Hive session */
function closeSessionThrift(session, callback) {
	var closeSessReq = new serviceTypes.TCloseSessionReq();
	closeSessReq.sessionHandle = session;
	client.CloseSession(closeSessReq, function(error, response) {
		callback(error, response);
	});
}

/* Execute SQL Statement */
function executeStatementThrift(session, statement, callback) {
	var request = new serviceTypes.TExecuteStatementReq();
	request.sessionHandle = session;
	request.statement = statement;
	request.runAsync = false;
	client.ExecuteStatement(request, function(error, response) {
		callback(error, response);
	});
}

/* Execute GetResultSetMetadata */
function getResultSetMetadataThrift(operation, callback) {
	var request = new serviceTypes.TGetResultSetMetadataReq();
	request.operationHandle = operation;
	client.GetResultSetMetadata(request, function(error, response) {
		callback(error, response);
	});
}

/* Execute FetchResults action on operation result */
function fetchResultsThrift(operation, callback) {
	var request = new serviceTypes.TFetchResultsReq();
	request.operationHandle = operation;
	request.orientation = serviceTypes.TFetchOrientation.FETCH_NEXT;
	request.maxRows = 2000; // Hardcoded maximum number of rows able to be returned
	client.FetchResults(request, function(error, response) {
		callback(error, response);
	});
}

/* Retrieve and parse the response, returning a result */
function getResults(operation, callback) {
	getResultSetMetadataThrift(operation, function(error, responseMeta) {
		if (error) {
			callback(error, null);
		} else {
			fetchResultsThrift(operation, function(error, responseFetch) {
				if (error) {
					callback(error, null);
				} else {
					var columnNames = responseMeta.schema.columns.map(
						column => column.columnName
					);
					/* Format result */
					const result = responseFetch.results.rows.map(row =>
						row.colVals.map((col, c) => ({
							[columnNames[c]]: Object.values(col).filter(
								val => val
							)[0].value
						}))
					);
					logger.info("SQL Query result : " + result);
					callback(error, result);
				}
			});
		}
	});
}

module.exports = new HiveService2ThriftClient();
