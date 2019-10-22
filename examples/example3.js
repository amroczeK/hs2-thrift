// const client = require("hs2-thrift");  // Use this if example.js is outside hs2-thrift package e.g. used 'npm install hs2-thrift'
const client = require("../index.js");

const config = {
  host: 'example.com.au',   // Change to correspond with your config
  port: 1234,               // Change to correspond with your config
  username: '',             // Change to correspond with your config
  password: '',             // Change to correspond with your config
  protocol_ver: 5,			// Version 1 - 11. Change to suit your HS2 Protocol Version, defaults to V5
  retain_session: false		// true - will NOT close connection and session
							// false - will close connection and session
}

var sqlQuery = "select * from default.temp";  // Change this query to suit your db/table

function getSession(config) {
	return new Promise((resolve, reject) => {
		console.log("Attempting to connect to: " + config.host + ":" + config.port)
		client.connect(config).then((session) => {
			resolve(session)
		}).catch((error) => {
			reject(error)
		}) 
	})
}

function endSession(session) {
	return new Promise((resolve, reject) => {
		console.log("Attempting to disconnect from server and close session.")
		client.disconnect(session).then((response) => {
			resolve(response)
		}).catch((error) => {
			reject(error)
		})
	})
}

function sendQuery(session) {
	return new Promise((resolve, reject) => {
		console.log("Sending query: " + sqlQuery)
		client.query(session, sqlQuery).then((result) => {
			resolve(result)
		}).catch((error) => {
			reject(error)
		})
	})
}

async function queryImpala(){
	try {
		const session = await getSession(config);
		console.log("Session created.")
		const result = await sendQuery(session);
		console.log("Result: " + sqlQuery + " => \n" + JSON.stringify(result));
		const response = await endSession(session);
		console.log("Disconnected from server and closed session successfully. " + JSON.stringify(response))
		process.exit(0)
	} catch(error) {
		console.log(JSON.stringify(error))
	}
}
queryImpala()
