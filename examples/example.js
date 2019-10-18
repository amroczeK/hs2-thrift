// const client = require("hs2-thrift");  // Use this if example.js is outside hs2-thrift package e.g. used 'npm install hs2-thrift'
const client = require("../index.js");

const config = {
  host: 'example.com.au',   // Change to correspond with your config
  port: 1234,               // Change to correspond with your config
  username: '',             // Change to correspond with your config
  password: ''              // Change to correspond with your config
}

var sqlQuery = "select * from default.temp";  // 

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

getSession(config).then((session) => {
	sendQuery(session).then((result) => {
		console.log("Result: " + sqlQuery + " => \n" + JSON.stringify(result));
		endSession(session).then((response) => {
			console.log("Disconnected from server and closed session successfully.")
			process.exit(0)
		}).catch((error) => {
			console.log("Disconnect and end session error : " + JSON.stringify(error))
			process.exit(1)
		})

	}).catch((error) => {
		console.log("\nSQL Query error\n" + error + session);
	})
}).catch((error) => {
	console.log("\nHive connection error : " + error);
})
