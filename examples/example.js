// const client = require("hs2-thrift");  // Use this if example.js is outside hs2-thrift package e.g. used 'npm install hs2-thrift'
const client = require("../index.js");

const config = {
	host: "example.com.au", // Change to correspond with your config
	port: 1234, // Change to correspond with your config
	username: "", // Change to correspond with your config
	password: "", // Change to correspond with your config
	protocol_ver: 5, // Version 1 - 11. Change to suit your HS2 Protocol Version, defaults to V5
	retain_session: false // true - will NOT close connection and session
	// false - will close connection and session
};

var sqlQuery = "select * from default.temp"; // Change this query to suit your db/table

client
	.connectAndQuery(config, sqlQuery)
	.then(result => {
		console.log("Result: " + sqlQuery + " => \n" + JSON.stringify(result));
		// if retain_session == true, connection & session will remain active, process will not close
		if (!config.retain_session) {
			process.exit(0);
		}
	})
	.catch(error => {
		console.log(JSON.stringify(error));
		process.exit(1);
	});
