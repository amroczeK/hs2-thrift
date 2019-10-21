// const client = require("hs2-thrift");  // Use this if example.js is outside hs2-thrift package e.g. used 'npm install hs2-thrift'
const client = require("../index.js");

const config = {
  host: 'example.com.au',   // Change to correspond with your config
  port: 1234,               // Change to correspond with your config
  username: '',             // Change to correspond with your config
  password: ''              // Change to correspond with your config
}

var sqlQuery = "select * from default.temp";  // Change this query to suit your db/table

async function queryImpala(){
	try {
		const session = await client.connect(config);
		console.log("Session created.")
		const result = await client.query(session, sqlQuery);
		console.log("Result: " + sqlQuery + " => \n" + JSON.stringify(result));
		await client.disconnect(session);
		console.log("Disconnected from server and closed session successfully.")
		process.exit(0)
	} catch(error) {
        console.log("Error: " + JSON.stringify(error))
        process.exit(1)
	}
}
queryImpala()