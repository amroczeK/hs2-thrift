# Hive Server 2 Thrift
Hive Server 2 client using Apache Thrift RPC able to query Impala written in Javascript.

NOTE: There is currently very limited functionality, it will only query the DB and return a result. More functionality will be added later if required for my project.

HiveServer2 Thrift protocol used fined in latest TCLIService.thrift: https://github.com/apache/hive/blob/master/service-rpc/if/TCLIService.thrift

## Getting Started

### Prerequisites
* Nodejs => 10.16.3
* npm
* git

### Option 1: Install the npm package and refer to example for usage
```
npm install hs2-thrift 
```

### Option 2: Clone git repo and install the npm packages detailed in package.json
```
git clone https://github.com/amroczeK/hs2-thrift.git hs2-thrift
cd hs2-thrift
npm install 
```

### Example using hs2-thrift
```
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

```

### Example output / result
```
PS C:\dev\javascript_projects\hs2-thrift-example> node .\main.js
Sending query: select * from default.temp
Result: select * from default.temp =>
[[{"col1":1},{"col2":2}],[{"col1":5},{"col2":6}],[{"col1":3},{"col2":4}]]
Attempting to disconnect from server and close session.
Disconnected from server and closed session successfully.
```

### Running the example
```
node example.js
```

### Troubleshooting errors
```
Hive connection error : TProtocolException: Required field operationHandle is unset!
// Make sure that the SQL query you are sending is valid and connecting to the correct DB.
```

#### Credits
This package was developed with the help of the hive thrift packaged developed by user SistemaStrategy https://github.com/SistemaStrategy/HiveThrift, with my own modifications, simplifications and improvements to the code and result output.