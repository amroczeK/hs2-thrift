# Hive Server 2 Thrift
Hive Server 2 client using Apache Thrift RPC able to query Impala written in Javascript.

NOTE: There is currently very limited functionality, it will only query the DB and return a result. More functionality will be added later if required for my project.

HiveServer2 Thrift protocol used fined in latest TCLIService.thrift: https://github.com/apache/hive/blob/master/service-rpc/if/TCLIService.thrift

## Getting Started

### Prerequisites
* Nodejs => 10.16.3
* npm
* git
* You know your HiveServer2 Protol Version (package uses V5 as default)

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
### Example 1: 
```
// const client = require("hs2-thrift");  // Use this if example.js is outside hs2-thrift package e.g. used 'npm install hs2-thrift'
const client = require("../index.js");

const config = {
    host: 'example.com.au',   // Change to correspond with your config
    port: 1234,               // Change to correspond with your config
    username: '',             // Change to correspond with your config
    password: '',             // Change to correspond with your config
    protocol_ver: 5,		  // Version 1 - 11. Change to suit your HS2 Protocol Version, defaults to V5
    retain_session: false	  // true - will NOT close connection and session
                              // false - will close connection and session
							  // Default: false, connection and session will close unless specified
  }

var sqlQuery = "select * from default.temp";  // Change this query to suit your db/table

client.connectAndQuery(config, sqlQuery).then((result) => {
	console.log("Result: " + sqlQuery + " => \n" + JSON.stringify(result));
	// if retain_session == true, connection & session will remain active, process will not close
	if (!config.retain_session) {
		process.exit(0);
	}
}).catch((error) => {
	console.log(JSON.stringify(error))
	process.exit(1)
})
```
### Example 2:
```
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