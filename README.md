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

### Example using HiveServer2
```
// const client = require("hs2-thrift");  // Use this if example.js is outside hs2-thrift package e.g. used 'npm install hs2-thrift'
const client = require("../index.js");

const config = {
  host: 'example.com.au',   // Change to correspond with your config
  port: 1234,               // Change to correspond with your config
  username: '',             // Change to correspond with your config
  password: ''              // Change to correspond with your config
}

var sqlQuery = "select * from db.table";  // Example query

const impalaQuery = (config, sqlQuery) => {

  /* Open session and connection */
  client.connect(config, function(error, session) {
    console.log("Attempting to connect to: " + config.host + ":" + config.port)
    if (error) {
      console.log("Hive connection error : " + error);
      process.exit(1);
    } else {
      client.query(session, sqlQuery, function(error, result) {
        if (error) {
          console.log("SQL Query error\n" + error + session);
        } else {
          console.log(sqlQuery + " => \n" + JSON.stringify(result));
  
          /*Close the session and the connection*/
          client.disconnect(session, function(error, res) {
            if (error) {
              console.log("Disconnection error : " + JSON.stringify(error));
              process.exit(1);  // Exit with failure code
            } else {
              console.log("Disconnected successfully.");
              process.exit(0);
            }
          });
        }
      });
    }
  });
}

impalaQuery(config, sqlQuery);
```

### Example output / result
```
PS C:\dev\javascript_projects\hs2-thrift-example> node .\main.js
Attempting to connect to: ***.***.com.au:****
select * from default.temp => 
[[{"col1":5},{"col2":6}],[{"col1":1},{"col2":2}],[{"col1":3},{"col2":4}]]
Disconnected successfully.
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
This project is largely based off of the work done by user SistemaStrategy (https://github.com/SistemaStrategy/HiveThrift), with my own modifications and improvements to the code and result output. More changes and improvements will be made to my version in the future.