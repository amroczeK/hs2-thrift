# ImpalaThrift
Impala connector using HiveServer2 Thrift Service.

## Getting Started
* Update the config.json file with your settings

### Install the npm packages detailed in package.json
```
npm install 
```

### Example using HiveServer2
```
const client = require("../index.js");

const config = {
  host: 'example.com.au',
  port: 1234,
  username: '',
  password: ''
}

var sqlQuery = "select * from db.table";  // 

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

### Running the example
```
node example.js
```

### Troubleshooting errors
```
Hive connection error : TProtocolException: Required field operationHandle is unset!
// Make sure that the SQL query you are sending is valid and connecting to the correct DB.
```