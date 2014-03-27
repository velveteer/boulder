/*jshint esnext: true*/
var co = require('co');
var mssql = require('co-mssql');
var r = require('co-rethinkdb');
var fs = require('fs');
var moment = require('moment');
var _s = require('underscore.string');

/* Connect to Rethink and remove need for run() */
var conn;
r.getConnection = function* () {
  return conn || (conn = yield r.connect({ host: 'localhost', port: 28015}));
};

/* Read SQL files into memory */
var chatSql = fs.readFileSync('chats.sql', 'utf-8');
var tranSql = fs.readFileSync('transcripts.sql', 'utf-8');

/* Set current date so we can pull chats daily */
var day = moment().format("DD");
var month = moment().format("MM");
var year = moment().format("YYYY");

co(function* () {

/* Set up databases */
  yield r.table('chats').delete();
  yield r.table('transcripts').delete();
  var connection = new mssql.Connection({
    user: process.env.DW_USER,
    password: process.env.DW_PASS,
    server: process.env.DW_URL,
    database: process.env.DW_DB,
  });

  try {

    /* Connect to MSSQL */
    yield connection.connect();

    var request = new mssql.Request(connection);
    /* Inject today's date into MSSQL query */
    request.input('day', mssql.VarChar, day);
    request.input('month', mssql.VarChar, month);
    request.input('year', mssql.VarChar, year);
    /* Execute query and return set of records */
    var recordset = yield request.query(chatSql);
    var results = [];
    var transArr = [];
    /* Iterate over records, insert them into Rethink
       Then push the insert results into an array (returnVals: true) */
    for (var record in recordset) {
      try { 
        results.push(yield r.table('chats').insert(recordset[record],{returnVals: true}));
        process.stdout.write("Inserting chat " + recordset[record].chatID + "\r");
      } catch (ex) { console.dir(ex.err); }
    }
    
    console.log("\nInserted " + results.length + " chats.");

    var requestTrans = new mssql.Request(connection);
    /* Iterate over Rethink results array
       Execute MSSQL query to pull transcripts for each result chatID */
    for (var i in results) {
      var chatID = results[i].new_val.chatID;
      process.stdout.write("Pulling transcripts for " + chatID + "\r");
      requestTrans.input('chatID', mssql.VarChar, chatID);
      var tranSet = yield requestTrans.query(tranSql);
     /* Push each MSSQL row returned into an array
        Strip HTML from transcript text */
      for (var tran in tranSet) {
        var text = tranSet[tran].text;
        text = _s.unescapeHTML(text);
        text = _s.stripTags(text);
        tranSet[tran].text = text;
        transArr.push(tranSet[tran]);
      }
    }
    console.log("\nBulk inserting transcripts.");
    try { yield r.table('transcripts').insert(transArr); }
    catch (ex) { console.dir(ex); }

    console.log("Inserted " + transArr.length + " chat messages.");

  } catch (ex) { console.dir(ex); 
  } finally {
    connection.close();
    conn.close();
  }
})();
