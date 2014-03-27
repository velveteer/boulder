/*jshint esnext: true */
var _s = require('underscore.string');
var co = require('co');
var fs = require('fs');
var moment = require('moment');
var monk = require('monk');
var mssql = require('co-mssql');
var wrap = require('co-monk');

/* Initialize MongoDB connection */
var db = monk('localhost/test');

/* Set up chats collection */
var chats = wrap(db.get('chats'));
chats.ensureIndex( { chatID: 1 }, { unique: true } );
chats.remove();

/* Set up transcripts collection */
var transcripts = wrap(db.get('transcripts'));
transcripts.ensureIndex( { messageID: 1 }, { unique: true } );
transcripts.remove();

/* Load SQL into memory */
var chatSql = fs.readFileSync('chats.sql', 'utf-8');
var tranSql = fs.readFileSync('transcripts.sql', 'utf-8');

/* Grab date so we can pull chats per day */
var day = moment().format("DD");
var month = moment().format("MM");
var year = moment().format("YYYY");

co(function* () {
  /* Initialize MSSQL connection */
  var connection = new mssql.Connection({
    user: process.env.DW_USER,
    password: process.env.DW_PASS,
    server: process.env.DW_URL,
    database: process.env.DW_DB,
  });

  try {

    yield connection.connect();

    var request = new mssql.Request(connection);
    /* Inject date into MSSQL query */
    request.input('day', mssql.VarChar, day);
    request.input('month', mssql.VarChar, month);
    request.input('year', mssql.VarChar, year);

    var recordset = yield request.query(chatSql);
    var results = [];
    var transArr = [];
    /* Insert each MSSQL row into chats collection
     * Push inserted document into results array
     * Chats that already exist in Mongo will not be inserted and
     * thus will not be added to the MSSQL query for transcripts */
    for (var record in recordset) {
      try {
        results.push(yield chats.insert(recordset[record]));
        process.stdout.write("Inserting chat " + recordset[record].chatID + "\r");
        /* If a chat already exists in Mongo
        *  then we catch the duplicate key error here */
      } catch (ex) { console.dir(ex.err); }
    }

    console.log("\nInserted " + results.length + " chats.");

    var requestTrans = new mssql.Request(connection);
    /* For each inserted chat document query for its transcripts */
    for (var chat in results) {
      process.stdout.write("Pulling transcripts for " + results[chat].chatID + "\r");
      requestTrans.input('chatID', mssql.VarChar, results[chat].chatID);
      var tranSet = yield requestTrans.query(tranSql);
      /* Iterate over returned transcript rows for each MSSQL query */
      for (var tran in tranSet) {
        /* Strip HTML from transcript text */
        var text = tranSet[tran].text;
        text = _s.unescapeHTML(text);
        text = _s.stripTags(text);
        tranSet[tran].text = text;
        /* Push each transcript into array for Mongo bulk insert */
        transArr.push(tranSet[tran]);
      }
    }

    /* Bulk insert transcripts */
    try { yield transcripts.insert(transArr); }
    catch (ex) { console.dir(ex); }
    console.log("\nBulk inserted " + transArr.length + " chat messages.");

  } catch (ex) { console.dir(ex); } finally {
    connection.close();
    db.close();
  }
})();
