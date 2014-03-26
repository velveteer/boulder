/*jshint esnext: true */
var _s = require('underscore.string');
var moment = require('moment');
var co = require('co');
var mssql = require('co-mssql');
var monk = require('monk');
var wrap = require('co-monk');

var db = monk('localhost/test');
var chats = wrap(db.get('chats'));
chats.ensureIndex( { chatID: 1 }, { unique: true } );
chats.remove();
var transcripts = wrap(db.get('transcripts'));
transcripts.ensureIndex( { messageID: 1 }, { unique: true } );
transcripts.remove();

var fs = require('fs');
var chatSql = fs.readFileSync('chats.sql', 'utf-8');
var tranSql = fs.readFileSync('transcripts.sql', 'utf-8');

var day = moment().format("DD");
var month = moment().format("MM");
var year = moment().format("YYYY");

co(function* () {
  var connection = new mssql.Connection({
    user: process.env.DW_USER,
    password: process.env.DW_PASS,
    server: process.env.DW_URL,
    database: process.env.DW_DB,
  });

  try {

    yield connection.connect();

    var request = new mssql.Request(connection);
    request.input('day', mssql.VarChar, day);
    request.input('month', mssql.VarChar, month);
    request.input('year', mssql.VarChar, year);
    var recordset = yield request.query(chatSql);
    var results = [];
    var transArr = [];
    for (var record in recordset) {
      try { 
        results.push(yield chats.insert(recordset[record]));
        process.stdout.write("Inserting chat " + recordset[record].chatID + "\r");
      } catch (ex) { console.dir(ex.err); }
    }
    
    console.log("\nInserted " + results.length + " chats.");

    var requestTrans = new mssql.Request(connection);
    for (var chat in results) {
      process.stdout.write("Pulling transcripts for " + results[chat].chatID + "\r");
      requestTrans.input('chatID', mssql.VarChar, results[chat].chatID);
      var tranSet = yield requestTrans.query(tranSql);
      for (var tran in tranSet) {
        var text = tranSet[tran].text;
        text = _s.unescapeHTML(text);
        text = _s.stripTags(text);
        tranSet[tran].text = text;
        try { transArr.push(yield transcripts.insert(tranSet[tran])); }
        catch (ex) { console.dir(ex); }
      }
    }

    console.log("\nInserted " + transArr.length + " chat messages.");

  } catch (ex) { console.dir(ex); } finally {
    connection.close();
    db.close();
  }
})();
