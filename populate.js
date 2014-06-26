// jshint esnext: true
console.time('time');
var _s     = require('underscore.string');
var fs     = require('fs');
var co     = require('co');
var moment = require('moment');
var mssql  = require('co-mssql');
var pg     = require('co-pg')(require('pg'));

// Load SQL into memory
var chatSql       = fs.readFileSync('chats.sql', 'utf-8');
var tranSql       = fs.readFileSync('transcripts.sql', 'utf-8');
var createTable   = fs.readFileSync('schema.sql', 'utf-8');
var chatsCopyFrom = fs.readFileSync('chatsCopyFrom.sql', 'utf-8');
var transCopyFrom = fs.readFileSync('transCopyFrom.sql', 'utf-8');

// Grab date so we can pull chats per day
var day   = moment().format('DD');
var month = moment().format('MM');
var year  = moment().format('YYYY');

co(function *() {

    // Connect to PostgreSQL
    var connectionString = process.env.DATABASE_URL;
    var client = new pg.Client(connectionString);
    yield client.connect_();
    yield client.query_(createTable);
    var chatsStream = client.copyFrom(chatsCopyFrom);
    var transStream = client.copyFrom(transCopyFrom);

    // Connect to MSSQL
    var connection = new mssql.Connection({
        user     : process.env.DW_USER,
        password : process.env.DW_PASS,
        server   : process.env.DW_URL,
        database : process.env.DW_DB,
    });
    yield connection.connect();

    try {
        var request = new mssql.Request(connection);
        request.input('day', mssql.VarChar, day);
        request.input('month', mssql.VarChar, month);
        request.input('year', mssql.VarChar, year);

        var recordset = yield request.query(chatSql);
        var results   = [];

        for (var record in recordset) {
            var c = recordset[record];
            process.stdout.write('Inserting chat_id ' + c.chat_id + '\r');
            results.push(c.chat_id);
            c.answered_at = moment(c.answered_at).format('YYYY-MM-DDTHH:mm:ss.SSSZZ');
            for (var key in c) {
                if (key === 'chat_status') {
                    chatsStream.write(c[key] + '\n');
                } else {
                    chatsStream.write(c[key] + '\t');
                }
            }
        }
        chatsStream.end();
        console.log('\nInserted ' + results.length + ' chats.');

        var requestTrans = new mssql.Request(connection);
        // For each successfully inserted chat document we query MSSQL for its transcripts
        var transcripts = 0;
        for (var chat in results) {
            process.stdout.write('Inserting transcript for ' + results[chat] + '\r');
            requestTrans.input('chatID', mssql.VarChar, results[chat]);
            var tranSet = yield requestTrans.query(tranSql);
            // Iterate over returned transcript rows for each chatID
            for (var tran in tranSet) {
                var t = tranSet[tran];
                ++transcripts;
                t.created_at = moment(t.created_at).format('YYYY-MM-DDTHH:mm:ss.SSSZZ');
                // Strip HTML from transcript text, thanks BoldChat
                var text = t.text;
                text = _s.unescapeHTML(text);
                text = _s.stripTags(text);
                text = text.replace(/[&]nbsp[;]/gi, ' ');
                text = text.replace(/\r?\n|\r|\t/g, ' ');
                text = text.replace(/\\/g, '');
                transStream.write(t.message_id + '\t');
                transStream.write(t.chat_id + '\t');
                transStream.write(t.person_type + '\t');
                transStream.write(t.name + '\t');
                transStream.write(t.created_at + '\t');
                transStream.write(text + '\n');
            }
        }
        transStream.end();
        console.log('\nInserted ' + transcripts + ' transcript rows.');

    } catch (ex) {
        console.dir(ex);
    } finally {
        connection.close();
        client.end();
        console.timeEnd('time');
    }
})();
