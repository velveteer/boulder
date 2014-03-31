/* jshint esnext:true */
var _     = require('underscore');
var koa   = require('koa');
var route = require('koa-route');
var app   = module.exports = koa();
var monk  = require('monk');
var wrap  = require('co-monk');
var db    = monk('localhost/test');
var chats = wrap(db.get('chats'));
var trans = wrap(db.get('transcripts'));

app.use(route.get('/chat', chatList));
app.use(route.get('/chat/:chatid', chatByID));
app.use(route.get('/sso/:sso', chatsBySSO));
app.use(route.get('/metrics/:sso', metrics));
app.use(route.get('/transcripts/:chatid', transcripts));

/* List all chats */
function* chatList() {
  var res   = yield chats.find({});
  this.body = res;
}

/* Grab chats by SSO */
function* chatsBySSO(sso) {
  var _sso  = decodeURI(sso);
  var res   = yield chats.find({rackerSSO: _sso}, {limit: 200});
  this.body = res;
}

/* Pull one chat by chatID */
function* chatByID(chatID) {
  var _chatID = decodeURI(chatID);
  var res     = yield chats.findOne({chatID: _chatID});
  this.body   = res;
}

/* Pull all transcripts for a chat */
function* transcripts(chatID) {
  var _chatID = decodeURI(chatID);
  var res     = yield trans.find({chatID: _chatID},{sort:{createdAt: 1}});
  this.body   = res;
}

/* Averages for EOCR and chat counts */
function* metrics(sso) {
  var _sso        = decodeURI(sso);
  var obj         = {};
  var chatsArr    = yield chats.find({rackerSSO: _sso});
  obj.avgEOCR     = averages(chatsArr);
  obj.totalChats  = chatsArr.length;
  this.body       = obj;
}

/* Averages helper function */
function averages(array) {
  var eocr  = [];
  var arr   = _.filter(array, function(item) { return !!item.chatRating;});
  _.each(arr, function(item) {
    eocr.push(parseInt(item.chatRating));
  });
  var sum   = _.reduce(eocr, function(acc, num) { return acc + num;}, 0);
  return ((sum/eocr.length).toFixed(2))/1;
}

if (!module.parent) app.listen(3000);
