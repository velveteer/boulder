var koa   = require('koa');
var route = require('koa-route');
var app   = module.exports = koa();
var monk  = require('monk');
var wrap  = require('co-monk');
var db    = monk('localhost/test');
var chats = wrap(db.get('chats'));
var trans = wrap(db.get('transcripts'));

app.use(route.get('/chat', list));
app.use(route.get('/chat/:sso', sso));

function* list() {
  var res = yield chats.find({});
  this.body = res;
}

function* sso(sso) {
  sso = decodeURI(sso);
  var res = yield chats.find({rackerSSO: sso});
  this.body = res;
}

if (!module.parent) app.listen(3000);
