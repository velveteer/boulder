// jshint esnext:true
var _ = require('underscore');
var koa = require('koa');
var route = require('koa-route');
var app = module.exports = koa();
var monk = require('monk');
var wrap = require('co-monk');
var db = monk('localhost/test');
var chats = wrap(db.get('chats'));
var trans = wrap(db.get('transcripts'));

var _chats = {
    // List all chats
    list: function * () {
        var res = yield chats.find({}, {
            sort: {
                answeredAt: -1
            },
            limit: 200
        });
        this.body = res;
    },

    // Grab chats by SSO
    sso: function * (sso) {
        var _sso = decodeURI(sso);
        var res = yield chats.find(
            { rackerSSO: _sso },
            { sort: { answeredAt: -1 }, limit: 200 }
        );
        this.body = res;
    },

    // Pull one chat by chatID
    id: function * (chatID) {
        var _chatID = decodeURI(chatID);
        var res = yield chats.findOne({
            chatID: _chatID
        });
        this.body = res;
    },

    // Pull chats by customer DDI
    ddi: function * (ddi) {
        var _ddi = decodeURI(ddi);
        var res = yield chats.find({
            cloudAccount: _ddi
        }, {
            sort: {
                answeredAt: -1
            }
        });
        this.body = res;
    },

    // Pull chats by CORE account
    core: function * (core) {
        var _core = decodeURI(core);
        var res = yield chats.find({
            coreAccount: _core
        }, {
            sort: {
                answeredAt: -1
            }
        });
        this.body = res;
    },

    // Pull chats by E&A account
    ea: function * (ea) {
        var _ea = decodeURI(ea);
        var res = yield chats.find({
            emailAccount: _ea
        }, {
            sort: {
                answeredAt: -1
            }
        });
        this.body = res;
    }
};

// Pull all transcripts for a chat
function * _transcripts(chatID) {
    var _chatID = decodeURI(chatID);
    var res = yield trans.find({
        chatID: _chatID
    }, {
        sort: {
            createdAt: 1
        }
    });
    this.body = res;
}

// Averages for EOCR and chat counts
function * _metrics(sso) {
    var _sso = decodeURI(sso);
    var obj = {};
    var chatsArr = yield chats.find({
        rackerSSO: _sso
    });
    obj.avgEOCR = averages(chatsArr);
    obj.totalChats = chatsArr.length;
    this.body = obj;
}

// Averages helper function
function averages(array) {
    var eocr = [];
    var arr = _.filter(array, function(item) {
        return !!item.chatRating;
    });
    _.each(arr, function(item) {
        eocr.push(parseInt(item.chatRating));
    });
    var sum = _.reduce(eocr, function(acc, num) {
        return acc + num;
    }, 0);
    return ((sum / eocr.length).toFixed(2)) / 1;
}

// Routes
app.use(route.get('/api/chats', _chats.list));
app.use(route.get('/api/chats/:chatid', _chats.id));
app.use(route.get('/api/chats/sso/:sso', _chats.sso));
app.use(route.get('/api/chats/ddi/:ddi', _chats.ddi));
app.use(route.get('/api/chats/core/:core', _chats.core));
app.use(route.get('/api/chats/ea/:ea', _chats.ea));
app.use(route.get('/api/metrics/:sso', _metrics));
app.use(route.get('/api/transcripts/:chatid', _transcripts));

if (!module.parent) app.listen(3000);
console.log("Listening on port 3000");
