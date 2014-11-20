var Query = require('./lib/query');

var ok = 'success',
    nok = 'fail';
var _sqlInfo = null;

var MM = module.exports = {};

/**
 * 根据路由规则进行数据库執行語句
 * @param sql
 * @param cb
 */

MM.invoke = function operateDb(sql, cb) {
    var parseData = {};
    parseData.sql = sql;
    parseData.params = '';
    parseData.readCache = false;
    parseData.writeCache = false;
    parseData.isDebug = false;
    parseData.explain = false;
    parseData.mode = 'sqlMode';
    var query = Query.create(parseData);
    workerLogger.notice('getQuery|token:' + query.token, JSON.stringify(parseData));
    query.getData(function (query) {
        // REQUEST_QUEUE.end(query, function (query) {
        if (__STAT__) {
            __STAT__.allUseTime += Date.now() - query.start;
            __STAT__.nowReqs--;
        }
        var result = formatResult(query);
        cb(result);
        workerLogger.notice('RES|token:' + query.token, JSON.stringify({timeUse: Date.now() - query.start, length: result.length}));
    });
}
/**
 * 處理sql返回的數據
 * @param query
 * @returns {{data: (query.result|*|Array), error: number, msg: (query.error|*|string), route: (*|maxLenRoute.route|ret.route|route|Reform.result.route|Function), columns: (res.columns|*), routeTime: (query.routeTime|*), getResDebug: (query.debugInfo|*), explain: (query.explain|*), expire: number}|{data: (query.result|*|Array), error: number, msg: (query.error|*|string), affectedRows: (*|ret.affectedRows|affectedRows|query.affectedRows), insertId: (*|ret.insertId|insertId|Number|query.insertId), expire: number}|{data: (query.result|*|Array), error: number, msg: (query.error|*|string), expire: number}}
 */
var formatResult = function (query) {
    if (query.error && query.error.toString) {
        query.error = query.error.toString();
    }

    if (query.result && query.result[0] && query.result[0].hasOwnProperty('insertId')) {
        query['affectedRows'] = query.result[0].affectedRows;
        query['insertId'] = query.result[0].insertId;
        query.result = [];
    }
    if (query.parseData.isDebug) {
        var ret = {
            data: query.result || [],
            error: query.flag,
            msg: query.error || 'operate success',
            route: query.route ? query.route.res.route : null,
            columns: query.route ? query.route.res.columns : null,
            routeTime: query.routeTime,
            getResDebug: query.debugInfo,
            explain: query.explain,
            expire: query.expire
        };
        if (query.insertId >= 0) {
            ret['affectedRows'] = query.affectedRows;
            ret['insertId'] = query.insertId;
        }
        workerLogger.debug('DebugRes', JSON.stringify(ret));
    } else {
        if(query.insertId instanceof Array){
            var ret = { data: query.result || [], error: query.flag, msg: query.error || 'operate success', affectedRows: query.affectedRows, insertId: query.insertId, expire: query.expire};
        }else{
            if (query.insertId >= 0)
                var ret = { data: query.result || [], error: query.flag, msg: query.error || 'operate success', affectedRows: query.affectedRows, insertId: query.insertId, expire: query.expire};
            else
                var ret = { data: rows2table(query.result) || [], error: query.flag, msg: query.error || 'operate success', expire: query.expire};
        }
    }
    return ret;
};
/**
 * 将数据转换成表的形式
 * @param rows
 * @returns {{columns: Array, data: Array}}
 */
function rows2table(rows) {
    if (rows === null) {
        return null;
    }
    var res = {
        columns: [],
        data: []
    };

    //rows meust be an array
    var fnode = rows[0];

    if (fnode) {
        var key;
        for (key in fnode) {
            res.columns.push(key);
        }
        var ele;
        var data = res.data;
        var len = rows.length, i = 0;
        while (i < len) {
            fnode = rows[i];
            ele = [];
            for (key in fnode) {
                ele.push(fnode[key]);
            }
            data.push(ele);
            i++;
        }
    }

    return res;
}






