'use strict';
/**
 *  author: Carson Cao
 *  date:   2017/06/02  
 **/

var router = require('koa-router')();
var request = require('koa-request');
var constants = require('../../config/constants');
var dataFormater = require('../../utils/data-format');
var ejs = require('ejs');
var loadbalance = require('../../utils/loadbalance');
var Log = require('log')
,log = new Log('info');


router.prefix('/druid/query');

router.get('/', function *(next) {
      yield this.render('index', {
    title: "Druid query restful"
  });
});

//get -topN
router.get('/topn', function *(next) {
    var tempParams = this.request.query;
    var druidData = yield topN(tempParams);
    if (druidData.length == 0) {
        this.body = [];
        log.info("!!!!Result set is empty!!!!");
    } else {
        this.body = druidData;
    }
});
//post -topN
router.post('/topn', function *(next) {
    var tempParams = this.request.body;

    /* //params for data format
    var spec = {
        dimension : tempParams.dimension,
        metrics : getMetrics(tempParams.aggregations)
    };*/
    var druidData = yield topN(tempParams);
    if (druidData.length == 0) {
        this.body = [];
        log.info("!!!!Result set is empty!!!!");
    } else {
        this.body = druidData;
        // this.body = dataFormater(druidData, spec, 'topn', 'column');//for data result format
    }
});

//get -rowCount
router.get('/rowcount', function *(next) {
    var tempParams = this.request.query;
    var rowCountJson = constants.ROWCOUNT_JSON_TEMPLATE;
    ejs.compile(rowCountJson);
    var content = ejs.render(rowCountJson, tempParams);

    var options = {
        url: loadbalance.getDruidBrokerUrl(tempParams.tableName),
        method: "POST",
        headers: {"content-type": "application/json"},
        body:content
    };

    var response = yield request(options);
    var druidData = JSON.parse(response.body);

    if (druidData.length == 0) {
        this.body = [];
        log.info("!!!!Result set is empty!!!!");
    } else {
        this.body = druidData;
       
    }
});
//get -timeseries
router.get('/timeseries', function *(next) {
    var tempParams = this.request.query;
    var druidData = yield timeseries(tempParams);
    if (druidData.length == 0) {
        this.body = [];
        log.info("!!!!Result set is empty!!!!");
    } else {
        this.body = druidData; 
    }
});
//post -timeseries
router.post('/timeseries', function *(next) {
    var tempParams = this.request.body;
    var druidData = yield timeseries(tempParams);
    if (druidData.length == 0) {
        this.body = [];
        log.info("!!!!Result set is empty!!!!");
    } else {
        this.body = druidData; 
    }
});

//get -timebound
router.get('/timebound', function *(next) {
    var tempParams = this.request.query;
    var druidData = yield timeBound(tempParams);
    if (druidData.length == 0) {
        this.body = [];
        log.info("!!!!Result set is empty!!!!");
    } else {
        this.body = druidData; 
    }
});
//post -timebound
router.post('/timebound', function *(next) {
    var tempParams = this.request.body;
    var druidData = yield timeBound(tempParams);
    if (druidData.length == 0) {
        this.body = [];
        log.info("!!!!Result set is empty!!!!");
    } else {
        this.body = druidData; 
    }
});

//post -groupby
router.post('/groupby', function *(next) {
    var td = 1000;

    if (this.request.body.threshold != null) {
        td = this.request.body.threshold;
    }
    var params = {
        threshold:td,
        tableName:this.request.body.tableName,
        dimension:this.request.body.dimension,
        intervals:this.request.body.intervals
    };
    var groupByJson = constants.TOPN_JSON_TEMPLATE;
        ejs.compile(groupByJson);
    var content = ejs.render(groupByJson, params);

    var options = {
        url: loadbalance.getDruidBrokerUrl(this.request.body.tableName),
        method: "POST",
        headers: {"content-type": "application/json"},
        body:content
    };
    var response = yield request(options);
    this.body = response.body;
});

//get topn data from druid
function * topN(tempParams) {
    log.info(JSON.stringify(tempParams));
    var aggregations = new Array();
    var minTd = constants.DRUID_QUERY_TOPN_MINTHRESHOLD;
    var td = 500;

    if (tempParams.minThreshold != null && tempParams.minThreshold != "") {
        minTd = tempParams.minThreshold;
    }
    if (tempParams.threshold != null && tempParams.threshold != "") {
        td = tempParams.threshold;
    }
    var params = {
        threshold: td,
        tableName: tempParams.tableName,
        dimension: tempParams.dimension,
        intervals: tempParams.intervals,
        filters: getFilters(tempParams),
        metric: tempParams.metric,
        granularity: getGranularity(tempParams.granularity),
        aggregations : JSON.stringify(getAggregations(tempParams.aggregations)),
        minTopNThreshold:minTd
    };
    var topNJson = constants.TOPN_JSON_TEMPLATE;
    ejs.compile(topNJson);

    var content = ejs.render(topNJson, params);
    //console.log(content);
    var options = {
        url: loadbalance.getDruidBrokerUrl(tempParams.tableName),
        method: "POST",
        headers: {"content-type": "application/json"},
        body:content
    };
    var response = yield request(options);
    return response.body;
}

//get timeseries data from druid
function * timeseries(tempParams){
    log.info(JSON.stringify(tempParams));
    var aggregations = new Array();

    var descending = 'false';
    if (tempParams.descending != null && tempParams.descending != "") {
        descending = tempParams.descending;
    }

    var params = {
        granularity: getGranularity(tempParams.granularity),
        tableName: tempParams.tableName,
        ifDescending: descending,
        intervals: tempParams.intervals,
        filters: getFilters(tempParams),
        aggregations : JSON.stringify(getAggregations(tempParams.aggregations))      
    };

    var timeseriesJson = constants.TIMESERIES_JSON_TEMPLATE;
    ejs.compile(timeseriesJson);

    var content = ejs.render(timeseriesJson, params);
    
    var options = {
        url: loadbalance.getDruidBrokerUrl(tempParams.tableName),
        method: "POST",
        headers: {"content-type": "application/json"},
        body:content
    };
    var response = yield request(options);
    return response.body;
}

//get timeseries data from druid
function * timeBound(tempParams){
    log.info(JSON.stringify(tempParams));
    var bound = "";
    var filters = getFilters(tempParams);
    
    if (tempParams.bound != null && tempParams.bound != "") {
        if (tempParams.bound == "maxTime") {
            bound = ",\n \"bound\":\"maxTime\"";
        }
        if (tempParams.bound == "minTime") {
            bound = ",\n \"bound\":\"minTime\"";
        }
    }
    if (bound == "" && filters != "") {
         filters = filters.substring(0,filters.length-1);
    }
    var params = {
        tableName: tempParams.tableName,
        bound: bound,
        filters: filters
    };
    ejs.compile(constants.TIMEBOUND_JSON_TEMPLATE);
    var content = ejs.render(constants.TIMEBOUND_JSON_TEMPLATE, params);
    
    var options = {
        url: loadbalance.getDruidBrokerUrl(tempParams.tableName),
        method: "POST",
        headers: {"content-type": "application/json"},
        body:content
    };
    var response = yield request(options);
    return response.body;
}

//get Aggregations
function getAggregations(aggstr) {
    if (aggstr == null) {
        return null;
    }
    var aggregations = [];
    var items = aggstr.split(',');

    for(var i = 0;i< items.length;i++) {
        var colDesc = items[i].split(':');
        if (colDesc.length >= 2) {
            switch(colDesc[1]){
                case 'count' : 
                    var temp = constants.INDEX_METRICSSPEC_COUNT;
                    ejs.compile(temp);
                    var metric = ejs.render(temp, {'name':colDesc[0]});
                    aggregations.push(JSON.parse(metric));
                    break;
                case 'longSum':
                    var temp = constants.INDEX_METRICSSPEC_LONGSUM;
                    ejs.compile(temp);
                    var tempObj = {};
                    tempObj['colName'] = colDesc[0];
                    if (colDesc.length == 2) {
                        tempObj['name'] = colDesc[0];
                    } else {
                        tempObj['name'] = colDesc[2];
                    }
                    var metric = ejs.render(temp, tempObj);
                    //console.log(tempObj);
                    aggregations.push(JSON.parse(metric));
                    break;
                case 'doubleSum': 
                    var temp = constants.INDEX_METRICSSPEC_DOUBLESUM;
                    ejs.compile(temp);
                    var tempObj = {};
                    tempObj['colName'] = colDesc[0];
                    if (colDesc.length == 2) {
                        tempObj['name'] = colDesc[0];
                    } else {
                        tempObj['name'] = colDesc[2];
                    }
                    var metric = ejs.render(temp, tempObj);
                    aggregations.push(JSON.parse(metric));
                    break;
                case 'hyperUnique':
                    var temp = constants.INDEX_METRICSSPEC_HYPERUNIQUE;
                    ejs.compile(temp);
                    var tempObj = {};
                    tempObj['colName'] = colDesc[0];
                    if (colDesc.length == 2) {
                        tempObj['name'] = colDesc[0];
                    } else {
                        tempObj['name'] = colDesc[2];
                    }
                    var metric = ejs.render(temp, tempObj);
                    aggregations.push(JSON.parse(metric));
                    break;
            }
        }    
    }
    return aggregations;

}

//get metrics
function getMetrics(aggstr) {
    if (aggstr == null) {
        return null;
    }
    var metrics = new Array();
    var items = aggstr.split(',');

    for (var i = 0;i < items.length;i++) {
        var colDesc = items[i].split(':');
        if (colDesc.length == 2) {
            metrics.push(colDesc[0]);
        }
    }
    return metrics;
}

//get granularity
function getGranularity(str){
    if (str == null) {
        return "\"all\"";
    }
    var items = str.split(':');
    if (items.length == 2) {
        var granularityTemp = constants.QUERY_GRANULARITY_TEMPLATE;
        ejs.compile(granularityTemp);
        var content = ejs.render(granularityTemp, {"granularityType":items[0],"granularityValue":items[1]});
        return content;
    } else if (items.length == 3){
        var granularityTemp = constants.QUERY_GRANULARITY_TEMPLATE2;
        ejs.compile(granularityTemp);
        var content = ejs.render(granularityTemp, {"granularityType":items[0],"granularityValue":items[1],"timeZone":items[2]});
        return content;
    }else{
        return "\""+str+"\"";
    }
}

//get filters
function getFilters(params) {
    var filters = "";

    if (params.filter != null && params.filter != "") {
        var items = params.filter.split(':');
        if (items.length >= 3 ) {
            var type = trim(items[0]).toLowerCase();
            var dimension = trim(items[1]);
            var valueStart = items[0].length + items[1].length + 2;
            var value = params.filter.substring(valueStart);

            if (type == 'selector'){
                filters = 
                "\"filter\": {"+
                "\"type\": \"selector\","+
                "\"dimension\": \""+dimension+"\","+
                "\"value\" : \""+value+"\""+
                "},";
            } else if (type == 'like') {
                filters = 
                "\"filter\": {"+
                "\"type\": \"like\","+
                "\"dimension\": \""+dimension+"\","+
                "\"pattern\" : \""+value+"\""+
                "},";
            } else if (type == 'regex') {
                filters = 
                "\"filter\": {"+
                "\"type\": \"regex\","+
                "\"dimension\": \""+dimension+"\","+
                "\"pattern\" : \""+value+"\""+
                "},";
            } else if (type == 'bound') {
                if (value == null || value == "") {
                    return filters;
                }
                var boundValues = value.split(',');
                if (boundValues.length < 2) {
                    return filters;
                }
                filters = 
                "\"filter\": {"+
                "\"type\": \"bound\","+
                "\"dimension\": \""+dimension+"\",";
                var ifHasLower = false;
                if (trim(boundValues[0]).length > 1) {
                    ifHasLower = true;
                    var temp = trim(boundValues[0]);
                    var lowerValue = temp.substring(1);
                    filters += "\"lower\":\""+lowerValue+"\"";
                    if (temp.slice(0, 1) == '(') {
                         filters += ",\"lowerStrict\": true";
                    }
                }
                if (trim(boundValues[1]).length > 1) {
                    var temp = trim(boundValues[1]);
                    var upperValue = temp.substring(0, temp.length - 1);
                    if (ifHasLower) {
                        filters += ",";
                    }
                    filters += "\"upper\":\""+upperValue+"\"";
                    if (temp.indexOf(')') != -1) {
                         filters += ",\"upperStrict\": true";
                    }
                }
                filters += "},";
            }

        }
    }
    if (params.filters != null && params.filters != "") {
        filters = "";
    }
    console.log(filters);
    return filters;
}

//Trim spaces
function trim(str){
    return str.replace(/(^\s*)|(\s*$)/g, "");
}

module.exports = router;


