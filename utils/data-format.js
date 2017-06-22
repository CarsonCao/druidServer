'use strict';
/**
 *  author: Carson Cao
 *  date:   2017/06/12  
 **/
var constants = require('../config/constants');
var Log = require('log')
,log = new Log('info');

/****************************
 * data format function.
 *
 * inType : search, topn, select, timeseries
 * outType: column, row,  null
 * spec: {dimension:,metrics[,,]}
 ****************************/
function dataFormat(data,spec, inputType,outputType) {
    if (data == null) {
        return data;
    }

    var outType;
    if (outputType != null) {
        outType = outputType;
    } else {
        outType = constants.DEFAULT_DATA_FORMAT_TYPE;
    }
    if (inputType == null) {
        log.error("dataFormat.js, inputType can not be null! Option values"+
        " : 'search','topn','select','timeseries'.");
        return [];
    }
    if (spec == null){
        log.error("dataFormat.js, data spec can not be null!");
        return [];
    }

    if (outType == "column") {
        return colFormat(data,spec, inputType);
    } else if (outType == "row") {
        return rowFormat(data,spec, inputType);
    } else {
        return [];
    }


}

function colFormat(data, spec, inType){

    var result = new Array();
    switch(inType) {
        case 'search':break;
        case 'topn':
            var oriData = data[0].result;
            var dimension = spec.dimension;
            var metrics = spec.metrics;
            if (dimension == null || metrics == null) {
                log.error("dataFormat.js, spec.dimension or spec.metrics can not be null!");
                break;
            }
            for (var i = 0;i < metrics.length;i++) {
                var obj = new Object();
                var data = new Array();
                obj['name'] = metrics[i];

                for (var j = 0;j< oriData.length;j++) {
                    var kv = new Object();
                    kv['name'] = oriData[j][dimension];
                    kv['value'] = oriData[j][metrics[i]];
                    data.push(kv);
                }
                obj['data'] = data;
                result.push(obj);
            }
            break;
        case 'select': break;
        case 'timeseries':break;

    }
    return result;
}

function rowFormat(data, spec, inType){}


module.exports = dataFormat;
