
var db = require('./db-manager');
var schedule = require('node-schedule');
var request = require('request');
var loadbalance = require('./loadbalance');
var constants = require('../config/constants');
var ejs = require('ejs');
var fs = require('fs');
var amqp = require('amqplib/callback_api');
var Log = require('log')
,log = new Log('info');

var itemsMap = {}; 
var successJobs = 0;
var failedJobs = 0;
var otherJobs = 0;

let getProjectsByState = state=> {
    return db.getProjectsByState(state);
}

let getJobsByProId = proId => {
    return db.getJobsByProId(proId);
}
//Init params
function paramsInit(){ 
    successJobs = 0;
    failedJobs = 0;
    otherJobs = 0;
}

//Get job status
let getJobInfoFromDruid = params=>{
    return new Promise(function(resolve, reject) {
        var indexerServerUrl = loadbalance.getDruidOverlordUrl();
        request(indexerServerUrl+"/"+params.taskId+"/status", function (error, response, body){
            if(error) {
                reject(error);
            } else {
                var result = JSON.parse(body);
                result.projectId = params.projectId;
                resolve(result);
            }
        });
    });
}

//Resubmit failed job
let resubmitJob = spec=> { 
    return new Promise(function(resolve, reject) { 
    //Set default value
    var indexTimeFormat = "";
    var queryGranularity = constants.INDEX_DEFAULT_QUERYGRANULARITY;
    var hadoopVersion = constants.INDEX_DEFAULT_HADOOP_VERSION;
    var dimensions = "";
    var columns = "";
    var timeColName = "";
    var metricsSpec = "";
    var hadoopVersion = constants.INDEX_DEFAULT_HADOOP_VERSION;
    var parseSpec = "";

    log.info("Starting submit single indexing job to druid...");
    if (spec.fileFormat == null || spec.hdfsUrl == null || spec.dataPath == null || 
        spec.tableName == null || spec.segmentGranularity == null || spec.columnsDesc == null || 
        spec.intervals == null ){
        var errorMeg = "Required parameters: 'fileFormat','hdfsUrl','dataPath','tableName'," +
                       "'segmentGranularity','columnsDesc','intervals' !";
        log.error(errorMeg);
        return {"ERROR": errorMeg +"   request:  "+JSON.stringify(spec)};
    }

    var columnsDescArray = spec.columnsDesc.split(',');
    columns = getIndexColumns(columnsDescArray);
    dimensions = getIndexDimensions(columnsDescArray);
    metricsSpec = getIndexMetricsSpec(columnsDescArray, spec.timeFormat);
    timeColName = getIndexTimeColumn(columnsDescArray);

    if (request.timeFormat != null){
        ejs.compile(constants.INDEX_TIMEFORMAT);
        indexTimeFormat = ejs.render(constants.INDEX_TIMEFORMAT,{"timeFormat":spec.timeFormat});
    }
    if (spec.hadoopVersion != null) {
        hadoopVersion = spec.hadoopVersion;
    }
    //If the format of input file is TSV
    if (spec.fileFormat == 'tsv' || spec.fileFormat == 'TSV') {
        if (spec.delimiter == null || spec.listDelimiter == null) {
            var errorMeg ="The indexing json can't be generated,because both 'delimiter' and "+
                           "'listDelimiter' should be specified.";
            log.error(errorMeg);
            return {"ERROR":  errorMeg +"   request:  "+JSON.stringify(spec) };
        } 
        var parseSpecParams = {
            "dimensions":dimensions,
            "delimiter":spec.delimiter,
            "listDelimiter":spec.listDelimiter,
            "columns":columns,
            "timeColumn":timeColName,
            "indexTimeFormat":indexTimeFormat
        };
        ejs.compile(constants.INDEX_PARSESPEC_TSV);
        parseSpec = ejs.render(constants.INDEX_PARSESPEC_TSV, parseSpecParams);
    }
    if (spec.fileFormat == 'csv' || spec.fileFormat == 'CSV') {
        var parseSpecParams = {
            "dimensions":dimensions,
            "columns":columns,
            "timeColumn":timeColName,
            "indexTimeFormat":indexTimeFormat
        };
        ejs.compile(constants.INDEX_PARSESPEC_CSV);
        parseSpec = ejs.render(constants.INDEX_PARSESPEC_CSV, parseSpecParams);

    }
    if (spec.hadoopVersion != null) {
        hadoopVersion = spec.hadoopVersion;
    }
    if (spec.queryGranularity != null) {
        queryGranularity = spec.queryGranularity;
    }

    var indexJsonTemplate = fs.readFileSync(__dirname+'/../templates/'+constants.INDEX_HADOOP_JSON_TEMPLATE,"utf-8");
    var params = {
        "hdfsUrl":spec.hdfsUrl,
        "dataPath":spec.dataPath,
        "tableName":spec.tableName,
        "segmentGranularity":spec.segmentGranularity,
        "queryGranularity":queryGranularity,
        "intervals":spec.intervals,
        "parseSpec":parseSpec,
        "metricsSpec":metricsSpec,
        "hadoopVersion":hadoopVersion
    };
    ejs.compile(indexJsonTemplate);
    var indexJson = ejs.render(indexJsonTemplate,params);
    
    var options = {
        url: loadbalance.getDruidOverlordUrl(),
        method: "POST",
        headers: {"content-type": "application/json"},
        body:indexJson
    };

    
        request(options, function (error, response, body){
            if(error) {
                reject(error);
            } else {
                resolve(body);
            }
        });
    });
}


//Get columns
function getIndexColumns(columnsDescArray){
    var columns = "";
    for (var i= 0;i < columnsDescArray.length; i++){
        var temp = columnsDescArray[i].split(':');
        if (temp.length == 2){
            columns +="\""+trim(temp[0])+"\"";
            if (i < columnsDescArray.length - 1) {
                columns += ",";
            }
        }
    }
    return columns;
}
//Get time column name
function getIndexTimeColumn(columnsDescArray){
    var timeColName = "";
    for (var i = 0;i< columnsDescArray.length;i++) {
        var temp = columnsDescArray[i].split(':');
        if (temp.length == 2 && trim(temp[1]) ==  'timestamp') {
            timeColName = trim(temp[0]);
        }
    }
    return timeColName;
}

//Get dimensions
function getIndexDimensions(columnsDescArray){
    var dimensions = "";
    for (var i = 0;i< columnsDescArray.length;i++) {
        var temp = columnsDescArray[i].split(':');
        if (temp.length == 2 && trim(temp[1]) ==  'string') {
            dimensions += "\""+trim(temp[0])+"\",";
        }
    }
    if (dimensions.length > 0) {
        dimensions = dimensions.substring(0, dimensions.length - 1);
    }
    return dimensions;
}

//Get metrics json
function getIndexMetricsSpec(columnsDescArray, timeFormat){
    var metricSpec = "";
    var timeFormatSpec = "";
    if (timeFormat != null || timeFormat != "") {
        ejs.compile(constants.INDEX_METRIC_TIMEFORMAT);
        timeFormatSpec = ejs.render(constants.INDEX_METRIC_TIMEFORMAT, {"timeFormat":timeFormat});
        
    }
    for (var i = 0; i < columnsDescArray.length; i++) {
        var temp = columnsDescArray[i].split(':');
        if (temp.length == 2){
            if (temp[1] == "long"){
                ejs.compile(constants.INDEX_METRICSSPEC_LONGSUM);
                var metricTemp = ejs.render(constants.INDEX_METRICSSPEC_LONGSUM, {"name":temp[0], "colName":temp[0]});
                metricSpec += metricTemp+",\n";
            }
            if (temp[1] == "double"){
                ejs.compile(constants.INDEX_METRICSSPEC_DOUBLESUM);
                var metricTemp = ejs.render(constants.INDEX_METRICSSPEC_DOUBLESUM, {"name":temp[0], "colName":temp[0]});
                metricSpec += metricTemp+",\n";
            }
            if (temp[1] == "string"){
                ejs.compile(constants.INDEX_METRICSSPEC_HYPERUNIQUE);
                var metricTemp = ejs.render(constants.INDEX_METRICSSPEC_HYPERUNIQUE, {"name":temp[0]+"Count", "colName":temp[0]});
                metricSpec += metricTemp+",\n";

            }
            if (temp[1] == "timestamp"){
                ejs.compile(constants.INDEX_METRICSSPEC_TIMEMAX);
                var metricTemp = ejs.render(constants.INDEX_METRICSSPEC_TIMEMAX, { "timeColumn":temp[0],"indexTimeFormat":timeFormatSpec});
                metricSpec += metricTemp+",\n";
                ejs.compile(constants.INDEX_METRICSSPEC_TIMEMIN);
                var metricTemp = ejs.render(constants.INDEX_METRICSSPEC_TIMEMIN, { "timeColumn":temp[0],"indexTimeFormat":timeFormatSpec});
                metricSpec += metricTemp+",\n";
            }
        }
    }
    ejs.compile(constants.INDEX_METRICSSPEC_COUNT);
    var metricCount = ejs.render(constants.INDEX_METRICSSPEC_COUNT, {"name":"count"});
    metricSpec += metricCount;
    return metricSpec;
}

//Trim spaces
function trim(str){
    return str.replace(/(^\s*)|(\s*$)/g, "");
}

//produce message to rabbitMQ
function produceMS(mgSpec){
amqp.connect(constants.RABBITMQ_QUEUE_ADDRESS, function(err, conn) {
  conn.createChannel(function(err, ch) {
    var ex = constants.RABBITMQ_EXCHANGE_NAME;
    var msg = ejs.render(constants.RABBITMQ_MESSAGE_TEMPLATE,mgSpec);
    ch.assertExchange(ex, constants.RABBITMQ_EXCHANGE_TYPE, {durable: true});
    ch.publish(ex, constants.RABBITMQ_RESULT_ROUTING_KEY, new Buffer(msg), {deliveryMode : 2});
    log.info(" [x] Sent %s", msg);
  });

  setTimeout(function() { conn.close(); }, 500);//process.exit(0) 
});

}


//Main function
function jobMonitor() {
    var j = schedule.scheduleJob('*/60 * * * * *', function (){ 
       
        itemsMap = {};
        paramsInit();
        //Traverse project items by state of 'running'
        getProjectsByState("running").then(function(proItems){
            if (proItems.length > 0) {
                itemsMap.size = proItems.length;
                proItems.forEach(function(proItem){

                    itemsMap[proItem.id] = proItem;

                    getJobsByProId(proItem.id).then(function(jobItems){
                        if (jobItems.length > 0) {
                            itemsMap[jobItems[0].projectId].size = jobItems.length;
                            
                            jobItems.forEach(function(jobItem){
                                itemsMap[jobItem.projectId][jobItem.taskId] = jobItem;
                                var params = {};
                                params.taskId = jobItem.taskId;
                                params.projectId = jobItem.projectId;
                                getJobInfoFromDruid(params).then(function(jobInfo){
                                    var oriStatus = itemsMap[jobInfo.projectId][jobInfo.task].state.toLowerCase();
                                    var curStatus = jobInfo.status.status.toLowerCase();
                                    var rsNum = itemsMap[jobInfo.projectId][jobInfo.task].resubmitCount;
                                    var sNum = itemsMap[jobInfo.projectId][jobInfo.task].submitCount;
                                    var allTaskNum = itemsMap[jobInfo.projectId].size;
                                    var mgThreshold = itemsMap[jobInfo.projectId].mgThreshold;
                                   // console.log("jobproject: "+jobInfo.projectId+", jobTask: "+jobInfo.task);
                                   // console.log("curStatus: "+curStatus+", oriStatus: "+oriStatus);
                                    if (oriStatus != "failed" && oriStatus != "success") {
                                        if (curStatus != oriStatus) {
                                            //!!!!!!!!!!!------update Database-----!!!!!!!!!!!!!
                                            var arr = [jobInfo.task, 
                                                       curStatus, 
                                                       sNum, 
                                                       jobInfo.status.duration,
                                                       "",
                                                       itemsMap[jobInfo.projectId][jobInfo.task].id ];
                                            db.updateJobById(arr);
                                        }
                                        if (curStatus == "failed") { 
                                            if (rsNum == sNum) {
                                                failedJobs++;
                                            } else if (rsNum > sNum){
                                                otherJobs++;
                                            }
                                        }
                                        if (curStatus == "success") {
                                            successJobs++;
                                        }
                                    
                                    } else if (oriStatus == "failed" && curStatus == "failed"){
                                        if (rsNum == sNum) {
                                            failedJobs++;
                                        } else if (rsNum > sNum){
                                            otherJobs++;
                                            //!!!!!!!!!!!------resubmit and update Database-----!!!!!!!!!!!!!
                                            var spec = JSON.parse(itemsMap[jobInfo.projectId][jobInfo.task].jobSpec);
                                            resubmitJob(spec).then(function(data){
                                                var arr = [JSON.parse(data).task, 
                                                       "pending", 
                                                       sNum+1, 
                                                       -1,
                                                       "",
                                                       itemsMap[jobInfo.projectId][jobInfo.task].id ];
                                                db.updateJobById(arr);
                                            });
                                             
                                        }                                       

                                    } else if (oriStatus == "success" && curStatus == "success"){
                                        successJobs++;
                                    }
                                    
                                    if (otherJobs + successJobs + failedJobs == allTaskNum){
                                        var proState = "running";
                                        var ifMessage = itemsMap[jobInfo.projectId].ifMessage;
                                        var ifMessChange = false;
                                        var proStateChange = false;
                                        console.log("all:"+ allTaskNum +". failed:"+failedJobs+", success:"+successJobs+", others:"+otherJobs);
                                        if(!ifMessage) {
                                            if ((successJobs * 1.0) / (allTaskNum * 1.0) > mgThreshold) {
                                                console.log("--------message----------success-----------");
                                                 var messageSpec = JSON.parse(itemsMap[jobInfo.projectId].messageSpec);
                                                 var mgSpec = {"userId":itemsMap[jobInfo.projectId].userId,
                                                     "projectId":itemsMap[jobInfo.projectId].projectId,
                                                     "features":messageSpec.features,
                                                     "filesId":messageSpec.filesId,
                                                     "header":messageSpec.header};
                                                produceMS(mgSpec);
                                                ifMessage = ifMessChange = true;
                                            }else if ((failedJobs * 1.0) /(allTaskNum * 1.0) >= (1.0 - mgThreshold)){
                                                console.log("--------message----------failed-----------");
                                                ifMessage = ifMessChange = true;
                                            }
                                        }
                                        if (otherJobs == 0 ) {
                                            proState = "completed";
                                            proStateChange = true;
                                        }
                                        //!!!!------update project table--------!!!!!!
                                        if (proStateChange || ifMessChange || successJobs != itemsMap[jobInfo.projectId].successCount) {
                                            var arr = [
                                                proState,
                                                ifMessage,
                                                successJobs,
                                                jobInfo.projectId];
                                            db.updateProjectById(arr);
                                        }
                                        paramsInit();
                                    }                                    
                                });
                            });
                        }
                    });
                });
            }
        });
    });
}

module.exports = jobMonitor;
