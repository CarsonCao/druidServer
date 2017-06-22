'use strict';

/**
 *  author: Carson Cao
 *  date:   2017/06/02  
 **/

var router = require('koa-router')();
var request = require('koa-request');
var ejs = require('ejs');
var fs = require('fs');
var uuidV1 = require('uuid/v1');
var Log = require('log')
,log = new Log('info');

var constants = require('../../config/constants');
var db = require('../../utils/db-manager');
var loadbalance = require('../../utils/loadbalance');



router.prefix('/druid/indexer');

router.get('/', function *(next) {
    yield this.render('index', {
        projects: JSON.stringify(yield db.getAllRows("t_projects")),
        jobs: JSON.stringify(yield db.getAllRows("t_jobs"))
    });
});

//submit jobs
router.post('/create',function *(next) {
    var projectSpec = this.request.body;
    var self = this
    if (projectSpec.projectId == null || projectSpec.userId == null || projectSpec.jobCount == null 
        || projectSpec.jobsSpec == null ||projectSpec.messageSpec == null){
        var errorMeg = "Required parameters: 'projectId', 'userId', 'jobCount', 'jobsSpec','messageSpec'!";
        this.body={"ERROR": errorMeg };
        return;
    }
    var curDate = new Date();
    //insert project info
    var projectObj = new Object();
    projectObj.id = uuidV1();
    projectObj.prjectId = projectSpec.projectId;
    projectObj.userId = projectSpec.userId;
    projectObj.state = "running";
    projectObj.jobCount = projectSpec.jobCount; 
    projectObj.mgThreshold = projectSpec.messageSpec.mgThreshold;
    projectObj.ifMessage = false;
    projectObj.successCount = 0;
    projectObj.submitTime = curDate.toLocaleString();
    projectObj.messageSpec = JSON.stringify(projectSpec.messageSpec);
    
    //!!!---Transactions should be used here and will be added later---!!!
    db.insertRows("t_projects", projectObj);
    var messages = {
        "projectId":projectObj.prjectId,
        "jobsResult":[]
    };

    for (var i = 0;i < projectSpec.jobCount;i++) {
       
        var jobObj = new Object();//JSON.stringify
        var jobSpec = projectSpec.jobsSpec[i];
        var taskDesc = yield submitJob(jobSpec);
        if (taskDesc.indexOf("ERROR") >= 0 || taskDesc.indexOf("Error") >= 0) {
            var tableName = jobSpec.tableName;
            messages.jobsResult.push({'tableName' : jobSpec.tableName,"return message" : taskDesc});
            continue;

        } else if(taskDesc.indexOf("task") < 10 && taskDesc.indexOf("task") > 0) {
            messages.jobsResult.push({'tableName' : jobSpec.tableName,"return message" : taskDesc});
            var taskId = JSON.parse(taskDesc);
            jobObj.id = uuidV1();
            jobObj.taskId = taskId.task;
            jobObj.tableName = jobSpec.tableName;
            jobObj.state = "pending";
            jobObj.duration = -1;
            jobObj.location = "";
            jobObj.projectId = projectObj.id;
            jobObj.resubmitCount = jobSpec.resubmitCount;
            jobObj.submitCount = 0;
            jobObj.jobSpec = JSON.stringify(jobSpec);
            db.insertRows("t_jobs", jobObj);
        }   
    }
    this.body = messages; 
});

//submit single job
function* submitJob(spec) {  
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

    if (spec.timeFormat != null){
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

    var indexJsonTemplate = fs.readFileSync(__dirname+'/../../templates/'+constants.INDEX_HADOOP_JSON_TEMPLATE,"utf-8");
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

    var response = yield request(options);
    return response.body;
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


module.exports = router;
