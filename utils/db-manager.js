"use strict";

var Log = require('log')
,log = new Log('info');

var sqlite3 = require('sqlite3').verbose();
var db;
var dbFileName = __dirname+'/../db/jobMonitor.db'; 

function initDB() {
    log.info("Init database "+ dbFileName+"...");
    db = new sqlite3.Database(dbFileName);
    db.serialize(function() {
        initTables();
    });
}


function initTables() {
    db.run("CREATE TABLE IF NOT EXISTS `t_jobs` ( "+
    " `id` TEXT NOT NULL PRIMARY KEY," +
    " `taskId` TEXT," +
    " `tableName` TEXT," +
    " `state` TEXT," +
    " `duration` INTEGER," +
    " `location` TEXT,"+ 
    " `projectId` TEXT," +
    " `resubmitCount` INTEGER," +
    " `submitCount` INTEGER," +
    " `jobSpec` TEXT" +
    ")");
    db.run("CREATE TABLE IF NOT EXISTS `t_projects` ( "+
    " `id` TEXT NOT NULL PRIMARY KEY," +
    " `projectId` TEXT," +
    " `userId` TEXT," +
    " `state` TEXT," +
    " `jobCount` INTEGER," +
    " `mgThreshold` DECIMAL(1,1),"+ 
    " `ifMessage` BOOLEAN," +
    " `successCount` INTEGER," +
    " `submitTime` TEXT," +
    " `messageSpec` TEXT" +
    ")");
}

exports.insertRows = function (tableName,value) {
    log.info("Insert into table "+tableName+"...");
    db.serialize(function() {  
        if (tableName == "t_jobs"){
	    var stmt = db.prepare("INSERT INTO `t_jobs` VALUES (?,?,?,?,?,?,?,?,?,?)");
	    stmt.run(value.id,value.taskId,value.tableName,value.state,value.duration,value.location,value.projectId,value.resubmitCount, value.submitCount,value.jobSpec);
        }
        if (tableName == "t_projects"){
	    var stmt = db.prepare("INSERT INTO `t_projects` VALUES (?,?,?,?,?,?,?,?,?,?)");
	    stmt.run(value.id, value.prjectId,value.userId, value.state, value.jobCount, value.mgThreshold, value.ifMessage, value.successCount, value.submitTime,value.messageSpec);
        }
    });
}

exports.getAllRows = function (tableName) {
    log.info("Get all rows from table "+tableName+"...");
    var promise = new Promise(function(resolve, reject) {
        db.serialize(function() {
            db.all("SELECT * FROM " + tableName, function(err, rows) {
            if (err) {
                reject(err);
            }else {
                resolve(rows);
            }
        });
    });
  });
  return promise;
}

exports.getProjectsByState = function (state) {
 //   log.info("Get rows from table t_projects filtered by state... ");
    var promise = new Promise(function(resolve, reject) {
        db.serialize(function() {
            db.all("SELECT * FROM t_projects where state = \'"+state+"\'", function(err, rows) {
            if (err) {
                reject(err);
            }else {
                resolve(rows);
            }
        });
    });
  });
  return promise;
}

exports.getJobsByProId = function (proId) {
    log.info("Get rows from table t_jobs filtered by projectId... ");
    var promise = new Promise(function(resolve, reject) {
        db.serialize(function() {
            db.all("SELECT * FROM t_jobs where projectId = \'"+proId+"\'", function(err, rows) {
            if (err) {
                reject(err);
            }else {
                resolve(rows);
            }
        });
    });
  });
  return promise;
}

exports.updateJobById = function (arr) {
    log.info("Update table t_jobs...");
    db.serialize(function() {  
	db.run("UPDATE t_jobs SET taskId = ?,state=?,submitCount=?,duration=?,location=? WHERE id = ?", arr);
    });
}

exports.updateProjectById = function (arr) {
    log.info("Update table t_projects...");
    db.serialize(function() {  
	db.run("UPDATE t_projects SET state = ?,ifMessage=?,successCount=? WHERE id = ?", arr);
    });
}


initDB();

