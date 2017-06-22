"use strict";
/**
 *author:Carson Cao
 *date:  2017/5/17  
 **/

var zookeeper = require('node-zookeeper-client');
var cache = require('./local-storage');
var constants = require('../config/constants');
var Log = require('log')
,log = new Log('info');

var zkClient = zookeeper.createClient(constants.ZK_HOSTS);
cache.setItem(constants.DRUID_BROKER_ROUTE_KEY, []);

function connect () {
    zkClient.connect();

    zkClient.once('connected', function() {
        log.info('Connected to ZooKeeper.');
        getDruidBrokerServices(constants.DRUID_BROKER_ZK_PATH);
        getDruidOverlordServices(constants.DRUID_OVERLORD_ZK_PATH);
        getDruidCoordinateServices(constants.DRUID_COORDINATE_ZK_PATH);
    });
}

/**
 *Get broker hosts
 * */
function getDruidBrokerServices(path) {
    zkClient.getChildren(
        path,
        function(event) {
            log.info('Got Services watcher event: %s', event);
            getDruidBrokerServices(constants.DRUID_BROKER_ZK_PATH);
        },
        function(error, children, stat) {
            if (error) {
                log.error(
                    'Failed to list children of %s due to: %s.',
                    path,
                    error
                );
                return;
            }
            // listing hosts,get host information
            cache.setItem(constants.DRUID_BROKER_ROUTE_KEY, []);
            children.forEach(function(item) {
                //console.log(item);
                getDruidHostService(path + '/' + item, "broker");
            });

        }
    );
}

/**
 *  *Get overlord hosts
 *   * */
function getDruidOverlordServices(path) {
    zkClient.getChildren(
        path,
        function(event) {
            log.info('Got Services watcher event: %s', event);
            getDruidBrokerServices(constants.DRUID_OVERLORD_ZK_PATH);
        },
        function(error, children, stat) {
            if (error) {
                log.error(
                    'Failed to list children of %s due to: %s.',
                    path,
                    error
                );
                return;
            }
            cache.setItem(constants.DRUID_OVERLORD_ROUTE_KEY, []);
            children.forEach(function(item) {
                getDruidHostService(path + '/' + item, "overlord");
            });
        }
    );
}


/**
 *  Get coordinator hosts
 **/
function getDruidCoordinateServices(path) {
    zkClient.getChildren(
        path,
        function(event) {
            log.info('Got Services watcher event: %s', event);
            getDruidBrokerServices(constants.DRUID_COORDINATE_ZK_PATH);
        },
        function(error, children, stat) {
            if (error) {
                log.error(
                    'Failed to list children of %s due to: %s.',
                    path,
                    error
                );
                return;
            }
            cache.setItem(constants.DRUID_COORDINATE_ROUTE_KEY, []);
            children.forEach(function(item) {
                getDruidHostService(path + '/' + item, "coordinate");
            });
        }
    );
}


/**
 *  *get host information （IP,Port）
 *   */
function getDruidHostService(path,type) {
    zkClient.getData(
        path,
        function(event) {
            log.info('Got Serivce watcher event: %s', event);
            getDruidHostService(path);
        },
        function(error, data, stat) {
            if (error) {
                log.error(
                    'Failed to get data of %s due to: %s.',
                    path,
                    error
                );
                return;
            }
            var json = JSON.parse(data.toString('utf8'));
            var address = json.address;
            var port = json.port;
            if (type == "broker"){
                var oriBrokerList = cache.getItem(constants.DRUID_BROKER_ROUTE_KEY);
                cache.setItem(constants.DRUID_BROKER_ROUTE_KEY, oriBrokerList.concat([address+':'+port]));
                log.info("Adding broker host "+address +":" + port +" to cacheDB");
            } else if (type == "overlord"){
                var oriOverloadList = cache.getItem(constants.DRUID_OVERLORD_ROUTE_KEY);
                cache.setItem(constants.DRUID_OVERLORD_ROUTE_KEY, oriOverloadList.concat([address+':'+port]));
                log.info("Adding overlord host "+address +":" + port +" to cacheDB");
            } else if (type = "coordinate"){
                var oriCoordinateList = cache.getItem(constants.DRUID_COORDINATE_ROUTE_KEY);
                cache.setItem(constants.DRUID_COORDINATE_ROUTE_KEY, oriCoordinateList.concat([address+':'+port]));
                log.info("Adding coordinator host "+address +":" + port +" to cacheDB");
            }
        }
    );
}

module.exports = connect;
