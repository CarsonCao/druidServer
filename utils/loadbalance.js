'use strict';
/**
 *  author: Carson Cao
 *  date:   2017/5/17  
 **/

var HashRing = require('hashring');
var constants = require('../config/constants');
var cache = require('./local-storage');
var Log = require('log')
,log = new Log('info');

/**
 * Broker load balancing using consistency hash
 */
exports.getDruidBrokerUrl = function(factor) {
    var brokerList = cache.getItem(constants.DRUID_BROKER_ROUTE_KEY);
    log.info("Starting broker load-balance.");
    log.info("Current broker list : [ "+brokerList+" ]. The factor is \'"+factor+"\'.");
    if (brokerList.length <= 0) {
        log.error('No broker nodes are available !!!!!');
        return;    
    }
    var ring = new HashRing(
            brokerList, 
            'md5',
            {'max cache size' : 1000}
        );
    var luckyHost = ring.get(factor);
    log.info('Broker host '+luckyHost+" was chosen.");

    return 'http://'+luckyHost+constants.DRUID_QUERY_SERVER_URL;
}

/**
 *  * overlord load balancing,return first host
 *   */
exports.getDruidOverlordUrl = function() {
    var overlordList = cache.getItem(constants.DRUID_OVERLORD_ROUTE_KEY);
    log.info("Starting overlord load-balance.");
    log.info("Current overlord list : [ "+overlordList+" ]. ");
    if (overlordList.length <= 0) {
        log.error('No overlord are available !!!!!');
        return;
    }
    var luckyHost = overlordList[0];
    log.info('Overlord host '+luckyHost+" was chosen.");

    return 'http://'+luckyHost+constants.DRUID_INDEXER_SERVER_URL;
}


