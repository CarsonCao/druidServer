"use strict";
/**
 *  author: Carson Cao
 *  date:   2017/5/17  
 **/


function define(name, value) {
    Object.defineProperty(exports, name, {
        value : value,
        enumerable : true
    });
}
define('ZK_HOSTS','bigdata04:2181,bigdata05:2181,bigdata06:2181');
define('DRUID_BROKER_ZK_PATH','/druid/discovery/druid_cluster:broker');
define('DRUID_OVERLORD_ZK_PATH','/druid/discovery/druid_cluster:overlord');
define('DRUID_COORDINATE_ZK_PATH','/druid/discovery/druid_cluster:coordinator');
define('DRUID_BROKER_ROUTE_KEY','DRUID_QUERY_SERVER_HOST');
define('DRUID_OVERLORD_ROUTE_KEY','DRUID_INDEXER_SERVER_HOST');
define('DRUID_COORDINATE_ROUTE_KEY','DRUID_SEGMENT_SERVER_HOST');

define('DRUID_QUERY_SERVER_URL','/druid/v2/?pretty');
define("DRUID_QUERY_TOPN_MINTHRESHOLD",1000);
define('DRUID_INDEXER_SERVER_URL','/druid/indexer/v1/task');
define("DRUID_INDEXER_TIMEOUT",3600000);
define("INDEX_HADOOP_JSON_TEMPLATE","indexHadoopJson.ejs");
define("INDEX_DEFAULT_QUERYGRANULARITY","none");
define("INDEX_DEFAULT_HADOOP_VERSION","2.6.0");

define("DEFAULT_DATA_FORMAT_TYPE","column");//column,row
define("RABBITMQ_QUEUE_ADDRESS","amqp://admin:adminkaishu@192.168.1.211");
define("RABBITMQ_EXCHANGE_NAME","campass.source");
define("RABBITMQ_EXCHANGE_TYPE","topic");
define("RABBITMQ_RESULT_ROUTING_KEY","file.result.load");
define("RABBITMQ_MESSAGE_TEMPLATE",
        "{\n" +
        "    \"status\": {\n" +
        "        \"code\": 0, \n" +
        "        \"message\": \"OK\"\n" +
        "    },\n" +
        "    \"task\": {\n" +
        "      \"user_id\": <%- userId %>,\n" +
        "      \"project_id\": <%- projectId %>,\n" +
        "      \"features\":\"<%- features %>\",\n" +
        "      \"file_ids\": [<%- filesId %>],\n" +
        "      \"header\": \"<%- header %>\"\n" +
        "    },\n" +
        "    \"result\": {\n" +
        "      \"mysql_host\": \"bigdata08\",\n" +
        "      \"mysql_port\": 3010,\n" +
        "      \"mysql_db\": \"druid\"\n" +
        "    }\n" +
        "  }");

define("TOPN_JSON_TEMPLATE",
            "{\n" +
            "  \"queryType\" : \"topN\",\n" +
            "  \"dataSource\" : \"<%- tableName %>\",\n" +
            "  \"intervals\" : [\"<%- intervals %>\"],\n" +
            "  \"granularity\" : <%- granularity %>,\n" +
            "  \"dimension\" : \"<%- dimension %>\",\n" +
            "  <%-filters%> \n" +
            "  \"metric\" : \"<%- metric %>\",\n" +
            "  \"threshold\" : <%- threshold %>,\n" +
            "  \"aggregations\" : <%- aggregations %>,\n" +
            "  \"context\" : {\"minTopNThreshold\":<%- minTopNThreshold %>}\n" +
            "}");

define("TIMEBOUND_JSON_TEMPLATE",
            "{\n" +
            "    \"queryType\" : \"timeBoundary\",\n"+
            "    \"dataSource\": \"<%- tableName %>\""+
            "    <%- filters %>\n"+
            "    <%- bound %>\n" +
            "}");
define("ROWCOUNT_JSON_TEMPLATE",
            "{\n" +
            "  \"queryType\": \"timeseries\",\n" +
            "  \"dataSource\": \"<%- tableName %>\",\n" +
            "  \"granularity\": \"all\",\n" +
            "  \"descending\": \"false\",\n" +
            "  \"aggregations\": [\n" +
            "    { \"type\": \"count\", \"name\": \"count\" }\n" +
            "  ],\n" +
            "  \"intervals\": [ \"<%- intervals %>\" ]\n" +
            "}");

define("TIMESERIES_JSON_TEMPLATE",
            "{\n " +
            "  \"queryType\": \"timeseries\",\n" +
            "  \"dataSource\": \"<%- tableName %>\",\n" +
            "  \"granularity\": <%- granularity %>,\n" +
            "  <%-filters%> \n" +
            "  \"descending\": \"<%- ifDescending %>\",\n" +
            "  \"aggregations\" : <%- aggregations %>,\n" +
            "  \"intervals\": [ \"<%- intervals %>\" ],\n" +
            "  \"context\" : {\"skipEmptyBuckets\": \"false\"}" +
            "}");
define("QUERY_GRANULARITY_TEMPLATE","{\"type\": \"<%-granularityType %>\", \"<%-granularityType %>\": \"<%- granularityValue %>\"}");
define("QUERY_GRANULARITY_TEMPLATE2","{\"type\": \"<%-granularityType %>\", \"<%-granularityType %>\": \"<%- granularityValue %>\",\"timeZone\": \"<%-timeZone %>\"}");
define("INDEX_METRICSSPEC_COUNT",
            "       {\n" +
            "          \"name\" : \"<%-name%>\",\n" +
            "          \"type\" : \"count\"\n" +
            "        }");
define("INDEX_METRICSSPEC_TIMEMAX",
            "       {\n" +
            "          \"name\" : \"tmax\",\n" +
            "          \"type\" : \"timeMax\",\n" +
            "          \"fieldName\" : \"<%- timeColumn %>\"\n" +
            "          <%- indexTimeFormat %>"+
            "        }");
define("INDEX_METRICSSPEC_TIMEMIN",
            "       {\n" +
            "          \"name\" : \"tmin\",\n" +
            "          \"type\" : \"timeMin\",\n" +
            "          \"fieldName\" : \"<%- timeColumn %>\"\n" +
            "          <%- indexTimeFormat %>"+
            "        }");
define("INDEX_METRICSSPEC_LONGSUM",
            "        {\n" +
            "          \"name\" : \"<%- name %>\",\n" +
            "          \"type\" : \"longSum\",\n" +
            "          \"fieldName\" : \"<%- colName %>\"\n" +
            "        }");
define("INDEX_METRICSSPEC_DOUBLESUM",
            "        {\n" +
            "          \"name\" : \"<%- name %>\",\n" +
            "          \"type\" : \"doubleSum\",\n" +
            "          \"fieldName\" : \"<%- colName %>\"\n" +
            "        }");
define("INDEX_METRICSSPEC_HYPERUNIQUE",
            "        {\n" +
            "          \"type\" : \"hyperUnique\",\n" +
            "          \"name\" : \"<%- name %>\",\n" +
            "          \"fieldName\" : \"<%- colName %>\"\n" +
            "        }");
define("INDEX_PARSESPEC_TSV",
        "          \"format\" : \"tsv\",\n" +
        "          \"dimensionsSpec\" : {\n" +
        "            \"dimensions\" : [<%- dimensions %>]\n" +
        "          },\n" +
        "          \"delimiter\":\"<%- delimiter %>\",\n" +
        "          \"listDelimiter\" : \"<%- listDelimiter %>\",\n" +
        "          \"columns\" : [<%- columns %>],\n" +
        "          \"timestampSpec\" : {\n" +
        "            \"column\" : \"<%- timeColumn %>\"<%- indexTimeFormat %>" +
        "          }");
define("INDEX_PARSESPEC_CSV",
        "          \"format\" : \"csv\",\n" +
        "          \"dimensionsSpec\" : {\n" +
        "            \"dimensions\" : [<%- dimensions %>]\n" +
        "          },\n" +
        "          \"columns\" : [<%- columns %>],\n" +
        "          \"timestampSpec\" : {\n" +
        "            \"column\" : \"<%- timeColumn %>\"<%- indexTimeFormat %>" +
        "          }");
define("INDEX_TIMEFORMAT",
        ",\n" +
        "            \"format\" : \"<%- timeFormat %>\"\n");
define("INDEX_METRIC_TIMEFORMAT",
        ",\n" +
        "            \"timeFormat\" : \"<%- timeFormat %>\"\n");




