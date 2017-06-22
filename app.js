var app = require('koa')()
  , logger = require('koa-logger')
  , json = require('koa-json')
  , views = require('koa-views')
  , onerror = require('koa-onerror');

var index = require('./routes/api/indexer');
var query = require('./routes/api/query');
var users = require('./routes/ui/users');
var discovery = require('./utils/discovery');
var jobMonitor = require('./utils/job-monitor');
//server discovery
discovery();

//indexer job monitor
jobMonitor();

// error handler
onerror(app);

// global middlewares
app.use(views('views', {
  root: __dirname + '/views',
  default: 'jade'
}));

app.use(require('koa-bodyparser')());
app.use(json());
app.use(logger());

app.use(require('koa-static')(__dirname + '/public'));

// routes definition
app.use(index.routes(), index.allowedMethods());
app.use(users.routes(), users.allowedMethods());
app.use(query.routes(), query.allowedMethods());
module.exports = app;

/*
post -topn:
curl -d "threshold=10&tableName=url_ip_test&dimension=url&aggregations=count:count&metric=count&intervals=2017-05-30T00:00:00/2017-06-01T08:00:00" http://localhost:3000/druid/query/topn
post -groupBy:
curl -d "tableName=url_ip_table&dimension=ip&intervals=2015-01-05T08:00:00/2015-01-16T08:00:00" http://localhost:3000/druid/query/groupby
post -timeseries:
curl -d "tableName=url_ip_test&granularity=hour&aggregations=count:count&intervals=2017-05-30T00:00:00/2017-06-01T08:00:00" http://localhost:3000/druid/query/timeseries

get -topn:
curl -s "http://192.168.1.137:3000/druid/query/topn?threshold=10&tableName=kaishu_data-url_ip_test&dimension=url&aggregations=counts:longSum&metric=counts&intervals=2017-05-30T07:00:00/2017-06-01T08:00:00"
get -rowCount:
curl -s "http://localhost:3000/druid/query/rowcount?tableName=kaishu_data-url_ip_test&intervals=2017-05-30T07:00:00/2017-06-01T08:00:00" 
get -timeseries:
curl -s "http://localhost:3000/druid/query/timeseries?tableName=url_ip_test&granularity=hour&aggregations=count:count&intervals=2017-05-30T00:00:00/2017-06-01T08:00:00" 

*/



