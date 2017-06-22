#!/bin/bash

curl -d "threshold=10&tableName=kaishu_data-url_ip_test&dimension=url&aggregations=count:count&metric=count&intervals=2017-05-30T00:00:00.000Z/2017-06-01T08:00:00.000Z" http://192.168.1.137:3000/druid/query/topn

#curl -d "threshold=10&tableName=kaishu_data-url_ip_test&dimension=url&aggregations=counts:longSum&metric=counts&intervals=2017-05-30T07:00:00/2017-06-01T08:00:00" http://192.168.1.137:3000/api/query/topn
