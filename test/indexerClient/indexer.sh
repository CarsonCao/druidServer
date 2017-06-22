#!/bin/bash

subProjId=$1
projId=17
userId=$2
mFeatures=$3
mFilesId=$4
mHeader=$5
hdfsUrl=hdfs://masters
dataPath=/user/hive/warehouse/kaishu.db
granularity=month
intervals=$6

template=./templates/indexer_all.json
dst=./workSpace/${subProjId}_indexer.json

cat ${template} | awk '$0 !~ /^\s*#.*$/' | sed 's/[ "]/\\&/g' |
while read -r line;do
    eval echo ${line}
done >${dst}

#curl -X 'POST' -H 'Content-Type:application/json' -d @${dst} localhost:3000/druid/indexer/create



