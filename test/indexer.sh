#!/bin/bash

curl -X 'POST' -H 'Content-Type:application/json' -d @indexer1.json localhost:3000/druid/indexer/create
