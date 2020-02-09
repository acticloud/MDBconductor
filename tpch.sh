#!/bin/bash

set -e

: "${DELAY:=0}"
: "${QUERIES:=../tpch-scripts/03_run/??.sql}"

while true
do
	for q in $QUERIES
	do
		curl -s http://localhost:8080/query/ -F query=@$q \
			| jq .advice
	done
	sleep $DELAY
done
