#!/bin/bash

set -e

: "${DELAY:=0}"
: "${QUERIES:=../tpch-scripts/03_run/??.sql}"

while true
do
	for q in $(ls $QUERIES | shuf )
	do
		qq="$(basename "$q")"
		reply="$(curl -s http://localhost:8080/query/ -F query=@$q | jq .advice)"
		echo  "q${qq%.sql} $reply"
	done
	sleep $DELAY
done
