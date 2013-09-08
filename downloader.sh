#!/bin/bash
year=2012
month=01
# day=01
if [ -z "$1" ]
then
	echo "Please tell me where to store these files."
else
	for day in $(seq 1 31)
	do
		for hour in $(seq 0 23)
		do
			if [ "$day" -lt "10" ]
			then
				wget -P "$1" -q http://data.githubarchive.org/"$year"-"$month"-0"$day"-"$hour".json.gz
			else
				wget -P "$1" -q http://data.githubarchive.org/"$year"-"$month"-"$day"-"$hour".json.gz
			fi
		done
	done
fi