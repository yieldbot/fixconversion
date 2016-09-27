# run this in docker container where datapipejobs is running
#ssh skawane@platform-apps-mesos-slave-3
#sudo docker ps
#sudo docker exec -t -i e06884ba11fb bash
#copy local file to docker
#tar -cv * | sudo docker exec -i e06884ba11fb tar x -C /tmp
#input is fix_conversion log files which had logs for Found parent-info with ri and server timestamps
#scan this file to get approx timestamp and look for them in 17 index
#save them in first pass
#save same events twice as they get matched based on sts

#remove dups and save
#sort adevents-2016-09-17-conv.txt | uniq -c >sorted-adevents-2016-09-17-conv.txt  

#delete thode in second pass


#dev
#skawane@platform-apps-mesos-slave-5
#scp  kafka_recs.txt skawane@platform-apps-mesos-slave-4:/home/skawane/
#sudo HOME=/apps/mesos/mesos-slave/ docker pull docker.yb0t.cc/datapipejobs:0.1.101 
#sudo HOME=/apps/mesos/mesos-slave/ docker run -it docker.yb0t.cc/datapipejobs:0.1.101   bash

#sudo docker ps
#sudo docker exec -t -i b047fd7d1b3d bash
#scp  kafka_recs.txt skawane@platform-apps-mesos-slave-5:/home/skawane/
#tar -cv *| sudo docker exec -i b047fd7d1b3d tar x -C /tmp

#sort adevents-2016-09-17-conv.txt | uniq -c >sorted-adevents-2016-09-17-conv.txt  

#in /tmp
#to get ids for deletion
#awk '!seen[$0]++' ids_adevents-2016-09-17-conv.txt > sorted_ids
#curl -s -XPOST http://analytics-standalone.elasticsearch.service.us-east-1-z.consul:9200/adevents-2016-09-17/conversion/_bulk --data-binary "@tmp_ids"; echo 
#curl -s -XPOST http://analytics-standalone.elasticsearch.service.us-east-1-z.consul:9200/adevents-2016-09-17/conversion/_bulk --data-binary "@sorted_ids"; echo

#coldevents
#sort cold-adevents-2016-09-17-conv.txt | uniq -c >sorted-cold-adevents-2016-09-17-conv.txt  
#in /tmp
#to get ids for deletion
#awk '!seen[$0]++' ids_cold_adevents-2016-09-17-conv.txt > sorted_cold_ids
#
#curl -s -XPOST http://analytics-coldevents.elasticsearch.service.us-east-1.consul:9200/adevents-2016-09-17/conversion/_bulk --data-binary "@tmp_ids"; echo 
#curl -s -XPOST http://analytics-coldevents.elasticsearch.service.us-east-1.consul:9200/adevents-2016-09-17/conversion/_bulk --data-binary "@sorted_cold_ids"; echo

it2cg4aj7fqdl74zs6_1474140391691

import datetime
import requests
import json

from elasticsearch import Elasticsearch
from datetime import date, datetime, timedelta
from elasticsearch.exceptions import NotFoundError

from ybot.utility import dates, slack, esutil
from elasticsearch.exceptions import NotFoundError


from ybot.utility import dates, slack
from ybot.utility.auth_token import apply_auth_token
from ybot.tasks import manager
from ybot.tasks.common import BaseTask, TaskFailed
from copy import copy
from ybot.dbinfo import nested_attr, Tasks

from ybot.aws import s3
from boto.exception import S3ResponseError
from boto.s3.connection import *
from boto.s3.key import Key
import gzip
import os
import httplib
from ybot import getLogger
log = getLogger( __name__ )

from time import gmtime, mktime, timezone
def datetime_to_epoch_millis(dt):
    """return datetime in utc to epoch milliseconds, even though the milliseconds are actually zeroed"""
    if dt is None:
        return None
    return int( ( mktime(dt.utctimetuple()) + dt.microsecond/1000.0 - timezone) * 1000)

with open ('/tmp/cold-adevents-2016-09-17-conv.txt', 'w') as f:
	f.write ("backup of deleted docs for cold-adevents-2016-09-17\n")

def get_17_recs(isohour, ri):
	es_index = "adevents-2016-09-17" 
	url = "http://analytics-coldevents.elasticsearch.service.us-east-1.consul:9200/%s/conversion/_search"%( es_index)
	sts_lt = datetime_to_epoch_millis(isohour)
	sts_gte =  datetime_to_epoch_millis(isohour + datetime.timedelta(milliseconds=-1000))
	body = {}
	body = {}
	body["size"] = 1000
 	body["query"] = {"filtered": {"query": { "terms": { "ri": ri } }, 
 	"filter": {"bool": {"must_not":[{"exists":{"field":"uuid"}}],"must":[{"range":{"sts": {"gte": sts_gte,"lte":sts_lt ,"format":"epoch_millis"}}}]}}}} 
	#print json.dumps(body)
	resp = requests.get(url, headers={}, data=json.dumps(body))
	if resp.status_code != requests.codes.ok:  # pylint: disable=E1101
		msg = "check_perfmetrics: es_index=%s received non-2xx resp, text=%s"%(es_index, resp.text)
		print msg
		sys.exit(2)
	respjson = resp.json()
	if respjson['hits']['total'] == 0 :
		print "Document missing for ri= %s timestamp=%s to %s" %(ri, sts_gte,sts_lt)
	print "%s Document found for ri= %s timestamp=%s to %s" %(respjson['hits']['total'],ri, sts_gte,sts_lt)
	data = [doc for doc in respjson['hits']['hits']]
	for doc in data:
		with open ('/tmp/cold-adevents-2016-09-17-conv.txt', 'a') as f: f.write (str(doc)+'\n')



def del_17_recs(isohour, ri):
	es_index = "adevents-2016-09-17" 
	url = "http://analytics-coldevents.elasticsearch.service.us-east-1.consul:9200/%s/conversion/_search"%( es_index)
	sts_lt = datetime_to_epoch_millis(isohour)
	sts_gte =  datetime_to_epoch_millis(isohour + datetime.timedelta(milliseconds=-1000))
	body = {}
	body = {}
	body["size"] = 1000
 	body["query"] = {"filtered": {"query": { "terms": { "ri": ri } }, 
 	"filter": {"bool": {"must_not":[{"exists":{"field":"uuid"}}],"must":[{"range":{"sts": {"gte": sts_gte,"lte":sts_lt ,"format":"epoch_millis"}}}]}}}} 
	#print json.dumps(body)
	resp = requests.get(url, headers={}, data=json.dumps(body))
	if resp.status_code != requests.codes.ok:  # pylint: disable=E1101
		msg = "check_perfmetrics: es_index=%s received non-2xx resp, text=%s"%(es_index, resp.text)
		print msg
		sys.exit(2)
	respjson = resp.json()
	if respjson['hits']['total'] == 0 :
		print "Document missing for ri= %s timestamp=%s to %s" %(ri, sts_gte,sts_lt)
	#print "%s Document found for ri= %s timestamp=%s to %s" %(respjson['hits']['total'],ri, sts_gte,sts_lt)
	data = [doc for doc in respjson['hits']['hits']]
	for doc in data:
		print doc["_id"]
		tid = doc["_id"]
		#'http://analytics-standalone.elasticsearch.service.us-east-1-z.consul:9200/adevents-2016-09-17/conversion/_bulk' -d '{ "delete" : { "_id" : "it2vlrizn7oealadgo_1474143306893"}}
		#stmt = "curl http://analytics-standalone.elasticsearch.service.us-east-1-z.consul:9200/adevents-2016-09-17/conversion/_bulk -d { \"delete\" : { _id :" + tid+ "}} "
		#exec( stmt)
		#print stmt
		stmt = "{ \"delete\" : { \"_id\" : \"" + tid +"\" } }"
		print stmt
		with open ('/tmp/ids_cold_adevents-2016-09-17-conv.txt', 'a') as f: f.write (str(stmt)+'\n')

with open('/tmp/kafka_recs.txt') as f:
	cnt = 0
	for line in f:
		line = line.rstrip('\n')
		coll = line.split(" ")
		yr = int(coll[0].split("-")[0])
		mon = int(coll[0].split("-")[1])
		day = int(coll[0].split("-")[2])
		hr =  int(((coll[1].split(","))[0]).split(":")[0])
		mins	=  int(((coll[1].split(","))[0]).split(":")[1])
		sec =  int(((coll[1].split(","))[0]).split(":")[2])
		msec = int((coll[1].split(","))[1])
		ri = [coll[10]]
		isohour = datetime.datetime(yr,mon,day,hr,mins,sec,msec)
		#get_17_recs(isohour, ri)
		del_17_recs(isohour, ri)
		cnt+=1
	print cnt


