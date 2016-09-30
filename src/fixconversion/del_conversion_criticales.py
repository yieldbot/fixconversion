##TO REMOVE FROM 12,13,14


# run this in docker container where datapipejobs is running
#ssh skawane@platform-apps-mesos-slave-3
#sudo docker ps
#sudo docker exec -t -i e06884ba11fb bash

#execute function to save ids
#in /tmp
#awk '!seen[$0]++' critical-12-psnNull.txt > sorted_critical_ids_12
#curl -s -XPOST http://criticales.elasticsearch.service.us-east-1.consul:9200/adevents-2016-09-12/conversion/_bulk --data-binary "@sorted_critical_ids_12"; echo

#awk '!seen[$0]++' critical-13-psnNull.txt > sorted_critical_ids_13
#curl -s -XPOST http://criticales.elasticsearch.service.us-east-1.consul:9200/adevents-2016-09-13/conversion/_bulk --data-binary "@sorted_critical_ids_13"; echo

#awk '!seen[$0]++' critical-14-psnNull.txt > sorted_critical_ids_14
#curl -s -XPOST http://criticales.elasticsearch.service.us-east-1.consul:9200/adevents-2016-09-14/conversion/_bulk --data-binary "@sorted_critical_ids_14"; echo


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


def get_psnNull_recs():
	es_index = "adevents-2016-09-14" 
	url = "http://criticales.elasticsearch.service.us-east-1.consul:9200/%s/conversion/_search"%( es_index)
	sts_lt = "1473886800000"
	sts_gte =  "1473800000000"
	body = {}
	body = {}
	body["size"] = 8000
 	body["query"] = {"filtered": {"filter": {"bool": {"must_not":[{"exists":{"field":"psn"}}],"must":[{"range":{"sts": {"gte": sts_gte,"lt":sts_lt ,"format":"epoch_millis"}}}]}}}}
	#print json.dumps(body)
	resp = requests.get(url, headers={}, data=json.dumps(body))
	if resp.status_code != requests.codes.ok:  # pylint: disable=E1101
		msg = "check_perfmetrics: es_index=%s received non-2xx resp, text=%s"%(es_index, resp.text)
		print msg
		sys.exit(2)
	respjson = resp.json()
	if respjson['hits']['total'] == 0 :
		print "Document missing for 13 timestamp=%s to %s" %( sts_gte,sts_lt)
	print "%s Document found for 13 timestamp=%s to %s" %(respjson['hits']['total'], sts_gte,sts_lt)
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
		with open ('/tmp/critical-13-psnNull.txt', 'a') as f: f.write (str(stmt)+'\n')




