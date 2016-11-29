from elasticsearch import Elasticsearch
from elasticsearch import helpers
import datetime
import asyncio

import elasticsearch
import tqdm

import sys

elk_host = "10.240.0.43"
elk_port = "9200"
elk_index = "jimmy_index"
elk_type = "jimmy_type"



def worker(es, writeCount):
    global elk_index
    global elk_type

    bulkArray = []

    for i in range(writeCount):
        doc = {
        "_index": elk_index,
        "_type": elk_type,
        "_source": {
            'author': 'jimmy',
            'address': 'tw',
            'number': i,
            'timestamp': datetime.datetime.now(),
            }
        }
        bulkArray.append(doc)

    runElk(es, bulkArray)

def runElk(es, bulkArray):
    if len(bulkArray) >0:
        helpers.bulk(es, bulkArray)

def main():
    es = Elasticsearch([{'host': elk_host, 'port': elk_port }])

    settings = {
	 "index": {
	  "number_of_replicas" : 2,
	  "number_of_shards" : 3
	}
    }

    mapping = {
        elk_type: {
        "properties":{
            "author":{
                "type":"text",
            },
            "address":{
                "type":"text",
            },
            "number":{
                "type": "integer",
            },
            "timestamp": {
                "type": "date",
            }
        }
        }
    }



    try:
        es.indices.delete(index=elk_index, ignore= [400, 404])
        es.indices.create(index=elk_index, ignore= 400, body=settings)
        es.indices.put_mapping(index=elk_index, doc_type=elk_type, body=mapping)

    except elasticsearch.ElasticsearchException as e:
        print (e)
        print ('Failed to save to ELK')


    writeCount = 300000

    worker(es, writeCount)



if __name__=="__main__":
    main()



