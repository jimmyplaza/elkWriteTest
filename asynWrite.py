from elasticsearch import Elasticsearch
from elasticsearch import helpers
import datetime
import asyncio

import elasticsearch
import tqdm

import sys

elk_host = "10.240.0.43"
elk_port = "9200"
elk_index = "jimmy_index3"
elk_type = "jimmy_type3"



async def worker(es, writeCount):
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

    await runElk(es, bulkArray)
        #try:
            ##res = es.index(index=elk_index, doc_type=elk_type, id=i, body=doc)
            ##res = es.index(index=elk_index, doc_type=elk_type, body=doc)
            #await runElk(elk_index, elk_type, doc)
            ##print ("Save to ELK: %r" % res['created'])
        #except elasticsearch.ElasticsearchException as e:
            #print (e)
            #print ('Failed to save to ELK')

async def runElk(es, bulkArray):
    if len(bulkArray) >0:
        helpers.bulk(es, bulkArray)

    #try:
        #es.index(index=elk_index, doc_type=elk_type, body=doc)
    #except elasticsearch.ElasticsearchException as e:
        #print (e)
        #print ('Failed to save to ELK')

#@asyncio.coroutine
#def wait_with_progress(coros):
    #for f in tqdm.tqdm(coros, total=len(coros), desc="Progress: ", smoothing=0.5):
        #yield from f


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
		"index": "no", 
		"include_in_all": "false",
                "type":"text",
		"doc_values": "false",
            },
            "address":{
		"index": "no",
		"include_in_all": "false",
                "type":"text",
		"doc_values": "false",
            },
            "number":{
		"index": "not_analyzed",
		"include_in_all": "false",
                "type": "integer",
		"doc_values": "false",
            },
            "timestamp": {
		"index": "not_analyzed",
		"include_in_all": "false",
                "type": "date",
		"doc_values": "false",
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


#    sys.exit(0)

    writeArray = [100000, 100000, 100000]

    loop = asyncio.get_event_loop()
    tasks = [worker(es, writeCount) for writeCount in writeArray]
    loop.run_until_complete(asyncio.gather(*tasks))
    loop.close()

    #loop.run_until_complete(wait_with_progress(tasks))


if __name__=="__main__":
    main()



