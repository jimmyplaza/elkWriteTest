from elasticsearch import Elasticsearch
from elasticsearch import helpers
import elasticsearch
import datetime

elk_host = "localhost"
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
    global elk_index
    global elk_type

    es = Elasticsearch([{'host': elk_host, 'port': elk_port}])
    writeCount = 300000

    worker(es, writeCount)




if __name__=="__main__":
    main()
