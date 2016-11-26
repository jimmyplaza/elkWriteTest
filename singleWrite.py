from elasticsearch import Elasticsearch
import elasticsearch
import datetime


def main():
    #host = "localhost"
    host = "104.198.87.194"
    port = "9200"
    elk_index = "jimmy_index"
    elk_type = "jimmy_type"

    es = Elasticsearch([{'host': host, 'port': port }])
    writeCount = 3000

    for i in range(writeCount):
        doc = {
            'author': 'jimmy',
            'number': i,
            'timestamp': datetime.datetime.now(),
        }
        try:
            res = es.index(index=elk_index, doc_type=elk_type, id=i, body=doc)
            #print ("Save to ELK: %r" % res['created'])
        except elasticsearch.ElasticsearchException as e:
            print (e)
            print ('Failed to save to ELK')

if __name__=="__main__":
    main()
