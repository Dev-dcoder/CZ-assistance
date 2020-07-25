import redis

ujjwal = "True"

red_conf={
'host': 'localhost',
'port':'6379',
'db':0
}


Mail_classifier_api = "http://<servername>:10101/classifier/v1/classify"
Cz_sentiment_api = "http://server IP:port/sentiment/v1/analyzer"

r_queue = "cz_assist_que" 
#write_quue=
"""
class conn(self,host,port,db):
    def __init__():
        host = self.host
        port = self.port
        db = self.db
        initialize_con()
    def initialize_con(self):
        conn = redis.Redis(host=host,port=port,db=db,encoding=utf8,retry_on_timeout=True)
        return conn

    def re_initialize_con(self):
"""
