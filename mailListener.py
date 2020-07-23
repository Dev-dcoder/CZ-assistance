import redis
import threading
import time
import json
import requests
import logging
import pylint
from setting import *


log = logging.getLogger("Mail-Sent Wrapper")

logging.basicConfig( \
    level=logging.DEBUG,\
    format="%(asctime)s - %(name)s - %(funcName)7s %(levelname)5s - %(lineno)3s - %(message)s",\
    filename="/var/log/mail-sentmnt.log" \
)


mail_jsn= dict()
sent_jsn= dict()

def con_red(**kwargs):
    """
    here we will create connection of redis .

    """ 
    r = redis.Redis(host=red_conf['host'],port=red_conf['port'],db=red_conf['db'])
    if not r.ping():
        con_red(red_conf)
    return r

def _hit_in_red():
    """
    here we will wrrite code to hit both api and mark state as 0 with session id as key for both api.

    """
    r = con_red(red_conf)
    while 1:

        try:
            if not r.ping():
                r = con_red(red_conf)
            data = r.rpop(r_queue)
            packet = json.loads(data)
            textData = packet['mailBody']
            messageId = packet['messageid']
            mail_id = packet['mail_id']
            client_id = packet['client_id']
            session_id = messageId+"##"+client_id
            webhook = "http://172.16.2.19/webhook"
            mail_jsn = {'mailBody':textData,'appId':'1234','sessionId':session_id+"##M",
                            'optionalKeys':mail_id,'webhooks':[{'url':webhook,'method':'post'}]}
            sent_jsn = {'textData':textData,'appId':'1234','sessionId':session_id+"##S",'SentimentLanguage':'en',
                            'webhooks':webhook,'optionalKeys':mail_id}
            mail_jsn_is = json.dumps(mail_jsn)
            sent_jsn_is = json.dumps(sent_jsn)
            mail_resp = requests.post(url=Mail_classifier_api,data=mail_jsn_is)
            sent_resp = requests.post(url=Cz_sentiment_api,data=sent_jsn_is)
            _stat_M = mail_resp.json()
            _stat_S = sent_resp.json()
            
            if _stat_M['status'] =="OK":
                _sessionidM = session_id+"##M"
                try:
                    if not r.ping():
                        r = con_red(red_conf)
                    red_respM = r.hmset(session_id,{'M_stat':'0','_sessionidM':''})
                    log.info(f"Mail Classifier API is returning :- {_stat_M['status']}  for Message ID:-  {messageId}  Redis response is :- {red_respM}")
                except Exception as e:
                    log.error(f"Could not place Mail Classifier on redis because of {e} for Message ID:-  {messageId}")
                    log.exception("Here we are catching new exceptions ======")

            else:
                log.error(f"Mail Classifier API is returning :- {_stat_M['status']}  for Message ID:-  {messageId}")
            
            if _stat_S['status'] =="OK":
                _sessionidS = session_id+"##S"
                try:
                    if not r.ping():
                        r = con_red(red_conf)
                    red_respS = r.hmset(session_id,{'S_stat':'0','_sessionidS':''})
                    log.info(f"Sentiment API is returning :- {_stat_S['status']}  for Message ID:-  {messageId}  Redis response is :- {red_respM}")
                except Exception as e:
                    log.error(f"Could not place Sentimnent on redis because of {e}  for Message ID:-  {messageId}")
                    log.exception("Here we are catching new exceptions ======")
            else:
                log.error(f"Sentiment API is returning :- {_stat_S['status']}  for Message ID:-  {messageId}")
        except:
            log.info(f"Input redis packet have problem")




def monitr_in_red():
    """
    here we will check session id for both with state 1 and read packet from redis for that session id
    and create packet accordingy and write back in queue.
    after writing delete the both key that maintain state.

    """
def main_manager():
    """
    here we will open thread for both function so that they will run continuously.

    """
    runing_thr = list()
    thr_to_run = ['monitr_in_red','_hit_in_red']
    while 1:
        try:

            for i in thr_to_run:
                if i not in runing_thr:
                    th = threading.Thread(name=i,target=i,daemon=True,args=())
                    th.start()
                    runing_thr.append(i)
        except:
            log.info("exception in thread opening")

if __name__=="__main__":
    main_manager()