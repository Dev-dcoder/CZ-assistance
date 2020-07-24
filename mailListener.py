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
traversable_key= set()

def con_red(**kwargs):
    """
    here we will create connection of redis .

    """ 
    r = redis.Redis(host=red_conf['host'],port=red_conf['port'],db=red_conf['db'])
    if not r.ping():
        con_red(red_conf)
    return r

def hit_in_red():
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
            stat_M = mail_resp.json()
            stat_S = sent_resp.json()
            traversable_key.add(session_id)
            if stat_M['status'] =="OK":
                sessionidM = session_id+"##M"
                try:
                    if not r.ping():
                        r = con_red(red_conf)
                    red_respM = r.hmset(session_id,{'M_stat':'0',sessionidM:''})
                    log.info(f"Mail Classifier API is returning :- {stat_M['status']}  for Message ID:-  {messageId}  Redis response is :- {red_respM}")
                except Exception as e:
                    log.error(f"Could not place Mail Classifier on redis because of {e} for Message ID:-  {messageId}")
                    log.exception("Here we are catching new exceptions ======")

            else:
                log.error(f"Mail Classifier API is returning :- {stat_M['status']}  for Message ID:-  {messageId}")
            
            if stat_S['status'] =="OK":
                sessionidS = session_id+"##S"
                try:
                    if not r.ping():
                        r = con_red(red_conf)
                    red_respS = r.hmset(session_id,{'S_stat':'0',sessionidS:''})
                    log.info(f"Sentiment API is returning :- {stat_S['status']}  for Message ID:-  {messageId}  Redis response is :- {red_respS}")
                except Exception as e:
                    log.error(f"Could not place Sentimnent on redis because of {e}  for Message ID:-  {messageId}")
                    log.exception("Here we are catching new exceptions ======")
            else:
                log.error(f"Sentiment API is returning :- {stat_S['status']}  for Message ID:-  {messageId}")
        except:
            log.info(f"Input redis packet have problem")




def monitr_in_red():
    """
    here we will check session id for both with state 1 and read packet from redis for that session id
    and create packet accordingy and write back in queue.
    after writing delete the both key that maintain state.

    """
    r = con_red(red_conf)
    while True:
        if not r.ping():
                r = con_red(red_conf)

        for acc_key in traversable_key:
            tmp_hold = r.hgetall(acc_key)
            if tmp_hold['S_stat'] and tmp_hold['M_stat']:
                try:
                    data_to_send = dict()
                    s_key = acc_key+"##S"
                    m_key = acc_key+"##M"
                    log.info("doing psckt prse")
                    resp_sent = tmp_hold[s_key]
                    resp_mail = tmp_hold[m_key]
                    mail_client_val: list = acc_key.split('##')
                    data_to_send.update({'disposition':{'dispositionVal':resp_mail[1],'dispositionCScore':resp_mail[2]},\
                                        'subdisposition':{'subdispositionVal':resp_mail[3],'subdispositionCScore':resp_mail[4]},\
                                        'sentiment':{'sentimentVal':resp_sent[1],'sentimentCScore':resp_sent[2]},\
                                        'priority':{'priority':None,'priorityCScore':None},\
                                        'remarks':None,'messageId':mail_client_val[0],'mail_id':resp_mail[6],'client_id':mail_client_val[1]})
                    dispatch_packet = json.dumps(data_to_send, default=str)
                    fn_resp = r.lpush('AIANALYSIS', dispatch_packet)
                    if fn_resp:
                        log.info(f"We have successfully placed packet in redis for message id :-{mail_client_val[0]}")
                        log.info(f"So now we are going to delete info from redis for session id :- {acc_key}")
                        r.del(acc_key)
                    else:
                        log.info(f"Could not place Packet in redis  for session id :- {acc_key}")
                        

                except Exception as e:
                    log.error(f"Got exception while creating packet for sessionis :-{acc_key}  and exception is :- {e}")


            else:
                log.info("No packet available for parsing")

def main_manager():
    """
    here we will open thread for both function so that they will run continuously.

    """
    runing_thr = list()
    thr_to_run = [monitr_in_red,hit_in_red]
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