from flask import Flask, request, jsonify, make_response
app = Flask(__name__)
import json

@app.route("/webhook", methods=["POST"])
def webhook() :
    _response_is = json.dumps(request.json)
    if _response_is is not "":
        try:
            session_is = str(_response_is['sessionId'])[:-3]
            sufix_is = str(_response_is['sessionId'])[-1]
            _stat_key = sufix_is+"_stat"
            _val_key = "_sessionid"+sufix_is
            r.hmset(session_is,{'_stat_key':'1','_val_key':_response_is})
        except r.ConnectionError:
            log.info(f'redis connection error')
            

    else:
        log.info(f"Response json is empty ")




    log.info(f"Response on webhook: {json.dumps(request.json, indent=2)}")
    return make_response(jsonify({"status": "got it"}), 200)
if __name__ == "__main__":
    print("direct call")
    app.run(host="0.0.0.0", port=20202)