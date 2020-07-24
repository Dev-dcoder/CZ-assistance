import json
import logging
from setting import red_conf
from flask import Flask, request, jsonify, make_response
from mailListener import con_red

log = logging.getLogger(__name__)
app = Flask(__name__)

r = con_red(red_conf)


@app.route("/webhook", methods=["POST"])
def webhook():
    response_is = json.loads(request.json)
    if response_is:
        try:
            session_is = str(response_is["sessionId"])[:-3]
            sufix_is = str(response_is["sessionId"])[-1]
            stat_key = sufix_is + "_stat"
            val_key = session_is + sufix_is
            r.hmset(session_is, {stat_key: "1", val_key: response_is})
        except r.ConnectionError:
            log.info(f"redis connection error")

    else:
        log.info(f"Response json is empty ")

    log.info(f"Response on webhook: {json.dumps(request.json, indent=2)}")
    return make_response(jsonify({"status": "got it"}), 200)


if __name__ == "__main__":
    print("direct call")
    app.run(host="0.0.0.0", port=20202)
