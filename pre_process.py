import json


def pre_process(line) -> dict:
    int_values = ["quote_count",
                  "reply_count",
                  "retweet_count",
                  "favorite_count", ]
    line = line.decode("utf-8")
    line = line.replace("null", '"None"')
    line = line.replace("false", '"False"')
    line = line.replace("true", '"True"')
    # line = line.replace(':', '-')
    line: dict = json.loads(line)
    line.pop("source")
    line.pop("entities")
    line["user"]["description"] = str(line["user"]["description"])
    line["geo"] = str(line["geo"])
    line["in_reply_to_status_id"] = str(line["in_reply_to_status_id"])
    line["in_reply_to_user_id"] = str(line["in_reply_to_user_id"])
    # line["created_at"] = "time."
    # line["display_text_range"] = "display_text_range"
    line["truncated"] = str(line["truncated"])
    line["in_reply_to_status_id_str"] = str(line["in_reply_to_status_id_str"])
    line["coordinates"] = str(line["coordinates"])
    line["source"] = " "
    # line["entities"] = " "
    value = line
    key = {"id": line["user"]["id"]}
    for k in value:
        if int_values.__contains__(k):
            value[k] = int(value[k])
        if isinstance(value[k], str):
            value[k] = value[k].replace("'", '')
            value[k] = value[k].replace('"', '')
            value[k] = value[k].replace(':', '-')
    return {"key": key, "value": value}

