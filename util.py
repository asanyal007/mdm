import pandas as pd
import json
import requests
import uuid


def load_config_file(config_data):
    """This method helps us Loading configuration file data
    Args:
        config_data: the json config data
    returns :
        array : the configuration json
    """
    with open(config_data) as data:
        config = json.load(data)
    return config


def slack_web_hook(data):
    web_hook = 'https://hooks.slack.com/services/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'
    json_data = {'text': data}
    return requests.post(url=web_hook, json=json_data)


def unique_id():
    return uuid.uuid4()
