import requests

# String manipulations
import urllib.parse


import os
import dotenv
from main import DOTENV_PATH


if os.path.exists(DOTENV_PATH):
    dotenv.load_dotenv(DOTENV_PATH)

HOST = "https://api-gandiva.s-stroy.ru"

def get_access_token(username, password):
    
    endpoint = "/Token"
    url = HOST + endpoint

    body = {
        "grant_type": "password",
        "username": username,
        "password": password
    }
    body = urllib.parse.urlencode(body)

    headers = {
    'Content-Type': 'application/x-www-form-urlencoded',
    }

    response = requests.request("POST", url, headers=headers, data=body)

    print(response.text)
