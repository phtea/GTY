import requests
import asyncio

# String manipulations
import urllib.parse

import os
import dotenv
import json

DOTENV_PATH = os.path.join(os.path.dirname(__file__), os.pardir)
DOTENV_PATH = os.path.join(DOTENV_PATH, '.env')
if os.path.exists(DOTENV_PATH):
    dotenv.load_dotenv(DOTENV_PATH)

GAND_ACCESS_TOKEN   = os.environ.get("GAND_ACCESS_TOKEN")
GAND_REFRESH_TOKEN   = os.environ.get("GAND_REFRESH_TOKEN")
GAND_PASSWORD       = os.environ.get("GAND_PASSWORD")
GAND_LOGIN          = os.environ.get("GAND_LOGIN")

# TODO: before each request add token regeneration if token expired

# Handle not enough data in .env
if not (GAND_PASSWORD and GAND_LOGIN):
    raise ValueError("""Not enough data found in environment variables. Please check your .env file:
                     GAND_PASSWORD
                     GAND_LOGIN""")

HOST = "https://api-gandiva.s-stroy.ru"

async def get_access_token(username, password):
    """Updates GAND_ACCESS_TOKEN and GAND_REFRESH_TOKEN from login+password
    
    Returns access_token"""
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

    response        = requests.request("POST", url, headers=headers, data=body)
    response_json   = response.json()
    access_token    = response_json['access_token']
    refresh_token   = response_json['refresh_token']

    global GAND_ACCESS_TOKEN
    GAND_ACCESS_TOKEN                 = access_token
    os.environ["GAND_ACCESS_TOKEN"]   = access_token
    dotenv.set_key(DOTENV_PATH, "GAND_ACCESS_TOKEN", os.environ["GAND_ACCESS_TOKEN"])

    global GAND_REFRESH_TOKEN
    GAND_REFRESH_TOKEN                 = refresh_token
    os.environ["GAND_REFRESH_TOKEN"]   = refresh_token
    dotenv.set_key(DOTENV_PATH, "GAND_REFRESH_TOKEN", os.environ["GAND_REFRESH_TOKEN"])

    return response_json['access_token']

async def refresh_access_token(refresh_token):
    """Gets and updates access_token + refresh_token
    
    Returns access_token"""
    endpoint = "/Token"
    url = HOST + endpoint

    body = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }
    body = urllib.parse.urlencode(body)

    headers = {
    'Content-Type': 'application/x-www-form-urlencoded',
    }

    response        = requests.request("POST", url, headers=headers, data=body)
    response_json   = response.json()
    access_token    = response_json['access_token']
    refresh_token   = response_json['refresh_token']

    global GAND_ACCESS_TOKEN
    GAND_ACCESS_TOKEN                 = access_token
    os.environ["GAND_ACCESS_TOKEN"]   = access_token
    dotenv.set_key(DOTENV_PATH, "GAND_ACCESS_TOKEN", os.environ["GAND_ACCESS_TOKEN"])

    global GAND_REFRESH_TOKEN
    GAND_REFRESH_TOKEN                 = refresh_token
    os.environ["GAND_REFRESH_TOKEN"]   = refresh_token
    dotenv.set_key(DOTENV_PATH, "GAND_REFRESH_TOKEN", os.environ["GAND_REFRESH_TOKEN"])
    print('all good')

    return response_json['access_token']

async def get_page_of_requests_by_filter(page_number):


    headers = {
        "Content-type": "application/json",
        "Authorization": f"Bearer {GAND_ACCESS_TOKEN}"
    }
    endpoint = "/api/Requests/Filter"
    url = HOST + endpoint
    filter_departments = [2]
    filter_categories = [32]

    filter_data = {
        "Departments": filter_departments,
        "Categories": filter_categories
    }

    body = {
        "Filtering": filter_data,
        "BaseFilter": 0,
        "Page": page_number,
        "Size": 100,
        "Sorting": 0,
        "Descending": False
    }

    body = json.dumps(body)
    
    response = requests.request("POST", url, headers=headers, data=body)

    if response.status_code in [200, 201]:
        return response.json()
    else:
        return False

async def get_request_by_id(request_id):
    headers = {
        "Content-type": "application/x-www-form-urlencoded",
        "Authorization": f"Bearer {GAND_ACCESS_TOKEN}"
    }

    url = f"{HOST}/api/Requests/{request_id}"


    response = requests.get(url, headers=headers)
    if response.status_code in [200, 201]:
        return response.json()
    else:
        return False

async def main():
    await get_access_token(GAND_LOGIN, GAND_PASSWORD)
    # response = await get_page_of_requests_by_filter(1)
    response = await get_request_by_id(1002)
    print(response)


if __name__ == '__main__':
    # get_access_token(GAND_LOGIN, GAND_PASSWORD)
    asyncio.run(main())