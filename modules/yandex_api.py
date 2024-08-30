import os
import dotenv
import logging
import json
import requests
import asyncio

# String manipulations
import urllib.parse

DOTENV_PATH = os.path.join(os.path.dirname(__file__), os.pardir)
DOTENV_PATH = os.path.join(DOTENV_PATH, '.env')
if os.path.exists(DOTENV_PATH):
    dotenv.load_dotenv(DOTENV_PATH)


YA_X_CLOUD_ORG_ID   = os.environ.get("YA_X_CLOUD_ORG_ID")
YA_ACCESS_TOKEN     = os.environ.get("YA_ACCESS_TOKEN")
YA_CLIENT_ID        = os.environ.get("YA_CLIENT_ID")
YA_CLIENT_SECRET    = os.environ.get("YA_CLIENT_SECRET")
YA_REFRESH_TOKEN    = os.environ.get("YA_REFRESH_TOKEN")
YA_OAUTH_TOKEN_URL  = "https://oauth.yandex.ru/token"
YA_INFO_TOKEN_URL   = "https://login.yandex.ru/info"

async def get_tasks_by_login(login):
    # TODO: add all filters in the future, should be universal
    body = json.dumps(
        {
            "filter": {
                "assignee": login,
            },
        }
    )
    response = requests.post(
        "https://api.tracker.yandex.net/v2/issues/_search?expand=transitions",
        headers={
            "Host": "api.tracker.yandex.net",
            "Authorization": f"OAuth {YA_ACCESS_TOKEN}",
            "Content-Type": "application/json",
            "X-Cloud-Org-Id": YA_X_CLOUD_ORG_ID,
        },
        data=body,
    )
    
    if response.status_code != 200:
        return None

    return response.json()

async def refresh_access_token(refresh_token=None) -> dict:
    """ Refresh access_token using refresh_token,
        update global variables and
        save changes to .env file
    """
    global YA_REFRESH_TOKEN
    if refresh_token is None:
        refresh_token = YA_REFRESH_TOKEN

    payload = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": YA_CLIENT_ID,
        "client_secret": YA_CLIENT_SECRET
    }
    # payload needs to be url encoded
    payload_url_encoded = urllib.parse.urlencode(payload)
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
    }

    response = requests.request("POST", YA_OAUTH_TOKEN_URL, headers=headers, data=payload_url_encoded)

    if response.status_code != 200:
        logging.error(f"Статус ошибки ответа запроса: {response.status_code}")
        return None


    logging.info(f"yandex refresh code response: {response.text}")
    device_creds    = json.loads(response.text)

    access_token    = device_creds['access_token']
    refresh_token   = device_creds['refresh_token']

    global YA_ACCESS_TOKEN
    YA_ACCESS_TOKEN                 = access_token
    os.environ["YA_ACCESS_TOKEN"]   = access_token
    dotenv.set_key(DOTENV_PATH, "YA_ACCESS_TOKEN", os.environ["YA_ACCESS_TOKEN"])


    YA_REFRESH_TOKEN                = refresh_token
    os.environ["YA_REFRESH_TOKEN"]  = refresh_token
    dotenv.set_key(DOTENV_PATH, "YA_REFRESH_TOKEN", os.environ["YA_REFRESH_TOKEN"])

    return access_token

async def check_yandex_access_token(access_token) -> str:
    """Recursive way to check if access token works.

    Returns only when login was successful (+ refresh the token)"""

    yandex_info_response = await get_account_info(access_token)


    if yandex_info_response.status_code == 200:
        print("Welcome, " + json.loads(yandex_info_response.text)['login'] + "!")
        return
    else:
        yandex_access_token = await refresh_access_token()
        yandex_info_response = await get_account_info(yandex_access_token)
        await check_yandex_access_token(access_token)

async def get_account_info(access_token) -> requests.Response:
    """Gets yandex account info by giving out access_token
    Usually used to check if access_token is still relevant or not
    """

    info_response = requests.post(
        YA_INFO_TOKEN_URL,
        data={
            "oauth_token": access_token,
            "format": "json",
        },
    )
    
    return info_response

async def get_access_token_captcha() -> str:

    # Handling code retrieved manually from yandex.
    # Sounds like bs but yet required.
    yandex_id_metadata = {}
    while not yandex_id_metadata.get("access_token"):
        yandex_captcha_code = input(f"Code (get from https://oauth.yandex.ru/authorize?response_type=code&client_id={YA_CLIENT_ID}): ")
        yandex_captcha_response = requests.post(
            YA_OAUTH_TOKEN_URL,
            data={
                "grant_type": "authorization_code",
                "code": yandex_captcha_code,
                "client_id": YA_CLIENT_ID,
                "client_secret": YA_CLIENT_SECRET,
            },
        )

        logging.info(f"yandex captcha code: {yandex_captcha_response.status_code}")
        logging.info(f"yandex captcha response: {yandex_captcha_response.text}")

        yandex_id_metadata = json.loads(yandex_captcha_response.text)

    yandex_access_token = yandex_id_metadata["access_token"]
    return yandex_access_token

async def main():
    
    # await refresh_access_token(YA_REFRESH_TOKEN)
    pass

if __name__ == '__main__':
    asyncio.run(main())