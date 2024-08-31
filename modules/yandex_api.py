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

if not (YA_X_CLOUD_ORG_ID and YA_ACCESS_TOKEN and YA_CLIENT_ID and YA_CLIENT_SECRET):
    raise ValueError("""Not enough data found in environment variables. Please check your .env file:
                     YA_X_CLOUD_ORG_ID
                     YA_ACCESS_TOKEN
                     YA_CLIENT_ID
                     YA_CLIENT_SECRET""")

YA_OAUTH_TOKEN_URL  = "https://oauth.yandex.ru/token"
YA_INFO_TOKEN_URL   = "https://login.yandex.ru/info"
HOST = "https://api.tracker.yandex.net"


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

async def get_tasks_by_login(login):
    # TODO: add all filters in the future, should be universal

    endpoint    = "/v2/issues/_search?expand=transitions"
    url         = HOST + endpoint

    body = {
        "filter": {
            "assignee": login
            }
    }
    body = json.dumps(body)

    headers= {
            "Authorization": f"OAuth {YA_ACCESS_TOKEN}",
            "Content-Type": "application/json",
            "X-Cloud-Org-Id": YA_X_CLOUD_ORG_ID,
        }
    
    response = requests.post(url, headers=headers, data=body)
    
    if response.status_code != 200:
        logging.error(f'[{response.status_code}] - {response.reason}')
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

async def check_access_token(access_token) -> str:
    """Recursive way to check if access token works.

    Returns only when login was successful (+ refresh the token)"""

    response = await get_account_info(access_token)


    if response.status_code == 200:
        print("Welcome, " + json.loads(response.text)['login'] + "!")
        return
    else:
        yandex_access_token = await refresh_access_token()
        response = await get_account_info(yandex_access_token)
        await check_access_token(access_token)

async def get_account_info(access_token) -> requests.Response:
    """Gets yandex account info by giving out access_token
    Usually used to check if access_token is still relevant or not
    """

    response = requests.post(
        YA_INFO_TOKEN_URL,
        data={
            "oauth_token": access_token,
            "format": "json",
        },
    )
    
    return response

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

async def add_task(
    task_id_gandiva,
    initiator_name,
    description="Без описания",
    queue="TESTQUEUE",
    assignee_yandex_login="",
    task_type=""
):
    # Convert the task ID to a string
    task_id_gandiva = str(task_id_gandiva)

    # Construct the task summary
    task_summary = f"{task_id_gandiva}. {description[:50]}..."

    # Prepare the request body
    body = {
        "summary": task_summary,
        "description": description,
        "queue": queue,
        "66c9888e114f9256cfd4fea2--TQ_Initiator": initiator_name,
        "unique": task_id_gandiva
    }

    # Optionally add assignee and task type if provided
    if assignee_yandex_login:
        body["assignee"] = assignee_yandex_login

    if task_type:
        body["type"] = task_type

    # Convert the body to a JSON string
    body = json.dumps(body)

    # Set up the headers
    headers = {
        "Content-type": "application/json",
        "X-Cloud-Org-Id": "bpflsbold6n4eeu80057",  # Replace with actual ID if different
        "Authorization": f"OAuth {YA_ACCESS_TOKEN}"
    }

    # Construct the URL
    url = f"{HOST}/v2/issues"

    # Make the HTTP POST request
    response = requests.post(url, data=body, headers=headers)

    sc = response.status_code

    # Check if the response is successful
    if sc in [200, 201]:
        return response.json()  # Return the json
    elif sc == 409:
        logging.info(f"Task {task_id_gandiva} already exists")
        return sc # we can handle this status code later
    else:
        return False

async def get_tasks(queue="TESTQUEUE", page_number=1, per_page=5000):
    # Determine the sorting order and page size
    sorting_order = "+key"

    # Construct page parameter
    if page_number == 1:
        page_parameter = ""
    else:
        page_parameter = f"&page={page_number}"

    # Prepare the request body with filter and order
    body = {
        "filter": {
            "queue": queue
        },
        "order": sorting_order
    }

    # Convert the body to a JSON string
    body = json.dumps(body)

    # Set up the headers
    headers = {
        "Content-type": "application/json",
        "X-Cloud-Org-Id": YA_X_CLOUD_ORG_ID,
        "Authorization": f"OAuth {YA_ACCESS_TOKEN}"
    }

    # Construct the URL
    url = f"{HOST}/v2/issues/_search?expand=transitions&perPage={per_page}{page_parameter}"

    # Make the HTTP POST request
    response = requests.post(url, data=body, headers=headers)

    # Check if the response is successful
    if response.status_code in [200, 201]:
        return response.json()
    else:
        return False

# import time # using for timing functions

async def main():
    
    # await refresh_access_token(YA_REFRESH_TOKEN)
    # start_time = time.time()
    res = await get_tasks_by_login("phtea")
    # res = await get_tasks()
    print(res)
    # print("--- %s seconds ---" % (time.time() - start_time))
    pass

if __name__ == '__main__':
    asyncio.run(main())