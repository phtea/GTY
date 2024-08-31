import os
import dotenv
import logging
import json
import requests
import asyncio
import aiohttp
from bs4 import BeautifulSoup

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

# Handle not enough data in .env
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

    endpoint = "/v2/issues/_search?expand=transitions"
    url = HOST + endpoint

    body = {
        "filter": {
            "assignee": login
        }
    }
    body_json = json.dumps(body)

    headers = {
        "Authorization": f"OAuth {YA_ACCESS_TOKEN}",
        "Content-Type": "application/json",
        "X-Cloud-Org-Id": YA_X_CLOUD_ORG_ID,
    }

    # Create a new ClientSession for the request
    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, data=body_json) as response:
            if response.status != 200:
                logging.error(f'[{response.status}] - {response.reason}')
                return None

            return await response.json()  # Return the JSON response

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
    
    # URL encode the payload
    payload_url_encoded = urllib.parse.urlencode(payload)
    
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
    }

    # Create a new ClientSession for the request
    async with aiohttp.ClientSession() as session:
        async with session.post(YA_OAUTH_TOKEN_URL, headers=headers, data=payload_url_encoded) as response:
            if response.status != 200:
                logging.error(f"Статус ошибки ответа запроса: {response.status}")
                return None

            logging.info(f"yandex refresh code response: {await response.text()}")
            device_creds = await response.json()

            access_token = device_creds['access_token']
            refresh_token = device_creds['refresh_token']

            global YA_ACCESS_TOKEN
            YA_ACCESS_TOKEN = access_token
            os.environ["YA_ACCESS_TOKEN"] = access_token
            dotenv.set_key(DOTENV_PATH, "YA_ACCESS_TOKEN", os.environ["YA_ACCESS_TOKEN"])

            YA_REFRESH_TOKEN = refresh_token
            os.environ["YA_REFRESH_TOKEN"] = refresh_token
            dotenv.set_key(DOTENV_PATH, "YA_REFRESH_TOKEN", os.environ["YA_REFRESH_TOKEN"])

            return access_token

async def check_access_token(access_token) -> str:
    """Recursive way to check if access token works.
    
    Returns only when login was successful (+ refresh the token).
    """
    # Get account info using the provided access token
    response = get_account_info(access_token)

    # Check if the response is successful
    if response.status_code == 200:
        user_info = response.json()  # Parse the JSON response
        print("Welcome, " + user_info['login'] + "!")
        return

    # If the response is not successful, refresh the access token
    yandex_access_token = await refresh_access_token()

    # Recheck the new access token
    return await check_access_token(yandex_access_token)

def get_account_info(access_token) -> requests.Response:
    """Gets Yandex account info by providing the access_token.

    Usually used to check if the access_token is still relevant or not.
    """
    # Prepare the request parameters
    data = {
        "oauth_token": access_token,
        "format": "json",
    }
    
    # Make the HTTP POST request
    response = requests.post(YA_INFO_TOKEN_URL, data=data)

    return response  # Return the response object

async def get_access_token_captcha() -> str:
    """Gets Yandex access token using manual captcha input."""

    # Handling code retrieved manually from Yandex
    yandex_id_metadata = {}

    async with aiohttp.ClientSession() as session:
        while not yandex_id_metadata.get("access_token"):
            yandex_captcha_code = input(f"Code (get from https://oauth.yandex.ru/authorize?response_type=code&client_id={YA_CLIENT_ID}): ")
            async with session.post(YA_OAUTH_TOKEN_URL, data={
                "grant_type": "authorization_code",
                "code": yandex_captcha_code,
                "client_id": YA_CLIENT_ID,
                "client_secret": YA_CLIENT_SECRET,
            }) as yandex_captcha_response:
                
                logging.info(f"Yandex captcha code: {yandex_captcha_response.status}")
                response_text = await yandex_captcha_response.text()
                logging.info(f"Yandex captcha response: {response_text}")

                yandex_id_metadata = json.loads(response_text)

    yandex_access_token = yandex_id_metadata.get("access_token")
    return yandex_access_token

async def add_task(
    task_id_gandiva,
    initiator_name,
    description="Без описания",
    queue="TESTQUEUE",
    assignee_yandex_login="",
    task_type="",
    session=None  # Accept session as a parameter
):
    if session is None:
        raise ValueError("Session must be provided.")

    # Convert the task ID to a string
    task_id_gandiva = str(task_id_gandiva)

    # Construct the task summary
    task_summary = f"{task_id_gandiva}. {description[:50]}..."

    # Prepare the request body
    body = {
        "summary": task_summary,
        "description": description,
        "queue": queue,
        "66d379ee99b2de14092e0185--Initiator": initiator_name,
        "unique": task_id_gandiva
    }

    # Optionally add assignee and task type if provided
    if assignee_yandex_login:
        body["assignee"] = assignee_yandex_login

    if task_type:
        body["type"] = task_type

    # Convert the body to a JSON string
    body_json = json.dumps(body)

    # Set up the headers
    headers = {
        "Content-type": "application/json",
        "X-Cloud-Org-Id": YA_X_CLOUD_ORG_ID,
        "Authorization": f"OAuth {YA_ACCESS_TOKEN}"
    }

    # Construct the URL
    url = f"{HOST}/v2/issues"

    try:
        # Make the HTTP POST request using the session
        async with session.post(url, data=body_json, headers=headers) as response:
            sc = response.status
            response_text = await response.text()  # Get the response text for logging

            # Check if the response is successful
            if sc in [200, 201]:
                logging.info(f"Task {task_id_gandiva} successfully added!")
                return await response.json()  # Return the json
            elif sc == 409:
                logging.info(f"Task {task_id_gandiva} already exists")
                return sc  # we can handle this status code later
            else:
                logging.error(f"Failed to add task {task_id_gandiva}: {sc} - {response_text}")
                return False  # Return False for any other error
    except Exception as e:
        logging.error(f"Exception occurred while adding task {task_id_gandiva}: {str(e)}")
        return False  # Return False if an exception occurs

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
    body_json = json.dumps(body)

    # Set up the headers
    headers = {
        "Content-type": "application/json",
        "X-Cloud-Org-Id": YA_X_CLOUD_ORG_ID,
        "Authorization": f"OAuth {YA_ACCESS_TOKEN}"
    }

    # Construct the URL
    url = f"{HOST}/v2/issues/_search?expand=transitions&perPage={per_page}{page_parameter}"

    # Use aiohttp to make the HTTP POST request
    async with aiohttp.ClientSession() as session:
        async with session.post(url, data=body_json, headers=headers) as response:
            # Check if the response is successful
            if response.status in [200, 201]:
                return await response.json()  # Return the JSON response
            else:
                logging.error(f"Failed to get tasks: {response.status} - {await response.text()}")
                return False
    
async def get_all_yandex_tasks(queue="TESTQUEUE"):
    # Initialize an empty list to store all tasks
    all_yandex_tasks = []
    page_number = 1

    # Loop to fetch tasks page by page until no more tasks are returned
    while True:
        # Fetch tasks for the current page
        yandex_tasks = await get_tasks(queue=queue, page_number=page_number)

        # Check if the current page has no tasks (i.e., the list is empty or None)
        if not yandex_tasks or len(yandex_tasks) == 0:
            break

        # Append the tasks to the all_yandex_tasks list
        all_yandex_tasks.extend(yandex_tasks)

        # Move to the next page
        page_number += 1

    # Return the full list of tasks
    return all_yandex_tasks

async def add_tasks(tasks_gandiva, queue="TESTQUEUE"):
    """
    Adds tasks from Gandiva to Yandex Tracker if they do not already exist in Yandex Tracker.

    :param tasks_gandiva: List of tasks from Gandiva
    :param queue: The queue to which tasks should be added
    """
    # Create a new aiohttp session
    async with aiohttp.ClientSession() as session:
        for task in tasks_gandiva:
            task_id = str(task['Id'])

            # Extract and format task details
            initiator_name = f"{task['Initiator']['FirstName']} {task['Initiator']['LastName']}"
            description_html = task['Description']
            description = extract_text_from_html(description_html)  # Assuming a helper function to extract text from HTML
            assignee_login_yandex = ""
            task_type = ""

            # Add the task to Yandex Tracker
            await add_task(task_id, initiator_name, description, queue, assignee_login_yandex, task_type, session)

def extract_text_from_html(html):
    soup = BeautifulSoup(html, "html.parser")
    return soup.get_text()

# import time # using for timing functions
async def main():
    
    # await refresh_access_token(YA_REFRESH_TOKEN)
    # start_time = time.time()
    # res = await get_tasks_by_login("phtea")
    res = await get_all_yandex_tasks()
    print(res)
    # print("--- %s seconds ---" % (time.time() - start_time))
    pass

if __name__ == '__main__':
    asyncio.run(main())