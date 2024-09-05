import os
import dotenv
import logging
import json
import requests
import asyncio
import aiohttp
from bs4 import BeautifulSoup
import gandiva_api

GANDIVA_TASK_URL = "https://gandiva.s-stroy.ru/Request/Edit/"

# Custom fields key names:
YANDEX_FIELD_ID_GANDIVA = "66d379ee99b2de14092e0185--Gandiva"
YANDEX_FIELD_ID_INITIATOR = "66d379ee99b2de14092e0185--Initiator"
YANDEX_FIELD_ID_INITIATOR_DEPARTMENT = "66d379ee99b2de14092e0185--InitiatorDepartment"

import db_module as db

# String manipulations
import urllib.parse

# Define the database URL
DB_URL = 'sqlite:///project.db'  # Using SQLite for simplicity

# Create the database and tables
DB_ENGINE = db.create_database(DB_URL)
DB_SESSION = db.get_session(DB_ENGINE)

# Load environment variables
DOTENV_PATH = ".env"
dotenv.load_dotenv()


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
HEADERS = {
        "X-Cloud-Org-Id": YA_X_CLOUD_ORG_ID,
        "Authorization": f"OAuth {YA_ACCESS_TOKEN}"
}


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def extract_text_from_html(html):
    soup = BeautifulSoup(html, "html.parser")
    return soup.get_text()

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

# Authorization block

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

async def get_access_token_captcha() -> str:
    """
    Asynchronously retrieves the Yandex access token using manual captcha input.

    This function prompts the user to enter a captcha code obtained from the Yandex OAuth authorization 
    endpoint. It sends a POST request to the Yandex OAuth token URL with the provided captcha code 
    and other necessary credentials to retrieve an access token.

    Returns:
    - str: The Yandex access token if successfully retrieved.

    Raises:
    - ValueError: If the captcha code is invalid and the access token cannot be obtained.

    Notes:
    - The user must manually obtain the captcha code by visiting the Yandex authorization URL:
      `https://oauth.yandex.ru/authorize?response_type=code&client_id={YA_CLIENT_ID}`.
    - The function will keep prompting for the captcha code until a valid access token is returned.
    - Logging is used to record the status of the HTTP response and the response text for debugging purposes.
    """

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

# Common helper function for HTTP requests
async def make_http_request(method, url, headers=None, body=None):
    """Generalized function to handle HTTP GET/POST requests."""

    valid_methods = ['GET', 'POST', 'PUT', 'PATCH', 'DELETE']
    if method.upper() not in valid_methods:
        logging.error(f"Invalid HTTP method: {method}")
        return None
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.request(method, url, headers=headers, data=body) as response:
                status_code = response.status
                response_text = await response.text()  # For logging purposes
                
                if status_code in [200, 201]:
                    return await response.json()  # Return the JSON response
                else:
                    logging.error(f"Request to {url} failed with status {status_code}: {response_text}")
                    return None
    except Exception as e:
        logging.error(f"Exception during request to {url}: {str(e)}")
        return None

# Functions

async def get_page_of_tasks(queue: str = None,
                            keys: str | list[str] = None,
                            filter: dict = None,
                            query: str = None,
                            order: str = None,
                            expand: str = None,
                            login: str = None,
                            page_number: int = 1,
                            per_page: int = 5000):
    # Initialize the request body
    body = {}

    # Respect the priority of parameters: queue > keys > filter > query
    if queue:
        body["filter"] = {"queue": queue}
        if login:
            body["filter"]["assignee"] = login
        if filter:
            body["filter"].update(filter)
    elif keys:
        body["keys"] = keys
    elif filter:
        body["filter"] = filter
        if login:
            body["filter"]["assignee"] = login
    elif login:
        body["filter"] = {"assignee": login}  # Allow filtering by login alone
    elif query:
        body["query"] = query

    # Include sorting order if filters are used
    if "filter" in body and order:
        body["order"] = order

    # Convert the body to a JSON string
    body_json = json.dumps(body)

    # Construct the URL with the expand and perPage parameters
    url = f"{HOST}/v2/issues/_search?perPage={per_page}"
    if page_number > 1:
        url += f"&page={page_number}"
    if expand:
        url += f"&expand={expand}"

    # Use the helper function to make the POST request
    response_json = await make_http_request('POST', url, headers=HEADERS, body=body_json)

    # Return the response JSON or None if the request failed
    if response_json:
        return response_json
    else:
        logging.error(f"Failed to get tasks with query: {body}")
        return None
    
async def get_all_tasks(queue: str = "TEA",
                                keys: str | list[str] = None,
                                filter: dict = None,
                                query: str = None,
                                order: str = None,
                                expand: str = None,
                                login: str = None,
                                per_page: int = 5000):

    # Initialize an empty list to store all tasks
    all_yandex_tasks = []
    page_number = 1

    # Loop to fetch tasks page by page until no more tasks are returned
    while True:
        # Fetch tasks for the current page with all provided arguments
        yandex_tasks = await get_page_of_tasks(
            queue=queue,
            keys=keys,
            filter=filter,
            query=query,
            order=order,
            expand=expand,
            login=login,
            page_number=page_number,
            per_page=per_page
        )

        # Check if the current page has no tasks (i.e., the list is empty or None)
        if not yandex_tasks or len(yandex_tasks) == 0:
            break

        # Append the tasks to the all_yandex_tasks list
        all_yandex_tasks.extend(yandex_tasks)

        # Move to the next page
        page_number += 1

    return all_yandex_tasks if all_yandex_tasks else None

async def get_tasks_by_gandiva_ids(gandiva_ids: list[str]) -> dict:
    """Get tasks by their Gandiva IDs"""

    # Initialize the request body
    body = {
        "filter": {"unique": gandiva_ids}
    }

    # Convert the body to a JSON string
    body_json = json.dumps(body)

    # Construct the URL
    url = f"{HOST}/v2/issues/_search?perPage=5000"

    # Use the helper function to make the POST request
    response_json = await make_http_request('POST', url, headers=HEADERS, body=body_json)

    # Return the response JSON or None if the request failed
    if response_json:
        return response_json
    else:
        logging.error(f"Failed to get tasks with Gandiva IDs: {gandiva_ids}")
        return None

async def get_task(yandex_task_id: str) -> dict:
    """Returns task (dictionary type) """

    url = f"{HOST}/v2/issues/{yandex_task_id}"
    
    # Use the helper function to make the HTTP request
    response = await make_http_request("GET", url, headers=HEADERS)
    
    # Check if the response is successful
    if response:
        return response  # Return the JSON response
    else:
        logging.error(f"Failed to get task {yandex_task_id}.")
        return None

async def add_task(task_id_gandiva,
                   initiator_name,
                   description="Без описания",
                   queue="TEA",
                   assignee_yandex_id=None,
                   task_type=None,
                   initiator_department=None):

    # Convert the task ID to a string
    task_id_gandiva = str(task_id_gandiva)

    # Check if the task is already in the database
    task_in_db = db.find_task_by_gandiva_id(DB_SESSION, task_id_gandiva=task_id_gandiva)
    if task_in_db:
        return task_in_db

    # Construct the task summary
    task_summary = f"{task_id_gandiva}. {description[:50]}..."

    # Prepare the request body
    body = {
        "summary": task_summary,
        "description": description,
        "queue": queue,
        YANDEX_FIELD_ID_INITIATOR: initiator_name,
        YANDEX_FIELD_ID_GANDIVA: GANDIVA_TASK_URL + task_id_gandiva,
        "unique": task_id_gandiva,
    }

    # Optionally add assignee and task type if provided
    if assignee_yandex_id:
        body["assignee"] = assignee_yandex_id
    if task_type:
        body["type"] = task_type
    if initiator_department:
        body[YANDEX_FIELD_ID_INITIATOR_DEPARTMENT] = initiator_department

    # Convert the body to a JSON string
    body_json = json.dumps(body)

    # Construct the URL
    url = f"{HOST}/v2/issues"

    # Use the helper function to make the POST request
    response_json = await make_http_request('POST', url, headers=HEADERS, body=body_json)

    # Process the response
    if response_json and isinstance(response_json, dict):
        task_id_yandex = response_json.get('key')
        if task_id_yandex:
            # Add task to the database
            new_task = db.Task(task_id_yandex=task_id_yandex, task_id_gandiva=task_id_gandiva)
            DB_SESSION.add(new_task)
            DB_SESSION.commit()
            logging.info(f"Task {task_id_yandex} added to the database.")
            return response_json
        else:
            logging.error(f"Failed to retrieve task key from response for task {task_id_gandiva}")
            return None
    else:
        logging.error(f"Failed to add task {task_id_gandiva}")
        return None

async def add_or_edit_tasks(tasks_gandiva, queue="TEA"):
    """
    Adds tasks from Gandiva to Yandex Tracker if they do not already exist in Yandex Tracker.

    :param tasks_gandiva: List of tasks from Gandiva
    :param queue: The queue to which tasks should be added
    """
    # Create a new aiohttp session

    async with aiohttp.ClientSession() as session:
        for task in tasks_gandiva:
            gandiva_task_id = str(task['Id'])

            # Extract and format task details
            initiator_name = f"{task['Initiator']['FirstName']} {task['Initiator']['LastName']}"
            description_html = task['Description']
            description = extract_text_from_html(description_html)  # Assuming a helper function to extract text from HTML
            assignee_id_yandex = ""
            task_type = "improvement"
            # TODO: get initiator's department from db??
            initiator_id = task['Initiator']['Id']
            initiator_department = await gandiva_api.get_department_by_user_id(user_id=initiator_id, session=session)

            # Add the task to Yandex Tracker
            response = await add_task(gandiva_task_id, initiator_name,
                                      initiator_department=initiator_department,
                                      description=description,
                                      queue=queue,
                                      assignee_yandex_id=assignee_id_yandex,
                                      task_type=task_type)
            # if a task is in db already, edit it:
            if isinstance(response, db.Task):
                # by default all edit fields are none, and only if yandex tracker's field is empty, change it to not none
                task_id_yandex = response.task_id_yandex

                edit_initiator_name = None
                edit_initiator_department = None
                edit = None

                task_yandex = await get_task(task_id_yandex)
                if initiator_name and not task_yandex.get(YANDEX_FIELD_ID_INITIATOR):
                    edit_initiator_name = initiator_name
                if not task_yandex.get(YANDEX_FIELD_ID_GANDIVA):
                    edit = True
                if initiator_department and not task_yandex.get(YANDEX_FIELD_ID_INITIATOR_DEPARTMENT):
                    edit_initiator_department = initiator_department
                if edit_initiator_name or edit or edit_initiator_department:
                    await edit_task(task_id_yandex=task_id_yandex,
                                    initiator_name=edit_initiator_name,
                                    initiator_department=edit_initiator_department)
                else:
                    logging.info(f"Task {gandiva_task_id} is already up-to-date.")
                # TODO: add analyst and assignee later
                # if not task_yandex['66d379ee99b2de14092e0185--Analyst']:
                #     edit_analyst = (???)
                # if not task_yandex['assignee'] and assignee_id_yandex:
                #     edit_assignee_id_yandex = assignee_id_yandex
                

async def edit_task(task_id_yandex,
                    summary=None,
                    description=None,
                    assignee_id_yandex=None,
                    task_type=None,
                    priority=None,
                    parent=None,
                    sprint=None,
                    followers=None,
                    initiator_name=None,
                    initiator_department=None):

    # Convert the task ID to a string
    task_id_yandex = str(task_id_yandex)

    # Prepare the request body
    body = {}
    # Fetch task from the database
    task_in_db = db.find_task_by_yandex_id(session=DB_SESSION, task_id_yandex=task_id_yandex)
    if task_in_db:
        task_id_gandiva = task_in_db.task_id_gandiva
        body[YANDEX_FIELD_ID_GANDIVA] = GANDIVA_TASK_URL + task_id_gandiva
    
    # Add fields to the request body if provided
    if summary: body["summary"] = summary
    if description: body["description"] = description
    if assignee_id_yandex: body["assignee"] = assignee_id_yandex
    if task_type: body["type"] = task_type
    if priority: body["priority"] = priority
    if parent: body["parent"] = parent
    if sprint: body["sprint"] = sprint
    if followers: body["followers"] = {"add": followers}
    if initiator_name: body[YANDEX_FIELD_ID_INITIATOR] = initiator_name
    if initiator_department: body[YANDEX_FIELD_ID_INITIATOR_DEPARTMENT] = initiator_department

    # Convert the body to a JSON string
    body_json = json.dumps(body)

    # Construct the URL for updating the task
    url = f"{HOST}/v2/issues/{task_id_yandex}"

    # Use the helper function to make the PATCH request
    return await make_http_request('PATCH', url, headers=HEADERS, body=body_json)

async def move_task_status(
    task_id_yandex,
    transition_id,
    comment="",
    resolution="fixed",  # Default resolution when closing the task
):

    # Convert the task ID to a string
    task_id_yandex = str(task_id_yandex)

    # Set a default comment if not provided
    if not comment:
        comment = f"Task status has been successfully moved to {transition_id} for {task_id_yandex}."

    # Prepare the request body
    body = {
        "comment": comment
    }

    # Add resolution if the transition is "close"
    if transition_id == "close":
        body["resolution"] = resolution

    # Convert the body to a JSON string
    body_json = json.dumps(body)

    # Construct the URL for moving the task
    url = f"{HOST}/v2/issues/{task_id_yandex}/transitions/{transition_id}/_execute"
    
    # Use the helper function to make the POST request
    return await make_http_request('POST', url, headers=HEADERS, body=body_json)

async def batch_move_tasks_status(gandiva_tasks: list):
    # Mapping of Gandiva Status to Yandex Tracker transition_id
    status_to_transition = {
        4: "fourinformationrequiredMeta",
        5: "onecancelledMeta",
        6: "threewritingtechnicalspecificMeta",
        8: "acceptanceintheworkbaseMeta",
        10: "twowaitingfortheanalystMeta",
        11: "twowaitingfortheanalystMeta",
        12: "twowaitingfortheanalystMeta",
        13: "threewritingtechnicalspecificMeta"
    }

    for task in gandiva_tasks:
        task_id_gandiva = str(task['Id'])
        gandiva_status = task['Status']
        transition_id = status_to_transition.get(gandiva_status)
        yandex_status = transition_id[:-4]
        current_yandex_status = None
        task_id_yandex = None

        task_in_db = db.find_task_by_gandiva_id(DB_SESSION, task_id_gandiva=task_id_gandiva)
        if task_in_db:
            task_id_yandex = task_in_db.task_id_yandex
            if task_id_yandex is None:
                logging.warning(f"The task {task_id_gandiva} is in db, but task_id_yandex is empty. (???) I guess it's on gandiva but still not on yandex, that's why")
                continue
            task_yandex = await get_tasks_by_gandiva_ids(task_id_gandiva)
            if task_yandex:
                current_yandex_status = task_yandex[0]['status']['key']
            if current_yandex_status == yandex_status:
                logging.info(f"Task {task_id_yandex} is already in status {yandex_status}")
                continue

        # If a transition ID is found for the status, move the task status
        if not transition_id:
            logging.warning(f"No transition ID found for task {task_id_gandiva} with status code {gandiva_status}")
            continue
        if task_id_yandex is None:
            logging.warning(f"No task ID found for gandiva task {task_id_gandiva}")
            continue
        try:
            await move_task_status(
                task_id_yandex=task_id_yandex,
                transition_id=transition_id)
        except Exception as e:
            logging.error(f"Exception occurred while processing task {task_id_gandiva}: {str(e)}")

async def batch_edit_tasks(values: dict, issues: list):
    """Bulk update tasks in Yandex Tracker."""

    # Prepare the request body
    body = {
        "values": values,
        "issues": issues
    }

    # Convert the body to a JSON string
    body_json = json.dumps(body)

    # Construct the URL for the bulk update
    url = f"{HOST}/v2/bulkchange/_update"

    # Use the helper function to make the HTTP request
    response = await make_http_request("POST", url, headers=HEADERS, body=body_json)

    if response:
        logging.info("Bulk change successful")
        return response  # Return the JSON response
    else:
        logging.error("Failed to perform bulk change.")
        return None

# TODO
async def add_comments_to_task(task_id_comment: dict):
    pass
# TODO
async def batch_add_comments_to_tasks(task_id_comments: dict):
    pass

def filter_tasks_with_unique(tasks):
    """Filters and returns only the tasks that contain the 'unique' key."""
    return [task for task in tasks if 'unique' in task]

async def add_existing_tracker_tasks_to_db():
    res = await get_all_tasks()
    res = filter_tasks_with_unique(res)
    db.add_tasks_to_db(session=DB_SESSION, tasks=res)

async def get_sprints_on_board(board_id: str) -> dict:
    """Returns sprints (as a dictionary) for the given board."""
    url = f"{HOST}/v2/boards/{board_id}/sprints"
    return await make_http_request('GET', url, headers=HEADERS)



import time # using for timing functions
async def main():
    # await refresh_access_token(YA_REFRESH_TOKEN)
    start_time = time.time()

    # await add_existing_tracker_tasks_to_db()
    res = await get_sprints_on_board(52)
    print(res)
    print("--- %s seconds ---" % (time.time() - start_time))
    pass


if __name__ == '__main__':
    asyncio.run(main())