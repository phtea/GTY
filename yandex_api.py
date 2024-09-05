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




logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def extract_text_from_html(html):
    soup = BeautifulSoup(html, "html.parser")
    return soup.get_text()

HEADERS = {
        "X-Cloud-Org-Id": YA_X_CLOUD_ORG_ID,
        "Authorization": f"OAuth {YA_ACCESS_TOKEN}"
}

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

async def get_page_of_tasks(queue: str = None,
                    keys: str | list[str] = None,
                    filter: dict = None,
                    query: str = None,
                    order: str = None,
                    expand: str = None,
                    login: str = None,
                    page_number: int = 1,
                    per_page: int = 5000):
    """
    Asynchronously retrieves tasks from the Yandex Tracker API based on specified filters and parameters.

    The function supports various filtering options, allowing the user to retrieve tasks by queue,
    specific task keys, filter criteria, or custom query language. Additionally, it allows for
    sorting and pagination of the results.

    Parameters:
    - queue (str, optional): The name of the queue to filter tasks by. This takes precedence over other filters.
    - keys (str | list[str], optional): A single task key or a list of task keys to retrieve specific tasks.
    - filter (dict, optional): A dictionary of additional filter criteria. This can include any field in the task.
    - query (str, optional): A custom query string using Yandex Tracker's query language for more complex filtering.
    - order (str, optional): The sorting order for the results, specified in the format '[+/-]<field_key>'.
                              Use '+' for ascending and '-' for descending order.
    - expand (str, optional): A comma-separated string of fields to include in the response (e.g., 'transitions,attachments').
    - login (str, optional): The assignee's login name to filter tasks by assignee.
    - page_number (int, optional): The page number for paginated results. Defaults to 1.
    - per_page (int, optional): The number of tasks per page. Defaults to 5000, but can be adjusted as needed.

    Returns:
    - dict: A dictionary containing the retrieved tasks in JSON format if the request is successful.
    - bool: Returns False if the request fails, logging the error message.

    Note:
    The priority of parameters is as follows:
    1. `queue`
    2. `keys`
    3. `filter`
    4. `query`

    Only one of these parameters can be used at a time. If both `queue` and `login` are provided,
    tasks will be filtered by both criteria. If `filter` is provided, it will be merged with the
    `assignee` if specified.

    Example Usage:
    >>> tasks = await get_tasks(queue="TREK", login="user_login", order="+status")
    >>> tasks = await get_tasks(keys=["TASK-1", "TASK-2"], expand="attachments")
    >>> tasks = await get_tasks(login="user_login")  # This will now work
    """
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

    # Use aiohttp to make the HTTP POST request
    async with aiohttp.ClientSession() as session:
        async with session.post(url, data=body_json, headers=HEADERS) as response:
            # Check if the response is successful
            if response.status in [200, 201]:
                return await response.json()  # Return the JSON response
            else:
                logging.error(f"Failed to get tasks: {response.status} - {await response.text()}")
                return False
    
async def get_all_tasks(queue: str = "TEA",
                                keys: str | list[str] = None,
                                filter: dict = None,
                                query: str = None,
                                order: str = None,
                                expand: str = None,
                                login: str = None,
                                per_page: int = 5000):
    """
    Asynchronously retrieves all tasks from the Yandex Tracker API based on specified filters and parameters.
    
    This function will paginate through the results, collecting all tasks until there are no more tasks to retrieve.

    Parameters:
    - queue (str, optional): The name of the queue to filter tasks by. Defaults to "TEA".
    - keys (str | list[str], optional): A single task key or a list of task keys to retrieve specific tasks.
    - filter (dict, optional): A dictionary of additional filter criteria. This can include any field in the task.
    - query (str, optional): A custom query string using Yandex Tracker's query language for more complex filtering.
    - order (str, optional): The sorting order for the results, specified in the format '[+/-]<field_key>'.
    - expand (str, optional): A comma-separated string of fields to include in the response (e.g., 'transitions,attachments').
    - login (str, optional): The assignee's login name to filter tasks by assignee.
    - per_page (int, optional): The number of tasks per page. Defaults to 5000.

    Returns:
    - list or bool: A list containing all retrieved tasks in JSON format, or False if no tasks are found.
    """
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

    # Return False if no tasks were found; otherwise, return the full list of tasks
    return all_yandex_tasks if all_yandex_tasks else False

async def get_tasks_by_gandiva_ids(gandiva_ids: str|list) -> list:
    # Initialize the request body
    body = {}
    per_page = 5000

    # Get tasks by gandiva ids
    body["filter"] = {"unique": gandiva_ids}

    # Convert the body to a JSON string
    body_json = json.dumps(body)

    url = f"{HOST}/v2/issues/_search?perPage={per_page}"

    # Use aiohttp to make the HTTP POST request
    async with aiohttp.ClientSession() as session:
        async with session.post(url, data=body_json, headers=HEADERS) as response:
            # Check if the response is successful
            if response.status in [200, 201]:
                return await response.json()  # Return the JSON response
            else:
                logging.error(f"Failed to get tasks: {response.status} - {await response.text()}")
                return False

async def get_task(yandex_task_id: str) -> dict:
    """Returns task (dictionary type) """

    url = f"{HOST}/v2/issues/{yandex_task_id}"

    # Use aiohttp to make the HTTP POST request
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=HEADERS) as response:
            # Check if the response is successful
            if response.status in [200, 201]:
                return await response.json()  # Return the JSON response
            else:
                logging.error(f"Failed to get task: {response.status} - {await response.text()}")
                return False

async def add_task(task_id_gandiva,
                   initiator_name,
                   description="Без описания",
                   queue="TEA",
                   assignee_yandex_id=None,
                   task_type=None,
                   initiator_department=None,
                   session=None):
    """
    Asynchronously adds a new task to the Yandex Tracker.

    This function constructs a task with the provided details and sends a POST request to 
    the Yandex Tracker API to create the task. It supports various parameters for task creation, 
    including the ability to specify an assignee and task type.

    Parameters:
    - task_id_gandiva (str or int): The unique identifier for the task in Gandiva. 
                                     This will be converted to a string.
    - initiator_name (str): The name of the user or entity initiating the task.
    - description (str, optional): A brief description of the task. Defaults to "Без описания" (No description).
    - queue (str, optional): The queue to which the task should be added. Defaults to "TEA".
    - assignee_yandex_login (str, optional): The Yandex login of the user to whom the task will be assigned. 
                                               Defaults to an empty string (no assignee).
    - task_type (str, optional): The type of the task being created. Defaults to an empty string.
    - session (aiohttp.ClientSession, optional): An optional aiohttp session object to use for the request. 
                                                 If None, an exception will be raised.

    Returns:
    - dict or bool: Returns the JSON response from the API if the task is successfully added, 
                    returns the status code (409) if the task already exists, 
                    and returns False for any other errors or exceptions.

    Raises:
    - ValueError: If session is None.

    Notes:
    - The task summary is generated from the task ID and the first 50 characters of the description.
    - If a task with the same ID already exists, a status code of 409 will be returned.
    - Proper error handling is implemented to catch exceptions during the request.
    """
    if session is None:
        raise ValueError("Session must be provided.")

    # Convert the task ID to a string
    task_id_gandiva = str(task_id_gandiva)

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

    try:
        # Make the HTTP POST request using the session
        async with session.post(url, data=body_json, headers=HEADERS) as response:
            sc = response.status
            response_json = await response.json()  # Get the response text for logging

            # Check if the response is successful
            if sc in [200, 201]:
                task_id_yandex = response_json['key']
                new_task = db.Task(task_id_yandex=task_id_yandex, task_id_gandiva=task_id_gandiva)
                DB_SESSION.add(new_task)
                logging.info(f"Task {task_id_yandex} added to the database.")
                DB_SESSION.commit()
                logging.info(f"Task {task_id_gandiva} successfully added!")
                return response_json  # Return the json
            elif sc == 409:
                logging.info(f"Task {task_id_gandiva} already exists (api)")
                return sc  # we can handle this status code later
            else:
                logging.error(f"Failed to add task {task_id_gandiva}: {sc} - {response_json}")
                return False  # Return False for any other error
    except Exception as e:
        logging.error(f"Exception occurred while adding task {task_id_gandiva}: {str(e)}")
        return False  # Return False if an exception occurs

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
                                      task_type=task_type,
                                      session=session)
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
                                    session=session,
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
                    initiator_department=None,
                    session=None):
    """
    Asynchronously updates an existing task in the Yandex Tracker.

    This function constructs a PATCH request to update fields of an existing task in the Yandex Tracker API.
    It allows updating the task's summary, description, assignee, type, priority, and other fields.

    Parameters:
    - task_id_gandiva (str or int): The unique identifier for the task in Gandiva. 
                                     This will be converted to a string.
    - summary (str, optional): The new title of the task.
    - description (str, optional): The new description of the task.
    - assignee_yandex_login (str, optional): The Yandex login of the user to whom the task will be assigned.
    - task_type (dict, optional): The new type of the task. Provide a dictionary with `id` or `key`.
    - priority (dict, optional): The new priority of the task. Provide a dictionary with `id` or `key`.
    - parent (dict, optional): The parent task. Provide a dictionary with `id` or `key`.
    - sprint (list[dict], optional): A list of sprints to add the task to. Provide dictionaries with `id`.
    - followers (list[str], optional): A list of Yandex logins to add as followers.
    - session (aiohttp.ClientSession, optional): An optional aiohttp session object to use for the request. 
                                                 If None, an exception will be raised.

    Returns:
    - dict or bool: Returns the JSON response from the API if the task is successfully updated, 
                    and returns False for any errors or exceptions.

    Raises:
    - ValueError: If session is None.

    Notes:
    - Only the fields provided as parameters will be updated. If a field is not provided, it remains unchanged.
    - Proper error handling is implemented to catch exceptions during the request.
    
    Example Usage:
    >>> await update_task(task_id_gandiva="TEST-1", summary="New Title", description="Updated description")
    """
    if session is None:
        raise ValueError("Session must be provided.")

    # Convert the task ID to a string
    task_id_yandex = str(task_id_yandex)

    # Prepare the request body
    body = {}
    task_in_db = db.find_task_by_yandex_id(session=DB_SESSION, task_id_yandex=task_id_yandex)
    if task_in_db:
        task_id_gandiva = task_in_db.task_id_gandiva
        body[YANDEX_FIELD_ID_GANDIVA] = GANDIVA_TASK_URL + task_id_gandiva
    if summary:
        body["summary"] = summary
    if description:
        body["description"] = description
    if assignee_id_yandex:
        body["assignee"] = assignee_id_yandex
    if task_type:
        body["type"] = task_type
    if priority:
        body["priority"] = priority
    if parent:
        body["parent"] = parent
    if sprint:
        body["sprint"] = sprint
    if followers:
        body["followers"] = {"add": followers}
    if initiator_name:
        body[YANDEX_FIELD_ID_INITIATOR] = initiator_name
    if initiator_department:
        body[YANDEX_FIELD_ID_INITIATOR_DEPARTMENT] = initiator_department

    # Convert the body to a JSON string
    body_json = json.dumps(body)

    # Construct the URL for updating the task
    url = f"{HOST}/v2/issues/{task_id_yandex}"

    try:
        # Make the HTTP PATCH request using the session
        async with session.patch(url, data=body_json, headers=HEADERS) as response:
            sc = response.status
            response_text = await response.text()  # Get the response text for logging

            # Check if the response is successful
            if sc in [200, 201]:
                logging.info(f"Task {task_id_yandex} successfully updated!")
                return await response.json()  # Return the JSON response
            else:
                logging.error(f"Failed to update task {task_id_yandex}: {sc} - {response_text}")
                return False  # Return False for any other error
    except Exception as e:
        logging.error(f"Exception occurred while updating task {task_id_yandex}: {str(e)}")
        return False  # Return False if an exception occurs

async def move_task_status(
    task_id_yandex,
    transition_id,
    comment="",
    resolution="fixed",  # Default resolution when closing the task
    session=None  # Accept session as a parameter
):
    """
    Asynchronously moves a task to another status in the Yandex Tracker.

    This function constructs a POST request to execute a transition for an existing task in the Yandex Tracker API.
    If the transition ID is "close", it adds a "resolution" to the request body.

    Parameters:
    - task_id_gandiva (str or int): The unique identifier for the task in Gandiva. 
                                     This will be converted to a string.
    - transition_id (str or int): The identifier of the transition to be executed.
    - comment (str, optional): A comment to add during the transition. Defaults to an empty string.
    - resolution (str, optional): The resolution for closing the task. Defaults to "fixed".
    - session (aiohttp.ClientSession, optional): An optional aiohttp session object to use for the request. 
                                                 If None, an exception will be raised.

    Returns:
    - dict or bool: Returns the JSON response from the API if the task status is successfully changed, 
                    and returns False for any errors or exceptions.

    Raises:
    - ValueError: If session is None.

    Notes:
    - Proper error handling is implemented to catch exceptions during the request.
    
    Example Usage:
    >>> await move_task_status(task_id_gandiva="TEST-1", transition_id="close", comment="Closing the task")
    """
    if session is None:
        raise ValueError("Session must be provided.")

    # Convert the task ID to a string
    task_id_yandex = str(task_id_yandex)

    if not comment:
        comment=f"Task status has been successfully moved to {transition_id} for {task_id_yandex}."

    # Prepare the request body
    body = {
        "comment": comment
    }

    # Add resolution if the transition is "close"
    if transition_id == "close":
        # Add the provided resolution or default to "fixed"
        body["resolution"] = resolution

    # Convert the body to a JSON string
    body_json = json.dumps(body)

    # Construct the URL for moving the task
    url = f"{HOST}/v2/issues/{task_id_yandex}/transitions/{transition_id}/_execute"

    try:
        # Make the HTTP POST request using the session
        async with session.post(url, data=body_json, headers=HEADERS) as response:
            sc = response.status
            response_text = await response.text()  # Get the response text for logging

            # Check if the response is successful
            if sc in [200, 201]:
                logging.info(f"Task {task_id_yandex} successfully moved to new status!")
                return await response.json()  # Return the JSON response
            else:
                logging.error(f"Failed to move task {task_id_yandex} to new status: {sc} - {response_text}")
                return False  # Return False for any other error
    except Exception as e:
        logging.error(f"Exception occurred while moving task {task_id_yandex} to new status: {str(e)}")
        return False  # Return False if an exception occurs

async def batch_move_tasks_status(gandiva_tasks: list):
    """
    Asynchronously moves multiple tasks to their corresponding statuses in the Yandex Tracker based on the 
    Gandiva request's "Status" field by calling the move_task_status function.

    Parameters:
    - gandiva_requests (list of dict): A list of Gandiva requests, each containing at least 'Id' and 'Status'.
    - session (aiohttp.ClientSession, optional): An optional aiohttp session object to use for the request. 
                                                 If None, an exception will be raised.

    Returns:
    - dict: A dictionary with task IDs as keys and the API responses (or errors) as values.

    Raises:
    - ValueError: If session is None.

    Notes:
    - Proper error handling is implemented to catch exceptions during the request.
    
    Example Usage:
    >>> await batch_move_tasks_status(gandiva_requests=[{"Id": "TEST-1", "Status": 4}])
    """

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

    async with aiohttp.ClientSession() as session:
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
                    transition_id=transition_id,
                    session=session)
            except Exception as e:
                logging.error(f"Exception occurred while processing task {task_id_gandiva}: {str(e)}")

async def batch_edit_tasks(values: dict,
                           issues: list):

    # Prepare the request body
    body = {
        "values": values,
        "issues": issues
    }

    # Convert the body to a JSON string
    body_json = json.dumps(body)

    # Construct the URL for moving the task
    url = f"{HOST}/v2/bulkchange/_update"

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=body_json, headers=HEADERS) as response:
                sc = response.status
                response_text = await response.text()  # Get the response text for logging

                if sc in [200, 201]:
                    logging.info(f"Bulkchange good")
                    return await response.json()  # Return the JSON response
                else:
                    logging.error(f"Failed to Bulkchange. // [{sc}]")
                    return False  # Return False for any other error
    except Exception as e:
        logging.error(f"Exception occurred while Bulkchanging")
        return False  # Return False if an exception occurs

# TODO
async def add_comments_to_task(task_id_comment: dict):
    pass
# TODO
async def batch_add_comments_to_tasks(task_id_comments: dict):
    pass

def filter_tasks_with_unique(tasks):
    """
    Filters and returns only the tasks that contain the 'unique' key.

    Parameters:
    - tasks (list[dict]): List of tasks, each represented as a dictionary.

    Returns:
    - list[dict]: A list of tasks that have the 'unique' key.
    """
    return [task for task in tasks if 'unique' in task]

async def add_existing_tracker_tasks_to_db():
    res = await get_all_tasks()
    res = filter_tasks_with_unique(res)
    db.add_tasks_to_db(session=DB_SESSION, tasks=res)

async def get_sprints_on_board(board_id: str) -> dict:
    """Returns task (dictionary type) """

    url = f"{HOST}/v2/boards/{board_id}/sprints"

    # Use aiohttp to make the HTTP POST request
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=HEADERS) as response:
            # Check if the response is successful
            if response.status in [200, 201]:
                return await response.json()  # Return the JSON response
            else:
                logging.error(f"Failed to get sessions: {response.status} - {await response.text()}")
                return None



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