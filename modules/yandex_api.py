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

    # Set up the headers
    headers = {
        "Content-type": "application/json",
        "X-Cloud-Org-Id": YA_X_CLOUD_ORG_ID,
        "Authorization": f"OAuth {YA_ACCESS_TOKEN}"
    }

    # Construct the URL with the expand and perPage parameters
    url = f"{HOST}/v2/issues/_search?perPage={per_page}"
    if page_number > 1:
        url += f"&page={page_number}"
    if expand:
        url += f"&expand={expand}"

    # Use aiohttp to make the HTTP POST request
    async with aiohttp.ClientSession() as session:
        async with session.post(url, data=body_json, headers=headers) as response:
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

async def add_task(task_id_gandiva,
                   initiator_name,
                   description="Без описания",
                   queue="TEA",
                   assignee_yandex_login="",
                   task_type="",
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

async def add_tasks(tasks_gandiva, queue="TEA"):
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

async def edit_task(task_id_yandex,
                    summary=None,
                    description=None,
                    assignee_yandex_login=None,
                    task_type=None,
                    priority=None,
                    parent=None,
                    sprint=None,
                    followers=None,
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

    if summary:
        body["summary"] = summary
    if description:
        body["description"] = description
    if assignee_yandex_login:
        body["assignee"] = assignee_yandex_login
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

    # Convert the body to a JSON string
    body_json = json.dumps(body)

    # Set up the headers
    headers = {
        "Content-type": "application/json",
        "X-Cloud-Org-Id": YA_X_CLOUD_ORG_ID,
        "Authorization": f"OAuth {YA_ACCESS_TOKEN}"
    }

    # Construct the URL for updating the task
    url = f"{HOST}/v2/issues/{task_id_yandex}"

    try:
        # Make the HTTP PATCH request using the session
        async with session.patch(url, data=body_json, headers=headers) as response:
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


import time # using for timing functions
async def main():
    
    # await refresh_access_token(YA_REFRESH_TOKEN)
    start_time = time.time()
    # async with aiohttp.ClientSession() as session:
    #     res = await edit_task(task_id_yandex="tea-1", summary="new summary", description="new desc", followers="phtea", session=session)
    res = await get_all_tasks(expand="transitions", queue="tea")
    print(res)
    print("--- %s seconds ---" % (time.time() - start_time))
    pass

if __name__ == '__main__':
    asyncio.run(main())