import logging
import json
import asyncio
import gandiva_api
import datetime
import utils
from utils import make_http_request
import configparser
import db_module as db
import urllib.parse

# Path to the .ini file
CONFIG_PATH = 'config.ini'
config = configparser.ConfigParser()
config.read(CONFIG_PATH)

# Globals
YA_X_CLOUD_ORG_ID                   = config.get('Yandex', 'x_cloud_org_id')
YA_ACCESS_TOKEN                     = config.get('Yandex', 'oauth_token')
YA_CLIENT_ID                        = config.get('Yandex', 'client_id')
YA_CLIENT_SECRET                    = config.get('Yandex', 'client_secret')
YA_REFRESH_TOKEN                    = ''
YA_FIELD_ID_GANDIVA                 = config.get('YandexCustomFieldIds', 'gandiva') 
YA_FIELD_ID_INITIATOR               = config.get('YandexCustomFieldIds', 'initiator') 
YA_FIELD_ID_INITIATOR_DEPARTMENT    = config.get('YandexCustomFieldIds', 'initiator_department') 
GANDIVA_TASK_URL                    = 'https://gandiva.s-stroy.ru/Request/Edit/'
DB_URL                              = config.get('Database', 'url')
DB_ENGINE                           = db.create_database(DB_URL)
DB_SESSION                          = db.get_session(DB_ENGINE)

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


# Authorization block

async def refresh_access_token(refresh_token=None) -> dict:
    """
    Refresh access_token using refresh_token,
    update global variables and
    save changes to .env file.
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

    # Use make_http_request to send the POST request
    response_json = await make_http_request("POST", YA_OAUTH_TOKEN_URL, headers=headers, body=payload_url_encoded)

    if response_json:
        access_token = response_json.get('access_token')
        refresh_token = response_json.get('refresh_token')

        global YA_ACCESS_TOKEN
        YA_ACCESS_TOKEN = access_token
        YA_REFRESH_TOKEN = refresh_token

        return access_token
    else:
        logging.error("Failed to refresh access token.")
        return None

async def check_access_token(access_token) -> str:
    """
    Recursive way to check if access token works.
    Returns only when login was successful (+ refresh the token).
    """
    # Get account info using the provided access token
    response = await get_account_info(access_token)

    # Check if the response is successful
    if response:
        logging.info(f"Authorized user: {response['login']} [Yandex Tracker]")
        return

    # If the response is not successful, refresh the access token
    yandex_access_token = await refresh_access_token()

    # Recheck the new access token
    return await check_access_token(yandex_access_token)

async def get_access_token_captcha() -> str:
    """
    Retrieves the Yandex access token via manual captcha input by sending a POST request to the Yandex OAuth token URL.
    Prompts the user for the captcha code until a valid access token is obtained.
    """

    # Handling code retrieved manually from Yandex
    yandex_id_metadata = {}

    while not yandex_id_metadata.get("access_token"):
        yandex_captcha_code = input(f"Code (get from https://oauth.yandex.ru/authorize?response_type=code&client_id={YA_CLIENT_ID}): ")
        
        # Prepare the payload for the request
        payload = {
            "grant_type": "authorization_code",
            "code": yandex_captcha_code,
            "client_id": YA_CLIENT_ID,
            "client_secret": YA_CLIENT_SECRET,
        }

        # Use make_http_request to send the POST request
        response_json = await make_http_request("POST", YA_OAUTH_TOKEN_URL, body=payload)

        if response_json:
            yandex_id_metadata = response_json
            logging.info("Successfully retrieved access token.")
        else:
            logging.error("Failed to retrieve access token. Please try again.")

    yandex_access_token = yandex_id_metadata.get("access_token")
    return yandex_access_token

# Utils specific to Yandex

async def convert_gandiva_to_yandex_fields(task: dict, edit: bool):
    """
    Converts fields from Gandiva task format to Yandex task format.

    Args:
        task (dict): The Gandiva task dictionary.
        edit (bool): If True, converts fields for editing tasks. If False, for adding tasks.

    Returns:
        dict: Converted Yandex task fields.
    """
    # Common fields for both add and edit tasks
    gandiva_task_id = str(task['Id'])
    initiator_name = f"{task['Initiator']['FirstName']} {task['Initiator']['LastName']}"
    description_html = task['Description']
    description = utils.html_to_yandex_format(description_html)  # Assuming a helper function to extract text from HTML
    initiator_id = task['Initiator']['Id']
    initiator_department = await gandiva_api.get_department_by_user_id(user_id=initiator_id)
    
    # Convert dates if they exist
    gandiva_start_date = task.get('RequiredStartDate')
    yandex_start_date = None
    if gandiva_start_date:
        yandex_start_date = utils.gandiva_to_yandex_date(gandiva_start_date)

    # Fields that are specific to the "add" operation
    if not edit:
        assignee_id_yandex = ""  # Assuming this is only used in the add task
        task_type = "improvement"  # Task type specific to add tasks

        return {
            'gandiva_task_id': gandiva_task_id,
            'initiator_name': initiator_name,
            'description': description,
            'assignee_id_yandex': assignee_id_yandex,
            'task_type': task_type,
            'initiator_id': initiator_id,
            'initiator_department': initiator_department,
            'yandex_start_date': yandex_start_date
        }

    # Fields that are specific to the "edit" operation
    return {
        'gandiva_task_id': gandiva_task_id,
        'initiator_name': initiator_name,
        'description': description,
        'initiator_id': initiator_id,
        'initiator_department': initiator_department,
        'yandex_start_date': yandex_start_date
    }

# Functions

async def get_account_info(access_token):
    """
    Gets Yandex account info by providing the access_token.
    """
    # Prepare the request parameters
    data = {
        "oauth_token": access_token,
        "format": "json",
    }
    
    # Make the HTTP POST request
    response = await make_http_request('POST', YA_INFO_TOKEN_URL, headers=HEADERS, body=json.dumps(data))

    return response  # Return the response object

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
        logging.error(f"Page {page_number} of tasks with query: {body} is empty.")
        return None

async def get_tasks_count(queue: str = None,
                            filter: dict = None,
                            query: str = None,
                            login: str = None):
    # Initialize the request body
    body = {}

    # Respect the priority of parameters: queue > keys > filter > query
    if queue:
        body["filter"] = {"queue": queue}
        if login:
            body["filter"]["assignee"] = login
        if filter:
            body["filter"].update(filter)
    elif filter:
        body["filter"] = filter
        if login:
            body["filter"]["assignee"] = login
    elif login:
        body["filter"] = {"assignee": login}  # Allow filtering by login alone
    elif query:
        body["query"] = query

    # Convert the body to a JSON string
    body_json = json.dumps(body)

    # Construct the URL with the expand and perPage parameters
    url = f"{HOST}/v2/issues/_count"

    # Use the helper function to make the POST request
    response_json = await make_http_request('POST', url, headers=HEADERS, body=body_json)

    # Return the response JSON or None if the request failed
    if response_json:
        return response_json
    else:
        logging.error(f"Tasks with query: {body} were not found.")
        return None

async def get_all_tasks(queue: str,
                        keys: str | list[str] = None,
                        filter: dict = None,
                        query: str = None,
                        order: str = None,
                        expand: str = None,
                        login: str = None):

    page_number = 1

    tasks_count = await get_tasks_count(queue=queue, filter=filter,query=query,login=login)

    yandex_tasks = await get_page_of_tasks(
        queue=queue,
        keys=keys,
        filter=filter,
        query=query,
        order=order,
        expand=expand,
        login=login,
        page_number=page_number,
        per_page=tasks_count)

    # Append the tasks to the all_yandex_tasks list

    return yandex_tasks

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
                   queue,
                   description="Без описания",
                   assignee_yandex_id=None,
                   task_type=None,
                   initiator_department=None,
                   start=None):

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
        YA_FIELD_ID_INITIATOR: initiator_name,
        YA_FIELD_ID_GANDIVA: GANDIVA_TASK_URL + task_id_gandiva,
        "unique": task_id_gandiva,
    }

    # Optionally add other fields
    if assignee_yandex_id:
        body["assignee"] = assignee_yandex_id
    if task_type:
        body["type"] = task_type
    if initiator_department:
        body[YA_FIELD_ID_INITIATOR_DEPARTMENT] = initiator_department
    if start:
        body["start"] = start

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
            logging.info(f"Task {task_id_yandex} added to Tracker and database.")
            return response_json
        else:
            logging.error(f"Failed to retrieve task key from response for task {task_id_gandiva}")
            return None
    else:
        logging.error(f"Failed to add task {task_id_gandiva}")
        return None

async def add_tasks(tasks_gandiva, queue):
    """
    Adds tasks from Gandiva to Yandex Tracker if they do not already exist.
    """
    logging.info("Adding tasks...")

    # Initialize a counter for added tasks
    added_task_count = 0

    for task in tasks_gandiva:
        fields = await convert_gandiva_to_yandex_fields(task, edit=False)
        initiator_name = fields.get('initiator_name')
        gandiva_task_id = fields.get('gandiva_task_id')
        initiator_department=fields.get('initiator_department')
        description=fields.get('description')
        assignee_yandex_id=fields.get('assignee_id_yandex')
        task_type=fields.get('task_type')
        start=fields.get('yandex_start_date')
        # Call add_task and check if a task was successfully added
        result = await add_task(gandiva_task_id,
                                initiator_name,
                                initiator_department=initiator_department,
                                description=description,
                                queue=queue,
                                assignee_yandex_id=assignee_yandex_id,
                                task_type=task_type,
                                start=start)

        # Check if the result indicates the task was added (assuming it returns a dict on success)
        if isinstance(result, dict):
            added_task_count += 1
        else:
            logging.debug(f"Task {gandiva_task_id} could not be added or already exists.")

    # Log the total number of added tasks
    logging.info(f"Total tasks added: {added_task_count}")

async def edit_tasks(tasks_gandiva, ya_tasks):
    """
    Edits tasks in Yandex Tracker based on the data from Gandiva.
    Only updates tasks that are outdated.
    """
    logging.info(f"Editing tasks...")
    
    # Create a dictionary to map Yandex task unique IDs to their corresponding task objects
    ya_tasks_dict = {yt['unique']: yt for yt in ya_tasks if yt.get('unique')}
    
    # Initialize a counter for edited tasks
    edited_task_count = 0

    for task in tasks_gandiva:
        fields = await convert_gandiva_to_yandex_fields(task, edit=True)
        initiator_name = fields.get('initiator_name')
        initiator_department = fields.get('initiator_department')
        description = fields.get('description')
        gandiva_task_id = fields.get('gandiva_task_id')
        yandex_start_date = fields.get('yandex_start_date')

        # Find the corresponding Yandex task using the dictionary
        task_yandex = ya_tasks_dict.get(gandiva_task_id)

        if task_yandex:
            # Determine which fields need to be updated
            edit_initiator_name = None
            edit_initiator_department = None
            edit_description = None
            edit_yandex_start_date = None
            task_id_yandex = task_yandex.get('key')
            edit = False
            if initiator_name and not task_yandex.get(YA_FIELD_ID_INITIATOR):
                edit_initiator_name = initiator_name
                edit = True
            if initiator_department and not task_yandex.get(YA_FIELD_ID_INITIATOR_DEPARTMENT):
                edit_initiator_department = initiator_department
                edit = True
            if description and not task_yandex.get('description'):
                edit_description = description
                edit = True
            if yandex_start_date and not task_yandex.get('start'):
                edit_yandex_start_date = yandex_start_date
                edit = True

            # TODO: add analyst and assignee later
            # if not task_yandex['66d379ee99b2de14092e0185--Analyst']:
            #     edit_analyst = (???)
            # if not task_yandex['assignee'] and assignee_id_yandex:
            #     edit_assignee_id_yandex = assignee_id_yandex

            # If any field needs to be edited, call the edit function
            if edit:
                await edit_task(task_id_yandex=task_id_yandex,
                                initiator_name=edit_initiator_name,
                                initiator_department=edit_initiator_department,
                                description=edit_description,
                                start=edit_yandex_start_date)
                # Increment the counter
                edited_task_count += 1
            else:
                logging.debug(f"Task {gandiva_task_id} is already up-to-date.")

    # Log the total number of edited tasks
    logging.info(f"Total tasks edited: {edited_task_count}")


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
                    start=None):

    # Convert the task ID to a string
    task_id_yandex = str(task_id_yandex)

    # Prepare the request body
    body = {}
    # Fetch task from the database
    task_in_db = db.find_task_by_yandex_id(session=DB_SESSION, task_id_yandex=task_id_yandex)
    if task_in_db:
        task_id_gandiva = task_in_db.task_id_gandiva
        body[YA_FIELD_ID_GANDIVA] = GANDIVA_TASK_URL + task_id_gandiva
    
    # Add fields to the request body if provided
    if summary: body["summary"] = summary
    if description: body["description"] = description
    if assignee_id_yandex: body["assignee"] = assignee_id_yandex
    if task_type: body["type"] = task_type
    if priority: body["priority"] = priority
    if parent: body["parent"] = parent
    if sprint: body["sprint"] = sprint
    if followers: body["followers"] = {"add": followers}
    if initiator_name: body[YA_FIELD_ID_INITIATOR] = initiator_name
    if initiator_department: body[YA_FIELD_ID_INITIATOR_DEPARTMENT] = initiator_department
    if start: body["start"] = start

    # Convert the body to a JSON string
    body_json = json.dumps(body)

    # Construct the URL for updating the task
    url = f"{HOST}/v2/issues/{task_id_yandex}"

    # Use the helper function to make the PATCH request
    response = await make_http_request('PATCH', url, headers=HEADERS, body=body_json)
    if response:
        logging.info(f"Successfully edited task {task_id_yandex}")
        return response
    else:
        logging.error(f"Failed editing task {task_id_yandex}")
        return None

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
        comment = f"Task status has been moved to {transition_id} for {task_id_yandex}."

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

async def move_tasks_status(tasks, new_status):
    """Returns sprints (as a dictionary) for the given board."""
    url = f"{HOST}/v2/bulkchange/_transition"
    transition = utils.get_yandex_transition_from_status(new_status)
    task_keys = utils.extract_task_keys(tasks)
    body = {
        "transition": transition,
        "issues": task_keys
    }

    # Convert the body to a JSON string
    body_json = json.dumps(body)

    response_json =  await make_http_request('POST', url, headers=HEADERS, body=body_json)

    # Return the response JSON or None if the request failed
    if response_json:
        logging.info(f"Tasks were moved to new status {new_status}: {len(task_keys)}")
        return response_json
    else:
        logging.error(f"Tasks were not moved to new status {new_status}")
        return None

async def move_groups_tasks_status(grouped_yandex_tasks: dict):
    """Processes grouped Yandex tasks by calling move_group_of_tasks_status for each group asynchronously."""
    tasks = []
    
    for new_status, tasks_list in grouped_yandex_tasks.items():
        # Create an asyncio task for each group
        tasks.append(move_tasks_status(tasks_list, new_status))
    
    # Run all tasks concurrently and wait for their completion
    results = await asyncio.gather(*tasks)
    
    return results

async def batch_move_tasks_status(g_tasks, ya_tasks):
    grouped_ya_tasks = utils.filter_and_group_tasks_by_new_status(gandiva_tasks=g_tasks, yandex_tasks=ya_tasks)
    await move_groups_tasks_status(grouped_ya_tasks)
    logging.info("Task statuses are up-to-date!")

async def batch_edit_tasks(values: dict, task_ids: list):
    """Bulk update tasks in Yandex Tracker."""

    # Prepare the request body
    body = {
        "values": values,
        "issues": task_ids
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

async def create_sprint(board_id: str, name: str, start_date: str, end_date: str):
    """Creates sprint for the given board.
    date_format: YYYY-MM-DD"""
    url = f"{HOST}/v2/sprints"
    body = {
  "name": name,
  "board": 
    {
      "id": board_id
    },
  "startDate": start_date,
  "endDate": end_date
}
    body_json = json.dumps(body)
    res = await make_http_request('POST', url, headers=HEADERS, body=body_json)
    if res:
        logging.info(f"Sprint '{name}' [{start_date} - {end_date}] was successfully created.")
    else:
        logging.error(f"Failed to create new sprint.")

async def create_weekly_release_sprint(board_id: int, day_of_the_week: int = 3, sprint_length_days: int = 8):
    """Creates a sprint starting on Thursday and ending on the next Thursday with the name 'Релиз [End Date]'.
       Skips creating if a sprint for the same period already exists."""
    
    today = datetime.date.today()
    next_thursday = today + datetime.timedelta((day_of_the_week - today.weekday()) % 7)

    # Start date is the upcoming Thursday
    start_date = next_thursday
    
    end_date = start_date + datetime.timedelta(days=sprint_length_days)
    
    # Extract day and month for formatting
    day = end_date.day
    month = utils.MONTHS_RUSSIAN[end_date.month]

    # Format the name as 'Релиз [End Date in Russian]'
    sprint_name = f"Релиз {day} {month}"
    
    # Get all sprints for the board
    existing_sprints = await get_sprints_on_board(board_id)
    
    # Check if a sprint with the same date range or name already exists
    for sprint in existing_sprints:
        sprint_start = datetime.date.fromisoformat(sprint['startDate'])
        sprint_end = datetime.date.fromisoformat(sprint['endDate'])
        if sprint_start == start_date and sprint_end == end_date and sprint['name'] == sprint_name:
            logging.info(f"Sprint '{sprint_name}' [{sprint_start} - {sprint_end}] already exists.")
            return sprint

    # Call the API to create the sprint since it doesn't exist
    return await create_sprint(board_id, sprint_name, start_date.isoformat(), end_date.isoformat())

async def get_page_of_comments(yandex_task_id: str, per_page: int = 100, after_id: str = None) -> tuple:
    """
    Fetches comments for the given Yandex task with pagination support.
    
    :param yandex_task_id: ID of the Yandex task
    :param per_page: Number of comments to fetch per page
    :param after_id: The ID of the last comment from the previous page (if any)
    :return: A tuple of (comments, next_page_url)
    """
    url = f"{HOST}/v2/issues/{yandex_task_id}/comments?perPage={per_page}"
    if after_id:
        url += f"&id={after_id}"
    
    response = await make_http_request('GET', url, headers=HEADERS)

    comments = response
    return comments

async def get_all_comments(yandex_task_id: str, per_page: int = 100) -> list:
    """
    Retrieves all comments for the given Yandex task, handling pagination.
    
    :param yandex_task_id: ID of the Yandex task
    :param per_page: Number of comments to fetch per page
    :return: A list of all comments
    """
    all_comments = []
    after_id = None

    while True:
        comments = await get_page_of_comments(yandex_task_id, per_page=per_page, after_id=after_id)
        all_comments.extend(comments)

        if len(comments) < per_page:
            break
        # Extract the last comment's ID to fetch the next page
        after_id = None
        if comments:
            after_id = comments[-1]['id']

        # If no next page is found, break out of the loop
        if not after_id:
            break


    return all_comments

async def add_comment(yandex_task_id: str, comment: str, g_comment_id: str = None, author_name: str = None):
    url = f"{HOST}/v2/issues/{yandex_task_id}/comments"
    
    # Format the comment according to the specified template
    if g_comment_id and author_name:
        comment = f"[{g_comment_id}] {author_name}:\n{comment}"
    else:
        comment = f"{comment}"  # In case g_comment_id or author_name are None, keep the original comment

    body = {
        "text": comment
    }
    # Convert the body to a JSON string
    body_json = json.dumps(body)
    response_json = await make_http_request("POST", url=url, headers=HEADERS, body=body_json)
        # Return the response JSON or None if the request failed
    if response_json:
        logging.debug(f"Comment was succesfully added to task {yandex_task_id}!")
        return response_json
    else:
        logging.error(f"Comment was not added to task {yandex_task_id}.")
        return None



import time # using for timing functions
async def main(): 
    utils.setup_logging()
    # await refresh_access_token(YA_REFRESH_TOKEN)
    res = await create_weekly_release_sprint(52)
    res1 = await get_sprints_on_board(52)
    pass


if __name__ == '__main__':
    asyncio.run(main())