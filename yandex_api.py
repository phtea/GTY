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
import re

# Path to the .ini file
CONFIG_PATH = 'config.ini'
config = configparser.ConfigParser()
config.read(CONFIG_PATH)

# Globals
YA_X_CLOUD_ORG_ID                   = config.get('Yandex', 'x_cloud_org_id')
YA_ACCESS_TOKEN                     = config.get('Yandex', 'oauth_token')
YA_CLIENT_ID                        = config.get('Yandex', 'client_id')
YA_CLIENT_SECRET                    = config.get('Yandex', 'client_secret')
YA_ROBOT_ID                         = config.get('Yandex', 'robot_id')
YA_FIELD_ID_GANDIVA                 = config.get('YandexCustomFieldIds', 'gandiva') 
YA_FIELD_ID_INITIATOR               = config.get('YandexCustomFieldIds', 'initiator') 
YA_FIELD_ID_INITIATOR_DEPARTMENT    = config.get('YandexCustomFieldIds', 'initiator_department')
YA_FIELD_ID_ANALYST                 = config.get('YandexCustomFieldIds', 'analyst')
YA_FIELD_ID_GANDIVA_TASK_ID         = config.get('YandexCustomFieldIds', 'gandiva_task_id')
YA_FIELD_ID_ND                      = config.get('YandexCustomFieldIds', 'nd')
YA_FIELD_ID_DATE_CREATED_IN_G       = config.get('YandexCustomFieldIds', 'date_created_in_g')
DB_URL                              = config.get('Database', 'url')
YA_REFRESH_TOKEN                    = ''
YA_DISK_ACCESS_TOKEN                = config.get('Yandex', 'disk_oauth_token')
GANDIVA_TASK_URL                    = 'https://gandiva.s-stroy.ru/Request/Edit/'

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
HEADERS_DISK = {
    "Authorization": f"OAuth {YA_DISK_ACCESS_TOKEN}"
}

def get_query_in_progress(queue):
    query = f'Resolution: empty() "Status Type": !cancelled "Status Type": !done Queue: {queue} "Sort by": Updated DESC'
    return query

def get_query_all(queue):
    query = f'Queue: {queue} "Sort by": Updated DESC'
    return query

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
    response = await make_http_request("POST", YA_OAUTH_TOKEN_URL, headers=headers, body=payload_url_encoded)

    if not response:
        logging.error("Failed to refresh access token.")
        return None

    access_token = response.get('access_token')
    refresh_token = response.get('refresh_token')

    global YA_ACCESS_TOKEN
    YA_ACCESS_TOKEN = access_token
    YA_REFRESH_TOKEN = refresh_token

    return access_token

async def check_access_token(access_token) -> str:
    """
    Recursive way to check if access token works.
    Returns only when login was successful (+ refresh the token).
    """
    # Get account info using the provided access token
    response = await get_account_info(access_token)

    # Check if the response is successful
    if response:
        logging.info(f"Authorized user: {response['login']}")
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

async def g_to_y_fields(g_task: dict, edit: bool, to_get_followers: bool = False):
    """
    Converts fields from Gandiva task format to Yandex task format.

    Args:
        task (dict): The Gandiva task dictionary.
        edit (bool): If True, converts fields for editing tasks. If False, for adding tasks.

    Returns:
        dict: Converted Yandex task fields.
    """
    # Common fields for both add and edit tasks
    g_task_id               = str(g_task['Id'])
    initiator_name          = f"{g_task['Initiator']['FirstName']} {g_task['Initiator']['LastName']}"
    description_html        = g_task['Description']
    description             = utils.html_to_yandex_format(description_html)  # Assuming a helper function to extract text from HTML
    initiator_id            = g_task['Initiator']['Id']
    initiator_department    = await gandiva_api.get_department_by_user_id(g_user_id=initiator_id)
    nd                      = None
    g_task_detailed         = None
    
    db_session              = None
    if initiator_department:
        db_session = db.get_db_session()
        nd = db.get_nd_by_department_name(session=db_session, department_name=initiator_department)
    
    # Add assignee if exists
    g_assignee        = g_task.get('Contractor')
    g_assignee_id     = None
    if g_assignee:
        g_assignee_id = g_assignee.get('Id')
    y_assignee_id = None
    if g_assignee_id:
        if db_session is None:
            db_session = db.get_db_session()
        assignee = db.get_user_by_gandiva_id(session=db_session, g_user_id=g_assignee_id)
        y_assignee_id = assignee.yandex_user_id if assignee else None

    # Convert dates if they exist
    y_start_date        = get_date_convert(g_task, "RequiredStartDate")
    y_date_created_in_g = get_date_convert(g_task, "CreateDate")
    
    # Fields that are specific to the "add" operation
    if not edit:
        task_type = "improvement"  # Task type specific to add tasks
        return {
            'gandiva_task_id': g_task_id,
            'initiator_name': initiator_name,
            'description': description,
            'assignee_id_yandex': y_assignee_id,
            'task_type': task_type,
            'initiator_id': initiator_id,
            'initiator_department': initiator_department,
            'yandex_start_date': y_start_date,
            'nd': nd,
            'date_created_in_g': y_date_created_in_g
        }
    
    followers = []
    if to_get_followers:
        if not g_task_detailed: g_task_detailed = await gandiva_api.get_task(g_task_id)
        observers = utils.extract_observers_from_detailed_task(g_task_detailed)
        if db_session is None:
            db_session = db.get_db_session()
        followers = db.convert_gandiva_observers_to_yandex_followers(session=db_session, gandiva_observers=observers)

    # Fields that are specific to the "edit" operation
    return {
        'gandiva_task_id': g_task_id,
        'initiator_name': initiator_name,
        'description': description,
        'initiator_id': initiator_id,
        'initiator_department': initiator_department,
        'yandex_start_date': y_start_date,
        'assignee_id_yandex': y_assignee_id,
        'followers': followers,
        'nd': nd,
        'date_created_in_g': y_date_created_in_g
    }

def get_date_convert(g_task: dict, key: str):
    g_start_date = g_task.get(key)
    y_start_date = None
    if g_start_date:
        y_start_date = utils.g_to_y_date(g_start_date)
    return y_start_date

# Functions

async def get_account_info(access_token):
    """
    Gets Yandex account info by providing the access_token.
    """
    data = {
        "oauth_token": access_token,
        "format": "json",
    }
    
    response = await make_http_request('POST', YA_INFO_TOKEN_URL, headers=HEADERS, body=json.dumps(data))
    return response

async def get_page_of_tasks(queue: str = None, keys: str | list[str] = None, filter: dict = None,
                            query: str = None, order: str = None, expand: str = None,
                            login: str = None, page_number: int = 1, per_page: int = 5000) -> dict:
    # Initialize the request body
    body = {}

    # Respect the priority of parameters: queue > keys > filter > query
    # Our priority of parameters: query > queue > filter
    if query:
        body["query"] = query
    elif queue:
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

    # Return the response JSON or [] if the request failed
    if response_json:
        return response_json
    elif response_json == []:
        return []
    else:
        logging.error(f"Page {page_number} of tasks with query: {body} is empty.")
        return None

async def get_tasks_count(queue: str = None, filter: dict = None,
                        query: str = None, assignee: str = None):
    # Initialize the request body
    body = {}

    # Respect the priority of parameters: queue > keys > filter > query
    # Our priority is: query > queue > filter
    if query:
        body["query"] = query
    elif queue:
        body["filter"] = {"queue": queue}
        if assignee:
            body["filter"]["assignee"] = assignee
        if filter:
            body["filter"].update(filter)
    elif filter:
        body["filter"] = filter
        if assignee:
            body["filter"]["assignee"] = assignee
    elif assignee:
        body["filter"] = {"assignee": assignee}  # Allow filtering by login alone
    

    # Convert the body to a JSON string
    body_json = json.dumps(body)

    # Construct the URL with the expand and perPage parameters
    url = f"{HOST}/v2/issues/_count"

    # Use the helper function to make the POST request
    response = await make_http_request('POST', url, headers=HEADERS, body=body_json)

    # Return the response JSON or None if the request failed
    if response:
        return response
    else:
        logging.error(f"Tasks with query: {body} were not found.")
        return None

async def get_tasks(queue: str = None, keys: str | list[str] = None,
                        filter: dict = None, query: str = None,
                        order: str = None, expand: str = None, login: str = None):
    
    if not queue and not query:
        raise ValueError("Queue or query have to be passed to this function.")

    

    # tasks_count = await get_tasks_count(queue=queue, filter=filter, query=query,assignee=login)
    per_page = 2000
    all_y_tasks = []
    page_number = 1
    while True:
        y_tasks = await get_page_of_tasks(queue=queue, keys=keys, filter=filter,query=query,
                                          order=order, expand=expand,login=login,page_number=page_number,
                                          per_page=per_page)
        if y_tasks is None:
            raise ValueError("[429] Too many requests.")
        all_y_tasks.extend(y_tasks)
        if len(y_tasks) < per_page: break
        page_number += 1


    # Append the tasks to the all_yandex_tasks list

    return all_y_tasks

async def get_tasks_by_gandiva_ids(g_ids: list[str]) -> dict:
    """Get tasks by their Gandiva IDs"""

    # Initialize the request body
    body = {
        "filter": {YA_FIELD_ID_GANDIVA_TASK_ID: g_ids}
    }

    # Convert the body to a JSON string
    body_json = json.dumps(body)

    # Construct the URL
    url = f"{HOST}/v2/issues/_search?perPage=5000"

    # Use the helper function to make the POST request
    response = await make_http_request('POST', url, headers=HEADERS, body=body_json)

    # Return the response JSON or None if the request failed
    if response:
        return response
    else:
        logging.error(f"Failed to get tasks with Gandiva IDs: {g_ids}")
        return None

async def get_page_of_users(page: int = 1, per_page: int = 100):
    """Returns page of users """

    url = f"{HOST}/v2/users?perPage={per_page}&page={page}"
    
    response = await make_http_request("GET", url, headers=HEADERS)
    
    if not response:
        logging.error(f"Failed to get users.")
        return None
    
    return response

async def get_all_users(per_page: int = 100) -> list:
    """
    """
    all_users = []
    page = 1
    while True:
        users = await get_page_of_users(page=page, per_page=per_page)
        all_users.extend(users)
        if len(users) < per_page:
            break
        page += 1


    return all_users

async def get_task(y_task_id: str) -> dict:
    """Returns task (dictionary type) """

    url = f"{HOST}/v2/issues/{y_task_id}"
    
    response = await make_http_request("GET", url, headers=HEADERS)
    
    if not response:
        logging.error(f"Failed to get task {y_task_id}.")
        return None
    return response

async def add_task(g_task_id, initiator_name, queue,
                   description="Без описания", y_assignee=None,
                   task_type=None, initiator_department=None, start=None, nd=None, y_step=None, date_created_in_g=None):

    g_task_id = str(g_task_id)
    db_session = db.get_db_session()
    task_in_db = db.get_task_by_gandiva_id(db_session, g_task_id=g_task_id)
    if task_in_db:
        return task_in_db

    # Construct the task summary
    task_summary = f"{g_task_id}. {description[:50]}..."

    # Prepare the request body
    body = {
        "summary": task_summary,
        "description": description,
        "queue": queue,
        YA_FIELD_ID_INITIATOR: initiator_name,
        YA_FIELD_ID_GANDIVA: GANDIVA_TASK_URL + g_task_id,
        YA_FIELD_ID_GANDIVA_TASK_ID: g_task_id,
        "unique": queue + g_task_id
    }

    # Optionally add other fields
    if y_assignee: body["assignee"] = y_assignee
    if task_type: body["type"] = task_type
    if initiator_department:
        body[YA_FIELD_ID_INITIATOR_DEPARTMENT] = initiator_department
        if db_session is None:
            db_session = db.get_db_session()
        analyst = db.get_user_id_by_department(session=db_session, department_name=initiator_department)
        if analyst: body[YA_FIELD_ID_ANALYST] = analyst
    if y_step in [0,1,2]: body["assignee"] = analyst
    if nd: body[YA_FIELD_ID_ND] = nd
    if date_created_in_g: body[YA_FIELD_ID_DATE_CREATED_IN_G] = date_created_in_g
    if start: body["start"] = start

    # Convert the body to a JSON string
    body_json = json.dumps(body)

    # Construct the URL
    url = f"{HOST}/v2/issues"

    # Use the helper function to make the POST request
    response = await make_http_request('POST', url, headers=HEADERS, body=body_json)

    # Process the response
    if not (response and isinstance(response, dict)):
        logging.error(f"Failed to add task {g_task_id}")
        return None
    task_id_yandex = response.get('key')

    if not task_id_yandex:
        logging.error(f"Failed to retrieve task key from response for task {g_task_id}")
        return None
    
    # Add task to the database
    if db_session is None:
        db_session = db.get_db_session()
    db.add_or_update_task(session=db_session, g_task_id=g_task_id, y_task_id=task_id_yandex)
    return response

async def add_tasks(g_tasks: list[dict], queue: str, non_closed_ya_task_ids: dict):
    """
    Adds tasks from Gandiva to Yandex Tracker if they do not already exist.
    """
    logging.info("Adding tasks...")

    # Initialize a counter for added tasks
    added_task_count = 0

    for g_task in g_tasks:

        g_task_id = str(g_task['Id'])
        if g_task_id in non_closed_ya_task_ids.keys():
            logging.debug(f"Task {g_task_id} already exists.")
            db_session = db.get_db_session()
            db.add_or_update_task(session=db_session, g_task_id=g_task_id, y_task_id=non_closed_ya_task_ids[g_task_id])
            continue

        fields = await g_to_y_fields(g_task, edit=False)
        initiator_name      = fields.get('initiator_name')
        initiator_department= fields.get('initiator_department')
        description         = fields.get('description')
        y_assignee          = fields.get('assignee_id_yandex')
        task_type           = fields.get('task_type')
        start               = fields.get('yandex_start_date')
        nd                  = fields.get('nd')
        y_date_created_in_g = fields.get('date_created_in_g')
        
        new_description     = await description_with_attachments(g_task_id, description)

        g_status            = g_task['Status']
        y_status            = utils.g_to_y_status(g_status)
        y_step              = utils.y_status_to_step(y_status)

        # Call add_task and check if a task was successfully added
        result = await add_task(g_task_id, initiator_name, initiator_department=initiator_department,
                                description=new_description, queue=queue, y_assignee=y_assignee,
                                task_type=task_type, start=start, nd=nd, y_step=y_step, date_created_in_g=y_date_created_in_g)

        # Check if the result indicates the task was added
        if isinstance(result, dict):
            added_task_count += 1
        elif isinstance(result, db.Task):
            logging.debug(f"Task {g_task_id} already exists.")
        else:
            logging.debug(f"Task {g_task_id} could not be added or already exists.")

    # Log the total number of added tasks
    logging.info(f"Total tasks added: {added_task_count}")
    return added_task_count

def edit_field_if_empty(y_task, field_id, new_value):
    """
    Helper function to determine if a field needs to be updated based on it being empty or not.
    
    :param task_yandex: The task object from Yandex.
    :param field_id: The field ID to check in the Yandex task.
    :param new_value: The new value that might need to be applied.
    :return: The new value if it needs to be updated, None otherwise.
    """
    if new_value and not y_task.get(field_id):
        return new_value
    return None

def edit_field_if_different_content(y_task, field_id, new_value):
    """
    Helper function to determine if a field needs to be updated based on it being empty or not.
    
    :param task_yandex: The task object from Yandex.
    :param field_id: The field ID to check in the Yandex task.
    :param new_value: The new value that might need to be applied.
    :return: The new value if it needs to be updated, None otherwise.
    """
    if y_task.get(field_id) and new_value != y_task.get(field_id):
        return new_value
    return None

async def update_task_if_needed(y_task, g_task_id, initiator_name, initiator_department, description, y_start_date,
                                y_assignee, followers, nd, y_step, g_contractor_id, y_date_created_in_g):
    """
    Check which fields need to be updated and call edit_task if any updates are necessary.
    """
    y_task_id = y_task.get('key')
    
    if initiator_department:
        initiator_department = utils.normalize_department_name(initiator_department)
    # Custom followers handler
    edit_followers = None
    if len(y_task.get('followers', ())) < len(followers):
        edit_followers = followers
    current_y_assignee = y_task.get('assignee', {}).get('id')
    # Custom assignee handler
    edit_assignee = None
    if y_step not in [0, 1, 2] and current_y_assignee != y_assignee:
        edit_assignee = y_assignee

    gandiva = GANDIVA_TASK_URL + g_task_id
    edit_fields = {
        'edit_initiator_name': edit_field_if_empty(y_task, YA_FIELD_ID_INITIATOR, initiator_name),
        'edit_initiator_department': edit_field_if_empty(y_task, YA_FIELD_ID_INITIATOR_DEPARTMENT, initiator_department),
        'edit_description': edit_field_if_different_content(y_task, 'description', description) if description else None,
        'edit_yandex_start_date': edit_field_if_empty(y_task, 'start', y_start_date),
        'edit_assignee_yandex_id': edit_assignee,
        'edit_analyst': None,
        'edit_followers': edit_followers,
        'edit_gandiva_task_id': edit_field_if_empty(y_task, YA_FIELD_ID_GANDIVA_TASK_ID, g_task_id),
        'edit_gandiva': edit_field_if_empty(y_task, YA_FIELD_ID_GANDIVA, gandiva),
        'edit_nd': edit_field_if_empty(y_task, YA_FIELD_ID_ND, nd),
        'y_date_created_in_g': edit_field_if_empty(y_task, YA_FIELD_ID_DATE_CREATED_IN_G, y_date_created_in_g)
    }

    # Check for analyst by department
    if initiator_department:
        db_session = db.get_db_session()
        analyst = db.get_user_id_by_department(session=db_session, department_name=initiator_department)
        if analyst:
            cur_y_analyst = y_task.get(YA_FIELD_ID_ANALYST, {}).get('id')
            if utils.EXCEL_UPDATED_IN_YANDEX_DISK:
                # Check if different content
                if cur_y_analyst != analyst:
                    edit_fields['edit_analyst'] = analyst
            else:
                # Check if empty or waiting for analyst
                if not cur_y_analyst and g_contractor_id in [None, gandiva_api.WAITING_ANALYST_ID]:
                    edit_fields['edit_analyst'] = analyst
            if y_step in [0, 1, 2] and current_y_assignee != analyst:
                edit_fields['edit_assignee_yandex_id'] = analyst


    if not any(edit_fields.values()):
        logging.debug(f"Task {g_task_id} is already up-to-date.")
        return False
    
    response_json = await edit_task(
        y_task_id           =y_task_id,
        initiator_name      =edit_fields['edit_initiator_name'],
        initiator_department=edit_fields['edit_initiator_department'],
        description         =edit_fields['edit_description'],
        start               =edit_fields['edit_yandex_start_date'],
        analyst             =edit_fields['edit_analyst'],
        y_assignee_id       =edit_fields['edit_assignee_yandex_id'],
        followers           =edit_fields['edit_followers'],
        g_task_id           =edit_fields['edit_gandiva_task_id'],
        gandiva             =edit_fields['edit_gandiva'],
        nd                  =edit_fields['edit_nd'],
        date_created_in_g   =edit_fields['y_date_created_in_g'])
    
    if not response_json:
        return False
    
    return True

def create_y_tasks_dict(y_tasks, use_summaries):
    """
    Creates a dictionary to map Yandex task unique IDs to their corresponding task objects.
    If `use_summaries` is True, it maps based on task IDs scraped from summaries.
    If `use_summaries` is False, it maps based on YA_FIELD_ID_GANDIVA_TASK_ID.

    :param y_tasks: List of Yandex task objects.
    :param use_summaries: Whether to use task summaries to map task IDs.
    :return: Dictionary with task IDs as keys and Yandex task objects as values.
    """
    y_tasks_dict = {}

    if not use_summaries:
        y_tasks_dict = {yt[YA_FIELD_ID_GANDIVA_TASK_ID]: yt for yt in y_tasks if yt.get(YA_FIELD_ID_GANDIVA_TASK_ID)}
        return y_tasks_dict
        # Map based on task IDs extracted from summaries
    
    for task in y_tasks:
        summary = task.get('summary', '')
        
        match = re.match(r'^(\d+)', summary)
        if match:
            task_id = match.group(1)  # Get the matched task ID
            y_tasks_dict[task_id] = task  # Map task_id to task object

    return y_tasks_dict

async def edit_tasks(g_tasks, y_tasks, to_get_followers, use_summaries=False, edit_descriptions=True):
    """
    Edits tasks in Yandex Tracker based on the data from Gandiva.
    Only updates tasks that are outdated.
    """
    logging.info(f"Editing tasks...")
    
    # Create a dictionary to map Yandex task unique IDs to their corresponding task objects
    y_tasks_dict = create_y_tasks_dict(y_tasks=y_tasks, use_summaries=use_summaries)
    
    # Initialize a counter for edited tasks
    edited_task_count = 0

    for g_task in g_tasks:
        g_task_id         = str(g_task['Id'])
        
        # Find the corresponding Yandex task using the dictionary
        y_task = y_tasks_dict.get(g_task_id)
        if not y_task: continue

        fields = await g_to_y_fields(g_task, edit=True, to_get_followers=to_get_followers)
        initiator_name          = fields.get('initiator_name')
        initiator_department    = fields.get('initiator_department')
        description             = fields.get('description')
        y_start_date            = fields.get('yandex_start_date')
        y_assignee_id           = fields.get('assignee_id_yandex')
        followers               = fields.get('followers')
        nd                      = fields.get('nd')
        y_date_created_in_g     = fields.get('date_created_in_g')
        g_status                = g_task['Status']
        y_status                = utils.g_to_y_status(g_status)
        y_step                  = utils.y_status_to_step(y_status)
        g_contractor        = g_task.get('Contractor')
        g_contractor_id     = None
        if g_contractor:
            g_contractor_id = g_contractor.get('Id')
        if edit_descriptions:
            description = await description_with_attachments(g_task_id, description)
        else:
            description = None

        if await update_task_if_needed(y_task, g_task_id, initiator_name, initiator_department, description, y_start_date,
                                       y_assignee_id, followers, nd, y_step, g_contractor_id, y_date_created_in_g):
            edited_task_count += 1
            y_task_id = y_task['key']
            db_session = db.get_db_session()
            db.add_or_update_task(session=db_session, g_task_id=g_task_id, y_task_id=y_task_id)

    # Log the total number of edited tasks
    logging.info(f"Total tasks edited: {edited_task_count}")

async def description_with_attachments(g_task_id, description):
    g_task_detailed     = await gandiva_api.get_task(g_task_id)
    g_attachments       = g_task_detailed.get('Attachments')
    attachments_text    = utils.format_attachments(g_attachments)
    new_description     = description + "\n\n" + attachments_text
    return new_description


async def edit_task(y_task_id, summary=None, description=None,y_assignee_id=None, task_type=None, priority=None,
                    parent=None, sprint=None, followers=None,initiator_name=None, initiator_department=None,
                    analyst=None, start=None, g_task_id=None, gandiva=None, nd=None, date_created_in_g=None):

    # Convert the task ID to a string
    y_task_id = str(y_task_id)

    # Prepare the request body
    body = {}
    # Fetch task from the database
    
    # Add fields to the request body if provided
    if summary: body["summary"] = summary
    if description: body["description"] = description
    if y_assignee_id: body["assignee"] = y_assignee_id
    if task_type: body["type"] = task_type
    if priority: body["priority"] = priority
    if parent: body["parent"] = parent
    if sprint: body["sprint"] = sprint
    if followers: body["followers"] = {"set": followers}
    if initiator_name: body[YA_FIELD_ID_INITIATOR] = initiator_name
    if initiator_department:body[YA_FIELD_ID_INITIATOR_DEPARTMENT] = initiator_department
    if nd: body[YA_FIELD_ID_ND] = nd
    if date_created_in_g: body[YA_FIELD_ID_DATE_CREATED_IN_G] = date_created_in_g
    if analyst: body[YA_FIELD_ID_ANALYST] = analyst
    if start: body["start"] = start
    if g_task_id:
        body[YA_FIELD_ID_GANDIVA_TASK_ID] = g_task_id
    elif g_task_id == '':
        body[YA_FIELD_ID_GANDIVA_TASK_ID] = None

    if gandiva: body[YA_FIELD_ID_GANDIVA] = gandiva


    # Convert the body to a JSON string
    body_json = json.dumps(body)

    # Construct the URL for updating the task
    url = f"{HOST}/v2/issues/{y_task_id}"

    # Use the helper function to make the PATCH request
    response = await make_http_request('PATCH', url, headers=HEADERS, body=body_json)
    if not response:
        logging.error(f"Failed editing task {y_task_id}")
        return None

    logging.info(f"Successfully edited task {y_task_id}")
    return response

async def move_task_status(y_task_id, transition_id,
                        comment="", resolution="fixed"):
    """ [Deprecated]. Used for solo task status move. """
    # Convert the task ID to a string
    y_task_id = str(y_task_id)

    # Set a default comment if not provided
    if not comment:
        comment = f"Task status has been moved to {transition_id} for {y_task_id}."

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
    url = f"{HOST}/v2/issues/{y_task_id}/transitions/{transition_id}/_execute"
    
    # Use the helper function to make the POST request
    return await make_http_request('POST', url, headers=HEADERS, body=body_json)

async def move_tasks_status(y_tasks, new_status):
    """Moves tasks to new status."""
    url = f"{HOST}/v2/bulkchange/_transition"
    transition = utils.y_status_to_y_transition(new_status)
    task_keys = utils.extract_task_keys(y_tasks)
    body = {
        "transition": transition,
        "issues": task_keys
    }

    response =  await make_http_request('POST', url, headers=HEADERS, body=json.dumps(body))

    # Return the response JSON or None if the request failed
    if not response:
        logging.error(f"Tasks were not moved to new status {new_status}")
        return None

    logging.info(f"Tasks were moved to new status {new_status}: {len(task_keys)}")
    return response

async def move_status_task_groups(grouped_y_tasks: dict):
    """Processes grouped Yandex tasks by calling move_group_of_tasks_status for each group asynchronously."""
    tasks = []
    
    for new_status, tasks_list in grouped_y_tasks.items():
        # Create an asyncio task for each group
        tasks.append(move_tasks_status(tasks_list, new_status))
    
    # Run all tasks concurrently and wait for their completion
    results = await asyncio.gather(*tasks)
    
    return results

async def batch_move_tasks_status(g_tasks, y_tasks, to_filter: bool = True):
    grouped_y_tasks = group_tasks_by_status(g_tasks=g_tasks, y_tasks=y_tasks, to_filter=to_filter)
    await move_status_task_groups(grouped_y_tasks)
    logging.info("Task statuses are up-to-date!")

async def batch_edit_tasks(values: dict, task_ids: list):
    """Bulk update tasks in Yandex Tracker."""

    # Prepare the request body
    body = {
        "issues": task_ids,
        "values": values
    }

    url = f"{HOST}/v2/bulkchange/_update"

    response = await make_http_request("POST", url, headers=HEADERS, body=json.dumps(body))

    if not response:
        logging.error("Failed to perform bulk change.")
        return None

    logging.info("Bulk change successful")
    return response

def filter_tasks_with_gandiva_task_id(y_tasks):
    """Filters and returns only the tasks that contain the YA_FIELD_ID_GANDIVA_TASK_ID key."""
    return [task for task in y_tasks if YA_FIELD_ID_GANDIVA_TASK_ID in task]

def filter_tasks_with_unique(y_tasks):
    """Filters and returns only the tasks that contain the 'unique' key."""
    return [task for task in y_tasks if 'unique' in task]

async def add_existing_tracker_tasks_to_db():
    res = await get_tasks()
    res = filter_tasks_with_gandiva_task_id(res)
    db_session = db.get_db_session()
    db.add_tasks(session=db_session, y_tasks=res)

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
    response = await make_http_request('POST', url, headers=HEADERS, body=body_json)
    if not response:
        logging.error(f"Failed to create new sprint.")
        return
    
    logging.info(f"Sprint '{name}' [{start_date} - {end_date}] was successfully created.")

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

async def get_page_of_comments(y_task_id: str, per_page: int = 100, after_id: str = None) -> tuple:
    """
    Fetches comments for the given Yandex task with pagination support.
    
    :param yandex_task_id: ID of the Yandex task
    :param per_page: Number of comments to fetch per page
    :param after_id: The ID of the last comment from the previous page (if any)
    :return: A tuple of (comments, next_page_url)
    """
    url = f"{HOST}/v2/issues/{y_task_id}/comments?perPage={per_page}"
    if after_id:
        url += f"&id={after_id}"
    
    response = await make_http_request('GET', url, headers=HEADERS)

    return response

async def get_comments(y_task_id: str, per_page: int = 100) -> list:
    """
    Retrieves all comments for the given Yandex task, handling pagination.
    
    :param yandex_task_id: ID of the Yandex task
    :param per_page: Number of comments to fetch per page
    :return: A list of all comments
    """
    all_comments = []
    after_id = None

    while True:
        comments = await get_page_of_comments(y_task_id, per_page=per_page, after_id=after_id)
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

async def add_comment(y_task_id: str, comment: str, g_comment_id: str = None, author_name: str = None):
    url = f"{HOST}/v2/issues/{y_task_id}/comments"
    
    comment = format_g_comment_to_y(comment, g_comment_id, author_name)

    body = {
        "text": comment
    }

    body_json = json.dumps(body)
    response = await make_http_request("POST", url=url, headers=HEADERS, body=body_json)

    if not response:
        logging.error(f"Comment was not added to task {y_task_id}.")
        return None

    logging.debug(f"Comment was successfully added to task {y_task_id}!")
    return response

def format_g_comment_to_y(comment, g_comment_id, author_name):
    if g_comment_id and author_name:
        comment = f"[{g_comment_id}] {author_name}:\n{comment}"
        return comment
    return comment

async def edit_comment(y_task_id: str, comment: str, g_comment_id: str = None, y_comment_id: str = None, author_name: str = None):
    url = f"{HOST}/v2/issues/{y_task_id}/comments/{y_comment_id}"
    
    if g_comment_id and author_name:
        comment = f"[{g_comment_id}] {author_name}:\n{comment}"

    body = {
        "text": comment
    }

    response = await make_http_request("PATCH", url=url, headers=HEADERS, body=json.dumps(body))

    if not response:
        logging.error(f"Comment was not edited in task {y_task_id}.")
        return None

    logging.debug(f"Comment was successfully edited in task {y_task_id}!")
    return response

async def remove_followers_in_tasks(y_task_ids: list[str], followers: list[str]):
    url = f"{HOST}/v2/bulkchange/_update"
    body = {
        "issues": y_task_ids,
        "values": {"followers": {"remove": followers}}}
    
    response = await make_http_request('POST', url, headers=HEADERS, body=json.dumps(body))

    return response

def group_tasks_by_status(g_tasks: list, y_tasks: list, to_filter: bool = True) -> dict:
    """
    Groups tasks by their current Gandiva status, with an option to filter tasks where 
    the Gandiva task has transitioned to a new status compared to the Yandex task.
    
    Parameters:
    - gandiva_tasks (list): List of tasks from Gandiva.
    - yandex_tasks (list): List of tasks from Yandex.
    - filter (bool): If True, only tasks where Gandiva status > Yandex status are grouped. 
                     If False, all tasks are grouped by their Gandiva status.
    
    Returns:
    - dict: Dictionary grouping tasks by their Gandiva status.
    """
    grouped_tasks = {}

    for g_task in g_tasks:
        # Step 1: Get the Gandiva task ID and status
        g_task_id = g_task['Id']
        g_status = g_task['Status']

        # Step 2: Get the Yandex task based on Gandiva task ID
        y_task = next((task for task in y_tasks if task.get(YA_FIELD_ID_GANDIVA_TASK_ID) == str(g_task_id)), None)
        if not y_task:
            continue

        # Step 3: Get the current status of the Yandex task
        y_status = y_task.get('status').get('key')

        # Step 4: Convert statuses using helper functions
        current_g_status = utils.g_to_y_status(g_status)
        current_g_status_step = utils.y_status_to_step(current_g_status)
        current_y_status_step = utils.y_status_to_step(y_status)
        users_to_skip = [1001, 878, 1020, 852, 410]
        waiting_analyst_id = gandiva_api.WAITING_ANALYST_ID
        if current_g_status_step == 3 and g_task.get('Contractor') and g_task.get('Contractor').get('Id'):
            contr = g_task.get('Contractor').get('Id')
            db_session = db.get_db_session()
            if contr == waiting_analyst_id or not db.get_user_by_gandiva_id(session=db_session, g_user_id=contr) and contr not in users_to_skip:
                current_g_status_step = 2
                current_g_status = utils.y_step_to_status(current_g_status_step)
        # Step 5: Apply filtering logic if required
        if to_filter:
            if current_g_status_step <= current_y_status_step: continue

        # Step 6: Group tasks by Gandiva status
        if current_g_status not in grouped_tasks:
            grouped_tasks[current_g_status] = []
        grouped_tasks[current_g_status].append(y_task)

    return grouped_tasks

async def handle_in_work_but_waiting_for_analyst(g_tasks: list[dict], y_tasks: list[dict]):
    needed_status = 'twowaitingfortheanalyst'
    cursed_g_tasks = []
    cursed_y_tasks = []
    users_to_skip = [1001, 878, 1020, 852, 410]
    for g_task in g_tasks:
        g_status    = g_task['Status']
        y_status    = utils.g_to_y_status(g_status)
        y_step      = utils.y_status_to_step(y_status)
        if y_step == 3 and g_task.get('Contractor') and g_task.get('Contractor').get('Id'):
            contr = g_task.get('Contractor').get('Id')
            db_session = db.get_db_session()
            if contr == gandiva_api.WAITING_ANALYST_ID or not db.get_user_by_gandiva_id(session=db_session, g_user_id=contr) and contr not in users_to_skip:
                cursed_g_tasks.append(g_task)

    cursed_g_task_ids = utils.extract_task_ids(cursed_g_tasks)
    cursed_g_task_ids = list(map(str, cursed_g_task_ids))
    if not cursed_g_tasks:
        logging.info(f"No anomalies found!")
        return True
    for y_task in y_tasks:
        y_task_id = y_task.get('key')
        g_task_id = y_task.get(YA_FIELD_ID_GANDIVA_TASK_ID)
        if g_task_id not in cursed_g_task_ids: continue
        if not g_task_id:
            logging.debug(f"No {g_task_id} in {y_task_id}...")
            continue
        if y_task.get('status', {}).get('key') == needed_status:
            logging.debug(f'Task {y_task_id} already in status {needed_status}')
            continue
        cursed_y_tasks.append(y_task)
    if not cursed_y_tasks:
        logging.info(f"No anomalies found!")
        return True
    res = await move_tasks_status(cursed_y_tasks, needed_status)
    if res:
        logging.info(f"Handled {len(cursed_y_tasks)} anomaly tasks!")
    if len(cursed_y_tasks) == 0:
        return False
    return True

async def handle_cancelled_tasks_still_have_g_task_ids(queue):
    count = 0
    y_tasks = await get_tasks(query=f'DEV."Номер заявки в Гандиве": notEmpty() (Resolution: notEmpty() or "Status Type": cancelled, done) Queue: {queue} Status: onecancelled "Sort by": Updated DESC')
    if not y_tasks:
        logging.info(f"No anomalies found!")
        return True
    for y_task in y_tasks:
        y_task_id = y_task.get('key')
        res = await edit_task(y_task_id, g_task_id='')
        if not res:
            logging.error(f"Failed removing GandivaTaskId from cancelled task {y_task_id}.")
        logging.info(f"Removed GandivaTaskId from cancelled task {y_task_id}!")
        count += 1

    if not count:
        return False

    logging.info(f"Handled {count} cancelled tasks (removed g_task_id from them)!")
    return True

async def download_file_from_yandex_disk(path):
    # GTY/GTY_table.xlsx
    url = f'https://cloud-api.yandex.net/v1/disk/resources/download?path={path}'
    file_dict = await make_http_request(method='GET', url=url, headers=HEADERS_DISK)
    if not file_dict and not file_dict.get('href'):
        logging.error(f"Failed getting file url from Yandex Disk.")
        return
    file_url = file_dict.get('href')
    file = await make_http_request(method='GET', url=file_url, headers=HEADERS_DISK)
    return file