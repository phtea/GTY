import csv
from bs4 import BeautifulSoup, MarkupResemblesLocatorWarning
import re
import logging
from logging.handlers import RotatingFileHandler
import aiohttp
import datetime
import warnings
warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning)

GANDIVA_HOST = "https://gandiva.s-stroy.ru"

def setup_logging():
    # 10 Megabytes
    max_size = 1024*1024*10
    backupCount=3
    
    # Create a rotating file handler
    handler = RotatingFileHandler("gty.log", maxBytes=max_size, backupCount=backupCount)

    # Set the logging format
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s (%(module)s: %(funcName)s)")
    handler.setFormatter(formatter)

    # Add handler to the root logger
    logging.getLogger().addHandler(handler)

    # Set logging level
    logging.getLogger().setLevel(logging.INFO)

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
        y_task = next((task for task in y_tasks if task.get(yapi.YA_FIELD_ID_GANDIVA_TASK_ID) == str(g_task_id)), None)
        if not y_task:
            continue

        # Step 3: Get the current status of the Yandex task
        y_status = y_task.get('status').get('key')

        # Step 4: Convert statuses using helper functions
        current_g_status = get_transition_from_gandiva_status(g_status)[:-4]
        current_g_status_step = get_ya_status_step(current_g_status)
        current_ya_status_step = get_ya_status_step(y_status)

        # Step 5: Apply filtering logic if required
        if to_filter:
            if current_g_status_step <= current_ya_status_step:
                continue

        # Step 6: Group tasks by Gandiva status
        if current_g_status not in grouped_tasks:
            grouped_tasks[current_g_status] = []
        grouped_tasks[current_g_status].append(y_task)

    return grouped_tasks

def get_yandex_transition_from_status(transition: str):
    return transition + "Meta"

def extract_task_keys(tasks: list) -> list:
    """Extracts the 'key' values from a list of tasks."""
    return [task.get('key') for task in tasks if 'key' in task]

def extract_task_ids(tasks: list) -> list:
    """Extracts the 'Id' values from a list of tasks."""
    return [task.get('Id') for task in tasks if 'Id' in task]

def get_ya_status_step(ya_status):
    status_to_step = {
        "open": 0,
        "onenew": 1,
        "twowaitingfortheanalyst": 2,
        "threewritingtechnicalspecific": 3,
        "fourinformationrequired": 4,
        "fiveapprovaloftheTOR": 5,
        "sixwaitingforthedeveloper": 6,
        "sevenprogramming": 7,
        "eightreadyfortesting": 8,
        "testingbyananalystQA": 9,
        "correctionoftherevision": 10,
        "writinginstructions": 11,
        "testingbytheinitiatorinthetest": 12,
        "readyforrelease": 13,
        "acceptanceintheworkbase": 14,
        "oneclosed": 15,
        "onecancelled": 16
    }
    return status_to_step.get(ya_status)

def get_transition_from_gandiva_status(gandiva_status):
    status_to_transition = {
        3: "onenewMeta",
        4: "fourinformationrequiredMeta",
        5: "onecancelledMeta",
        6: "threewritingtechnicalspecificMeta",
        7: "onecancelledMeta",
        8: "acceptanceintheworkbaseMeta",
        9: "oneclosedMeta",
        10: "twowaitingfortheanalystMeta",
        11: "twowaitingfortheanalystMeta",
        12: "twowaitingfortheanalystMeta",
        13: "threewritingtechnicalspecificMeta"
    }
    return status_to_transition.get(gandiva_status)


def extract_text_from_html(html):
    soup = BeautifulSoup(html, "html.parser")
    return soup.get_text()

def get_all_unique_initiators(gandiva_tasks):
    """Extract unique Initiator IDs"""
    unique_initiator_ids = {task['Initiator']['Id'] for task in gandiva_tasks}

    # Convert to a list (if needed)
    unique_initiator_ids_list = list(unique_initiator_ids)

    return unique_initiator_ids_list


def save_list_to_csv(data_list, filename):
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        for item in data_list:
            writer.writerow([item])  # Write each item in a new row

def html_to_yandex_format(html):
    """
    Converts HTML content to plain text, handling newlines for <br> and block-level elements.
    Processes links to the format [filename](url), 
    and images to the format [Картинка {number}](url), 
    replacing images in place and adding a newline after each image.
    """
    soup = BeautifulSoup(html, "html.parser")

    # Replace <br> with newlines
    for br in soup.find_all("br"):
        br.replace_with("\n")
    
    # Handle links, converting them to markdown-style [filename](url)
    for a in soup.find_all("a"):
        href = a.get('href', '')
        # Prepend GANDIVA_HOST if the link doesn't have a host (i.e., it's a relative URL)
        if not re.match(r'^https?://', href):
            href = f"{GANDIVA_HOST}{href}"

        text = a.get_text(strip=True)
        # Remove square brackets and strip spaces from filename
        cleaned_text = re.sub(r'[\[\]]', '', text).strip()
        a.replace_with(f"[{cleaned_text}]({href})")

    # Handle images, replacing them with clickable links formatted as 'Картинка {number}'
    img_count = 1
    for img in soup.find_all("img"):
        src = img.get('src', '')
        # Prepend GANDIVA_HOST if the image link doesn't have a host (i.e., it's a relative URL)
        if not re.match(r'^https?://', src):
            src = f"{GANDIVA_HOST}{src}"

        # Replace the <img> tag with the formatted link and add a newline
        img.replace_with(f"[Картинка {img_count}]({src})\n")
        img_count += 1

    # Add newlines after block-level elements like <p>
    for p in soup.find_all("p"):
        p.insert_after("\n")

    # Final text extraction
    return soup.get_text().strip()

def gandiva_to_yandex_date(gandiva_date: str) -> str:
    """
    Converts a Gandiva date (with time and timezone) to Yandex date (only date part).
    Args:
        gandiva_date (str): The date string in Gandiva format (e.g., '2025-08-01T00:00:00+03:00').
    Returns:
        str: The date in Yandex format (e.g., '2025-08-01').
    """
    # Parse the Gandiva date string into a datetime object
    date_obj = datetime.datetime.fromisoformat(gandiva_date)
    
    # Convert the datetime object back to a string in 'YYYY-MM-DD' format
    return date_obj.strftime('%Y-%m-%d')

async def make_http_request(method, url, headers=None, body=None, session=None):
    """
    Generalized function to handle HTTP GET/POST requests.
    
    Args:
    - method (str): HTTP method (GET, POST, PUT, PATCH, DELETE).
    - url (str): The URL for the request.
    - headers (dict, optional): Request headers.
    - body (dict, optional): Request body.
    - session (aiohttp.ClientSession, optional): Existing aiohttp session.
    
    Returns:
    - dict or None: JSON response if successful, None otherwise.
    """
    
    valid_methods = ['GET', 'POST', 'PUT', 'PATCH', 'DELETE']
    if method.upper() not in valid_methods:
        logging.error(f"Invalid HTTP method: {method}")
        return None

    try:
        # Create a new session if one is not provided
        if session is None:
            async with aiohttp.ClientSession() as session:
                return await _make_request(session, method, url, headers, body)
        else:
            return await _make_request(session, method, url, headers, body)

    except Exception as e:
        logging.error(f"Exception during request to {url}: {str(e)}")
        return None

async def _make_request(session, method, url, headers, body):
    """
    Helper function to make an HTTP request using a given session.
    """
    async with session.request(method, url, headers=headers, data=body) as response:
        status_code = response.status
        response_text = await response.text()  # For logging purposes
        
        if status_code in [200, 201]:
            return await response.json()  # Return the JSON response
        else:
            logging.error(f"Request to {url} failed with status {status_code}: {response_text}")
            return None

# Manual mapping of month names in Russian
MONTHS_RUSSIAN = {
    1: "января",
    2: "февраля",
    3: "марта",
    4: "апреля",
    5: "мая",
    6: "июня",
    7: "июля",
    8: "августа",
    9: "сентября",
    10: "октября",
    11: "ноября",
    12: "декабря"
}

def extract_department_analysts(csv_file: str) -> dict:
    """
    Extracts departments, their corresponding activity direction (НД), and analyst emails from the given CSV file.

    Parameters:
    - csv_file (str): Path to the CSV file containing department, НД, and analyst information.

    Returns:
    - dict: A dictionary where the keys are department names and the values are dictionaries containing 'НД' and 'yandex_analyst_mail'.
    """
    department_analyst_dict = {}

    # Try a different encoding, such as ISO-8859-1 or cp1251
    with open(csv_file, mode='r', encoding='cp1251') as file:
        reader = csv.DictReader(file, delimiter=';')

        for row in reader:
            department = row['department']
            nd = row['НД']  # Extract the НД column
            analyst_email = row['yandex_analyst_mail']

            # Store the НД and analyst email corresponding to the department
            department_analyst_dict[department] = {
                'НД': nd,
                'yandex_analyst_mail': analyst_email
            }

    return department_analyst_dict

def map_department_nd_to_user_id(department_analyst_dict: dict, users: list) -> dict:
    """
    Maps each department and НД (activity direction) to the corresponding user ID based on matching email addresses.

    Parameters:
    - department_analyst_dict (dict): A dictionary where the keys are department names and the values are dictionaries 
      containing 'НД' and 'yandex_analyst_mail'.
    - users (list): A list of user objects, where each object contains an 'email' and 'uid' key.

    Returns:
    - dict: A dictionary where the keys are tuples of (department, НД) and the values are the corresponding user IDs (uids).
    """
    department_user_mapping = {}

    # Create a dictionary to quickly lookup user 'uid' by their email
    email_to_uid = {user['email']: user['uid'] for user in users}

    # Iterate over each department and НД, and find the matching uid for the analyst's email
    for department, details in department_analyst_dict.items():
        email = details['yandex_analyst_mail']
        nd = details['НД']  # Extract the НД value
        uid = email_to_uid.get(email)  # Find the user 'uid' by email

        if uid:
            department_user_mapping[(department, nd)] = uid
        else:
            logging.warning(f"No user found with email {email} for department {department} and НД {nd}")

    return department_user_mapping

def extract_it_users(csv_file_path: str) -> dict:
    """
    Extracts data from it_users.csv file and returns a dictionary where
    yandex_mail is the key and gandiva_mail is the value.

    :param csv_file_path: Path to the CSV file.
    :return: A dictionary with yandex_mail as keys and gandiva_mail as values.
    """
    it_users_dict = {}

    with open(csv_file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file, delimiter=';')
        
        # Iterate over each row and map yandex_mail to gandiva_mail
        for row in reader:
            yandex_mail = row['yandex_mail']
            gandiva_mail = row['gandiva_mail']
            it_users_dict[yandex_mail] = gandiva_mail

    return it_users_dict

async def map_emails_to_ids(it_users_dict: dict, ya_users: list) -> dict:
    """
    Replace the Yandex email keys in it_users_dict with the corresponding Yandex user IDs (uid) 
    based on the ya_users list.
    
    :param it_users_dict: Dictionary where keys are Yandex emails and values are Gandiva emails.
    :param ya_users: List of Yandex users (each with 'email' and 'uid' keys).
    :return: A new dictionary with Yandex user IDs (uid) as keys and Gandiva emails as values.
    """
    # Create a new dictionary to store the mapping of user ids to Gandiva emails
    mapped_dict = {}

    # Iterate over each Yandex email in it_users_dict
    for yandex_email, gandiva_mail in it_users_dict.items():
        # Find the Yandex user that has the same email in the ya_users list
        yandex_user = next((user for user in ya_users if user.get('email') == yandex_email), None)
        
        if yandex_user:
            # Replace the Yandex email with the corresponding Yandex user ID (uid)
            yandex_uid = yandex_user.get('uid')
            mapped_dict[yandex_uid] = gandiva_mail
        else:
            # If no user is found, log it or handle the case as needed
            logging.warning(f"No Yandex user found for email {yandex_email}")

    return mapped_dict

def extract_unique_gandiva_users(g_tasks: list) -> dict:
    """
    Extracts unique email and ID pairs from Gandiva tasks for both Initiator and Contractor.
    
    :param g_tasks: List of Gandiva tasks.
    :return: Dictionary with unique email as the key and ID as the value.
    """
    unique_users = {}

    # Iterate over each task
    for task in g_tasks:
        # Extract Initiator details if present
        initiator = task.get('Initiator')
        if initiator:
            initiator_email = initiator.get('Login')
            initiator_id = initiator.get('Id')
            if initiator_email and initiator_id:
                unique_users[initiator_email] = initiator_id

        # Extract Contractor details if present
        contractor = task.get('Contractor')
        if contractor:
            contractor_email = contractor.get('Login')
            contractor_id = contractor.get('Id')
            if contractor_email and contractor_id:
                unique_users[contractor_email] = contractor_id

    return unique_users

def map_it_uids_to_gandiva_ids(it_uids: dict, g_users: dict) -> dict:
    """
    Maps Yandex user IDs to Gandiva user IDs based on email matching.
    
    :param it_uids: Dictionary where keys are Yandex user IDs and values are emails.
    :param g_users: Dictionary where keys are emails and values are Gandiva user IDs.
    :return: Dictionary where keys are Yandex user IDs and values are Gandiva user IDs.
    """
    mapped_ids = {}

    # Iterate over it_uids and match emails to g_users
    for yandex_uid, email in it_uids.items():
        if email in g_users:
            # Map the Yandex UID to the Gandiva ID
            gandiva_id = g_users[email]
            mapped_ids[yandex_uid] = gandiva_id

    return mapped_ids

def extract_task_id_from_summary(summary):
    """
    Extracts a task ID from a single summary string.

    :param summary: A string containing the task summary.
    :return: The extracted task ID, or None if no match is found.
    """
    # Use a regex to match the task ID at the start of the summary
    match = re.match(r'^(\d+)', summary)
    
    if match:
        return match.group(1)  # Return the matched task ID
    else:
        return None


def extract_task_ids_from_summaries(ya_tasks):
    """
    Extracts task IDs from the 'summary' field of each task in ya_tasks and detects duplicates.

    :param ya_tasks: List of task dictionaries containing 'summary' and 'key' fields.
    :return: A dictionary with task_id as keys and ya_task_key as values.
    """
    task_info_dict = {}
    seen_task_ids = set()  # To track task IDs we've already encountered

    for task in ya_tasks:
        summary = task.get('summary', '')
        ya_task_key = task.get('key')  # Extract the 'key' field

        # Extract the task ID from the summary
        task_id = extract_task_id_from_summary(summary)

        if task_id:
            if task_id in seen_task_ids:
                logging.warning(f"Duplicate task ID found: {task_id}")
            else:
                task_info_dict[task_id] = ya_task_key
                seen_task_ids.add(task_id)

    return task_info_dict

def extract_gandiva_task_id_from_task(task):
    """
    Extracts the Gandiva task ID from a single task dictionary.

    :param task: A task dictionary containing the GandivaTaskId.
    :return: The Gandiva task ID or None if no task ID is found.
    """
    return task.get(yapi.YA_FIELD_ID_GANDIVA_TASK_ID)


def extract_task_ids_from_gandiva_task_id(ya_tasks):
    """
    Extracts task IDs from the 'GandivaTaskId' field of each task in ya_tasks and detects duplicates.

    :param ya_tasks: List of task dictionaries containing 'GandivaTaskId' and 'key' fields.
    :return: A dictionary with gandiva_task_id as keys and ya_task_key as values.
    """
    task_info_dict = {}
    seen_gandiva_task_ids = set()  # To track task IDs we've already encountered

    for task in ya_tasks:
        gandiva_task_id = extract_gandiva_task_id_from_task(task)
        ya_task_key = task.get('key')  # Extract the 'key' field

        if gandiva_task_id:
            if gandiva_task_id in seen_gandiva_task_ids:
                logging.warning(f"Duplicate Gandiva task ID found: {gandiva_task_id}")
            else:
                task_info_dict[gandiva_task_id] = ya_task_key
                seen_gandiva_task_ids.add(gandiva_task_id)

    return task_info_dict

def find_unmatched_tasks(ya_tasks, extracted_task_ids):
    """
    Finds tasks whose IDs could not be extracted from the 'summary' field.

    :param ya_tasks: List of task dictionaries containing 'summary' fields.
    :param extracted_task_ids: Set of task IDs that were successfully extracted.
    :return: List of tasks (summaries) that were not found during extraction.
    """
    unmatched_tasks = []

    for task in ya_tasks:
        summary = task.get('summary', '')
        
        # Extract the task ID using regex
        match = re.match(r'^(\d+)', summary)
        
        if match:
            task_id = match.group(1)
            # If the extracted task ID is not in the set of extracted IDs, add it to the unmatched list
            if task_id not in extracted_task_ids:
                unmatched_tasks.append(task)
        else:
            # If no task ID was extracted, add it to the unmatched list
            unmatched_tasks.append(task)
    
    return unmatched_tasks

def extract_observers_from_detailed_task(detailed_task: dict) -> list:
    """
    Extracts observer IDs from a detailed task.

    :param detailed_task: The detailed task object (a dictionary) which contains an "Observers" field.
    :return: A list of observer IDs. If no observers are found, returns an empty list.
    """
    observers = detailed_task.get("Observers", [])
    
    # Extract observer IDs if they exist
    observer_ids = [observer.get('Id') for observer in observers if 'Id' in observer]

    return observer_ids

def task_exists_in_list(g_tasks, task_id):
    """
    Check if a task with the specified ID exists in the list of tasks.
    
    :param g_tasks: List of task dictionaries, each containing an 'Id' field.
    :param task_id: The task ID to search for.
    :return: True if the task exists, False otherwise.
    """
    for task in g_tasks:
        if task.get('Id') == task_id:
            return True
    return False

def g_addressee_exists(addressees, addressee):
    for addressee in addressees:
        if addressee['User']['Id'] == addressee:
            return True
    return False

def remove_mentions(text: str) -> str:
    """
    Removes @xxxxx mentions from the given text.

    :param text: The text from which to remove mentions.
    :return: The text with all mentions removed.
    """
    # Use regex to find and remove all occurrences of @ followed by non-space characters
    return re.sub(r'@\S+', '', text).strip()

def is_id_in_summonees(y_author_id: str, y_summonees: list) -> bool:
    """
    Check if y_author_id is present in the 'id' field of summonees.

    :param y_author_id: The author ID to check for.
    :param summonees: A list of dictionaries representing the summonees.
    :return: True if y_author_id is found in summonees, False otherwise.
    """
    for summonee in y_summonees:
        if summonee.get('id') == y_author_id:
            return True
    return False

def extract_existing_comments_from_gandiva(g_comments):
    """
    Extracts the existing comments from Gandiva and returns a mapping of g_comment_id to g_text.
    """
    existing_g_comments = {}

    for g_comment in g_comments:
        g_text = g_comment['Text']
        match = re.match(r'\[(\d+)\]', g_text)  # Match [g_comment_id] format
        if match:
            g_comment_id = match.group(1)
            existing_g_comments[g_comment_id] = g_text  # Map g_comment_id to g_text

    return existing_g_comments

def is_g_comment_author_this(g_comment, author_id):
    return True if g_comment.get('Author') and g_comment.get('Author').get('Id') and str(g_comment.get('Author').get('Id')) == str(author_id) else None

import yandex_api as yapi
import gandiva_api as gapi
import asyncio    
async def main():
    g_task  = gapi.get_task_by_id(196295)
    g_tasks = [g_task]
    pass



if __name__ == '__main__':
    asyncio.run(main())
    
