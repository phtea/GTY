import csv
from bs4 import BeautifulSoup, MarkupResemblesLocatorWarning
import re
import logging
from logging.handlers import RotatingFileHandler
import aiohttp
import datetime
import warnings
import markdown
import configparser
from typing import Callable


import pandas as pd
from pandas import DataFrame
import io

warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning)

GANDIVA_HOST = "https://gandiva.s-stroy.ru"
YANDEX_HOST     = "https://tracker.yandex.ru"
EXCEL_UPDATED_IN_YANDEX_DISK = False

def setup_logging():
    # 10 Megabytes
    max_size = 1024*1024*10
    backupCount=3
    
    # Create a rotating file handler
    handler = RotatingFileHandler("gty.log", maxBytes=max_size, backupCount=backupCount)

    # Set the logging format
    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s (%(module)s: %(funcName)s)")
    handler.setFormatter(formatter)

    # Add handler to the root logger
    logging.getLogger().addHandler(handler)

    # Set logging level
    logging.getLogger().setLevel(logging.INFO)



def y_status_to_y_transition(transition: str):
    return transition + "Meta"

def extract_task_keys(tasks: list) -> list:
    """Extracts the 'key' values from a list of tasks."""
    return [task.get('key') for task in tasks if 'key' in task]

def extract_task_ids(tasks: list) -> list:
    """Extracts the 'Id' values from a list of tasks."""
    return [task.get('Id') for task in tasks if 'Id' in task]

def y_status_to_step(y_status: str) -> int:
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
    step:int|None = status_to_step.get(y_status)
    if not step:
        return 0
    return step

def y_step_to_status(y_step):
    step_to_status = {
        0: "open",
        1: "onenew",
        2: "twowaitingfortheanalyst",
        3: "threewritingtechnicalspecific",
        4: "fourinformationrequired",
        5: "fiveapprovaloftheTOR",
        6: "sixwaitingforthedeveloper",
        7: "sevenprogramming",
        8: "eightreadyfortesting",
        9: "testingbyananalystQA",
        10: "correctionoftherevision",
        11: "writinginstructions",
        12: "testingbytheinitiatorinthetest",
        13: "readyforrelease",
        14: "acceptanceintheworkbase",
        15: "oneclosed",
        16: "onecancelled"
    }
    return step_to_status.get(y_step)

def get_y_transition_from_g_status(g_status):
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
    return status_to_transition.get(g_status)

def g_to_y_status(g_status: int) -> str:
    g_status_to_y_status = {
        3: "onenew",
        4: "fourinformationrequired",
        5: "onecancelled",
        6: "threewritingtechnicalspecific",
        7: "onecancelled",
        8: "acceptanceintheworkbase",
        9: "oneclosed",
        10: "twowaitingfortheanalyst",
        11: "twowaitingfortheanalyst",
        12: "twowaitingfortheanalyst",
        13: "threewritingtechnicalspecific"
    }
    y_status = g_status_to_y_status.get(g_status)
    if not y_status:
        return ""
    return y_status

def extract_text_from_html(html):
    soup = BeautifulSoup(html, "html.parser")
    return soup.get_text()

def get_all_unique_initiators(g_tasks):
    """Extract unique Initiator IDs"""
    unique_initiator_ids = {task['Initiator']['Id'] for task in g_tasks}

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

def markdown_to_html_old(markdown_text):
    """
    Converts markdown-style links and images back to HTML format.
    Handles links, images, and newlines properly.
    Prepend YANDEX_HOST if the link doesn't have a host (i.e., it's a relative URL).
    """

    # Remove leading '!' for image links (to just show the link instead of embedding the image)
    markdown_text = markdown_text.replace("![" , "[")  # Remove '!' from the markdown images

    # Convert markdown links back to HTML <a> tags
    markdown_text = re.sub(
        r'\[([^\]]+)\]\(([^)]+)\)', 
        lambda match: f'<a href="{match.group(2) if re.match(r"^https?://", match.group(2)) else YANDEX_HOST + match.group(2)}">{match.group(1)}</a>',
        markdown_text
    )
    
    
    # Convert markdown image links back to HTML <img> tags (with a placeholder for alt text)
    markdown_text = re.sub(
        r'\[Картинка (\d+)\]\(([^)]+)\)',
        lambda match: f'<img src="{match.group(2) if re.match(r"^https?://", match.group(2)) else YANDEX_HOST + match.group(2)}" alt="Картинка {match.group(1)}" />',
        markdown_text
    )

    # Convert newlines to <br> or wrap paragraphs in <p> tags
    paragraphs = markdown_text.split("\n")
    html_text = "".join([f"<p>{p}</p>" if p else "<br />" for p in paragraphs])

    # Use BeautifulSoup to clean and format the final HTML output
    soup = BeautifulSoup(html_text, "html.parser")
    
    return str(soup)

def markdown_to_html(markdown_text):
    """
    Converts markdown-style links and images back to HTML format.
    Handles links, images, and newlines properly.
    Prepend YANDEX_HOST if the link doesn't have a host (i.e., it's a relative URL).
    """
    html_text = markdown.markdown(markdown_text)
    html_text = prepend_host_to_relative_links(html_text, YANDEX_HOST)
    # html_text = wrap_raw_urls_in_html(html_text, YANDEX_HOST)
    html_text = convert_raw_urls_to_html(html_text)
    
    return html_text

def prepend_host_to_relative_links(html_text, host):
    """
    Adds host to relative URLs in <a> and <img> tags in the given HTML content.
    """
    soup = BeautifulSoup(html_text, "html.parser")

    # Find all <a> tags
    for a in soup.find_all('a'):
        href = a.get('href', '')
        starts_with_https = re.match(r'^https?://', href)
        if not starts_with_https:
            a['href'] = host + href

    # Find all <img> tags
    for img in soup.find_all('img'):
        src = img.get('src', '')
        starts_with_https = re.match(r'^https?://', src)
        if not starts_with_https:
            img['src'] = host + src

    return str(soup)

def convert_raw_urls_to_html(text):
    url_pattern = r'(?<!["\'])\bhttps?://[^\s<>"]+'
    
    def wrap_url_in_link(match):
        raw_url = match.group(0)
        return f'<a href="{raw_url}">{raw_url}</a>'
    
    return re.sub(url_pattern, wrap_url_in_link, text)

def convert_gandiva_to_yandex_date(gandiva_date: str) -> str:
    parsed_date = datetime.datetime.fromisoformat(gandiva_date)
    return parsed_date.strftime('%Y-%m-%d')

async def perform_http_request(
        method: str, url: str, headers: dict|None = None,
        body: str|None = None, session: aiohttp.ClientSession|None = None):
    
    allowed_methods = {'GET', 'POST', 'PUT', 'PATCH', 'DELETE'}
    if method.upper() not in allowed_methods:
        logging.error(f"Invalid HTTP method: {method}")
        return None

    try:
        if session is None:
            async with aiohttp.ClientSession() as session:
                return await _make_request(session, method, url, headers, body)
        else:
            return await _make_request(session, method, url, headers, body)

    except Exception as e:
        logging.error(f"Exception during request to {url}: {str(e)}")
        return None

async def _make_request(
        session: aiohttp.ClientSession, method: str,
        url: str, headers: dict|None, body: str|None):
    """
    Helper function to make an HTTP request using a given session.
    Handles both JSON/text responses and binary file downloads.
    """
    async with session.request(method, url, headers=headers, data=body) as response:
        status_code = response.status
        content_type = response.headers.get('Content-Type', '')

        successful_statuses = {200, 201}
        if status_code in successful_statuses:
            response_data_handler = data_handler_factory(content_type)
            return await response_data_handler(response)

        if status_code != 204:
            logging.error(f"Request to {url} failed with status {status_code}: {response.reason}")
            return None
        
        return True

def data_handler_factory(content_type: str) -> Callable:
    if 'application/json' in content_type or 'text' in content_type:
        return parse_json_data
    return read_binary_data

async def parse_json_data(response: aiohttp.ClientResponse) -> str:
    try:
        return await response.json()
    except Exception as e:
        logging.error(f"Failed to parse JSON: {e}")
        return await response.text()

async def read_binary_data(response: aiohttp.ClientResponse) -> bytes:
    response_data = await response.read()
    return response_data

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
    department_analyst_mapping = {}

    with open(csv_file, mode='r', encoding='cp1251') as file:
        reader = csv.DictReader(file, delimiter=';')

        for entry in reader:
            department = entry['department']
            activity_direction = entry['НД']
            analyst_email = entry['yandex_analyst_mail']

            department_analyst_mapping[department] = {
                'НД': activity_direction,
                'yandex_analyst_mail': analyst_email
            }

    return department_analyst_mapping

def map_department_nd_to_user_id(department_analyst_mapping: dict, user_list: list) -> dict:
    department_user_mapping = {}

    email_to_uid_mapping = {user['email']: user['uid'] for user in user_list}

    for department, details in department_analyst_mapping.items():
        analyst_email = str(details['yandex_analyst_mail']).strip()
        activity_direction = str(details['НД'])
        user_id = email_to_uid_mapping.get(analyst_email)

        if user_id:
            department_user_mapping[(department.strip(), activity_direction)] = user_id
        else:
            logging.warning((
                f"No user found with email {analyst_email} for department "
                f"{department} and НД {activity_direction}"))

    return department_user_mapping

def extract_it_users(csv_file_path: str) -> dict:
    it_users_mapping = {}

    with open(csv_file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file, delimiter=';')

        for row in reader:
            yandex_email = row['yandex_mail']
            gandiva_email = row['gandiva_mail']
            it_users_mapping[yandex_email] = gandiva_email

    return it_users_mapping

def extract_it_users_from_excel(excel_sheets: dict[str, DataFrame]) -> dict:
    it_users_mapping = {}

    if 'it_users' not in excel_sheets:
        raise ValueError("Sheet 'it_users' not found in the Excel file.")
    
    dataframe = excel_sheets['it_users']

    if not {'yandex_mail', 'gandiva_mail'}.issubset(dataframe.columns):
        raise ValueError(
            "Required columns ('yandex_mail', 'gandiva_mail') are missing in the sheet.")

    for _, row in dataframe.iterrows():
        yandex_email = row['yandex_mail']
        gandiva_email = row['gandiva_mail']
        it_users_mapping[yandex_email] = gandiva_email

    return it_users_mapping

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

def map_it_uids_to_g_ids(it_uids: dict, g_users: dict) -> dict:
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
    summary = summary.strip()
    match = re.match(r'^(\d+)', summary)
    
    if match:
        return match.group(1)  # Return the matched task ID
    else:
        return None


def extract_task_ids_from_summaries(y_tasks):
    """
    Extracts task IDs from the 'summary' field of each task in y_tasks and detects duplicates.

    :param y_tasks: List of task dictionaries containing 'summary' and 'key' fields.
    :return: A dictionary with task_id as keys and ya_task_key as values.
    """
    task_info_dict = {}
    seen_task_ids = set()  # To track task IDs we've already encountered

    for task in y_tasks:
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

def extract_gandiva_task_id_from_task(task, field_id_gandiva_task_id):
    """
    Extracts the Gandiva task ID from a single task dictionary.

    :param task: A task dictionary containing the GandivaTaskId.
    :return: The Gandiva task ID or None if no task ID is found.
    """
    return task.get(field_id_gandiva_task_id)


def extract_task_ids_from_gandiva_task_id(y_tasks, field_id_gandiva_task_id):
    """
    Extracts task IDs from the 'GandivaTaskId' field of each task in y_tasks and detects duplicates.

    :param y_tasks: List of task dictionaries containing 'GandivaTaskId' and 'key' fields.
    :return: A dictionary with gandiva_task_id as keys and ya_task_key as values.
    """
    task_info_dict = {}
    seen_gandiva_task_ids = set()  # To track task IDs we've already encountered

    for task in y_tasks:
        gandiva_task_id = extract_gandiva_task_id_from_task(task, field_id_gandiva_task_id)
        ya_task_key = task.get('key')  # Extract the 'key' field

        if gandiva_task_id:
            if gandiva_task_id in seen_gandiva_task_ids:
                logging.warning(f"Duplicate Gandiva task ID found: {gandiva_task_id}")
            else:
                task_info_dict[gandiva_task_id] = ya_task_key
                seen_gandiva_task_ids.add(gandiva_task_id)

    return task_info_dict

def find_unmatched_tasks(y_tasks, extracted_task_ids):
    """
    Finds tasks whose IDs could not be extracted from the 'summary' field.

    :param y_tasks: List of task dictionaries containing 'summary' fields.
    :param extracted_task_ids: Set of task IDs that were successfully extracted.
    :return: List of tasks (summaries) that were not found during extraction.
    """
    unmatched_tasks = []

    for task in y_tasks:
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

def extract_observers_from_detailed_task(detailed_task: dict) -> list[str]:
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

def g_addressee_exists(addressees, addressee_id):
    if not addressee_id:
        return None
    for addressee in addressees:
        if addressee['User']['Id'] == addressee_id:
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

def id_in_summonees_exists(y_author_id: str, y_summonees: list) -> bool:
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
    Extracts the existing comments and their answers from Gandiva and returns two mappings:
    - existing_g_comments: A mapping of y_comment_id to g_comment_id.
    - comment_texts_in_gandiva: A mapping of y_comment_id to g_text.
    
    :param g_comments: List of Gandiva comments.
    :return: Two dictionaries - existing_g_comments and comment_texts_in_gandiva.
    """
    
    existing_g_comments = {}
    comment_texts_in_gandiva = {}

    def process_comment(g_comment):
        """Helper function to process a comment and extract relevant data."""
        g_text = g_comment['Text']
        match = re.match(r'\[(\d+)\]', g_text)  # Match [y_comment_id] format
        
        if match:
            y_comment_id = match.group(1)
            g_comment_id = g_comment.get('Id')  # Assuming 'Id' is the g_comment_id in the g_comment object
            existing_g_comments[y_comment_id] = g_comment_id  # Map y_comment_id to g_comment_id
            comment_texts_in_gandiva[y_comment_id] = g_text  # Map y_comment_id to g_text

        # Process any answers recursively
        for answer in g_comment.get('Answers', []):
            process_comment(answer)

    # Process each top-level comment
    for g_comment in g_comments:
        process_comment(g_comment)

    return existing_g_comments, comment_texts_in_gandiva

def is_g_comment_author_this(g_comment, author_id):
    return True if g_comment.get('Author') and g_comment.get('Author').get('Id') and str(g_comment.get('Author').get('Id')) == str(author_id) else False

def format_attachments(g_attachments):
    """
    Formats a list of attachments into a string with numbered links.

    :param g_attachments: A list of dictionaries containing attachment details.
    :return: A formatted string listing the attachments with links.
    """
    if not g_attachments:
        return ""

    attachments_text = "Вложения:\n"
    base_url = "https://gandiva.s-stroy.ru/Resources/Attachment/"

    for i, attachment in enumerate(g_attachments, start=1):
        # Extract the attachment's name and GUID to create the link
        attachment_name = attachment.get('Name', 'Unnamed attachment')
        guid = attachment.get('Guid')
        
        # Construct the attachment line with a number, name, and link
        if guid:
            attachment_link = f"{base_url}{guid}"
            attachments_text += f"{i}. [{attachment_name}]({attachment_link})\n"
        else:
            attachments_text += f"{i}. {attachment_name} (no link available)\n"

    return attachments_text

def get_next_year_datetime():
    # Get the current year
    current_year = datetime.datetime.now().year
    
    # Calculate the next year
    next_year = current_year + 1
    
    # Return the next year's January 1st in the desired format
    next_year_datetime = f"{next_year}-01-01T00:00:00+03:00"
    
    return next_year_datetime


def read_excel_from_bytes(excel_bytes) -> dict[str, DataFrame]|None:
    """
    Reads an Excel file from bytes and returns a pandas DataFrame.
    
    :param excel_bytes: The byte content of an Excel file.
    :return: A dictionary of DataFrames, where the keys are sheet names and values are DataFrames.
    """
    try:
        # Create a BytesIO object from the byte data
        excel_io = io.BytesIO(excel_bytes)
        
        # Read the Excel file using pandas
        # pd.read_excel can handle both single-sheet and multi-sheet Excel files.
        xls = pd.read_excel(excel_io, sheet_name=None)  # Read all sheets into a dictionary of DataFrames
        
        return xls  # Returns a dictionary where keys are sheet names, and values are DataFrames
    except Exception as e:
        print(f"Error reading Excel from bytes: {e}")
        return None

def extract_department_analysts_from_excel(excel_sheets: dict) -> dict:
    """
    Extracts departments, their corresponding activity direction (НД), and analyst emails from the given Excel sheet.

    Parameters:
    - excel_sheets (dict): Dictionary of sheets read from the Excel file (with sheet names as keys).

    Returns:
    - dict: A dictionary where the keys are department names and the values are dictionaries containing 'НД' and 'yandex_analyst_mail'.
    """
    department_analyst_dict = {}

    # Access the 'department_analyst_nd' sheet
    if 'department_analyst_nd' in excel_sheets:
        df = excel_sheets['department_analyst_nd']

        # Ensure the columns are present
        if {'department', 'НД', 'yandex_analyst_mail'}.issubset(df.columns):
            # Iterate through the rows to populate the dictionary
            for _, row in df.iterrows():
                department = row['department']
                nd = row['НД']  # Extract the НД column
                analyst_email = row['yandex_analyst_mail']

                # Store the НД and analyst email corresponding to the department
                department_analyst_dict[department] = {
                    'НД': nd,
                    'yandex_analyst_mail': analyst_email
                }
        else:
            raise ValueError("Required columns ('department', 'НД', 'yandex_analyst_mail') are missing in the sheet.")
    else:
        raise ValueError("Sheet 'department_analyst_nd' not found in the Excel file.")
    
    return department_analyst_dict

def normalize_department_name(department_name: str):
        # Step 1: Replace NBSP (non-breaking space) with a regular space
    department_name = department_name.replace('\u00A0', ' ')

    # Step 2: Collapse multiple spaces into a single space
    department_name = re.sub(r'\s+', ' ', department_name)

    # Step 3: Trim leading/trailing spaces
    department_name = department_name.strip()
    
    return department_name

def load_config(path: str):
    config = configparser.ConfigParser()
    config.read(path)
    return config

config = load_config('config.ini')