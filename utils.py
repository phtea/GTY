import io
import re
import csv
import logging
import warnings
import datetime
import configparser
from typing import Callable, Any, Union, Awaitable
from configparser import ConfigParser
from logging.handlers import RotatingFileHandler

# Third-Party Libraries
import pandas as pd
import aiohttp
import markdown
from bs4 import BeautifulSoup, MarkupResemblesLocatorWarning

ResponseType = str | dict[str, Any] | bytes | bool | None


warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning)

GANDIVA_HOST = "https://gandiva.s-stroy.ru"
YANDEX_HOST = "https://tracker.yandex.ru"
EXCEL_UPDATED_IN_YANDEX_DISK = False


def setup_logging(
        logging_level: str
) -> None:
    """Sets up logging with a rotating file handler."""
    handler = create_rotating_file_handler()
    handler.setFormatter(create_log_formatter())

    logger = logging.getLogger()
    logger.addHandler(handler)

    set_logging_level(logging_level, logger)


def set_logging_level(
        logging_level: str,
        logger: logging.Logger
) -> None:
    """
    Sets the logging level of the given logger.

    The logging level can be one of:

        * debug
        * info
        * warning
        * error
        * critical

    The level is case-insensitive.

    :param logging_level: The desired logging level
    :type logging_level: str

    :param logger: The logger for which to set the level
    :type logger: :py:class:`logging.Logger`
    """
    logging_level = logging_level.lower()
    if logging_level == "debug":
        logger.setLevel(logging.DEBUG)
    elif logging_level == "info":
        logger.setLevel(logging.INFO)
    elif logging_level == "warning":
        logger.setLevel(logging.WARNING)
    elif logging_level == "error":
        logger.setLevel(logging.ERROR)
    elif logging_level == "critical":
        logger.setLevel(logging.CRITICAL)


def create_rotating_file_handler() -> RotatingFileHandler:
    """Creates a rotating file handler with defined max size and backup count."""
    max_log_size = 10 * 1024 * 1024  # 10 Megabytes
    return RotatingFileHandler("gty.log", maxBytes=max_log_size, backupCount=3)


def create_log_formatter() -> logging.Formatter:
    """Creates a log formatter with a specified format."""
    log_format = "%(asctime)s - %(levelname)s - %(message)s (%(module)s: %(funcName)s)"
    return logging.Formatter(log_format)


def y_status_to_y_transition(
        transition: str
) -> str:
    return transition + "Meta"


def extract_task_keys(
        tasks: list[dict[str, str]]
) -> list[str] | None:
    task_keys = [str(task.get('key')) for task in tasks if 'key' in task]
    if not task_keys:
        return None
    return task_keys


def extract_task_ids(
        tasks: list[dict[str, str]]
) -> list[str] | None:
    """Extracts the 'ID' values from a list of tasks."""
    task_ids = [str(task.get('Id')) for task in tasks if 'Id' in task]
    if not task_ids:
        return None
    return task_ids


def y_status_to_step(
        y_status: str
) -> int | None:
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
    return status_to_step.get(y_status)


def y_step_to_status(
        y_step: str
) -> str | None:
    step_to_status = {
        "0": "open",
        "1": "onenew",
        "2": "twowaitingfortheanalyst",
        "3": "threewritingtechnicalspecific",
        "4": "fourinformationrequired",
        "5": "fiveapprovaloftheTOR",
        "6": "sixwaitingforthedeveloper",
        "7": "sevenprogramming",
        "8": "eightreadyfortesting",
        "9": "testingbyananalystQA",
        "10": "correctionoftherevision",
        "11": "writinginstructions",
        "12": "testingbytheinitiatorinthetest",
        "13": "readyforrelease",
        "14": "acceptanceintheworkbase",
        "15": "oneclosed",
        "16": "onecancelled"
    }
    return step_to_status.get(y_step)


def get_y_transition_from_g_status(
        g_status: str
) -> str | None:
    status_to_transition = {
        "3": "onenewMeta",
        "4": "fourinformationrequiredMeta",
        "5": "onecancelledMeta",
        "6": "threewritingtechnicalspecificMeta",
        "7": "onecancelledMeta",
        "8": "acceptanceintheworkbaseMeta",
        "9": "oneclosedMeta",
        "10": "twowaitingfortheanalystMeta",
        "11": "twowaitingfortheanalystMeta",
        "12": "twowaitingfortheanalystMeta",
        "13": "threewritingtechnicalspecificMeta"
    }
    return status_to_transition.get(g_status)


def g_to_y_status(
        g_status: int | str
) -> str | None:
    """Converts a Gandiva status to a corresponding Yandex status.

    Logs the error (when key is missing)"""

    status_mapping = {
        "3": "onenew",
        "4": "fourinformationrequired",
        "5": "onecancelled",
        "6": "threewritingtechnicalspecific",
        "7": "onecancelled",
        "8": "acceptanceintheworkbase",
        "9": "oneclosed",
        "10": "twowaitingfortheanalyst",
        "11": "twowaitingfortheanalyst",
        "12": "twowaitingfortheanalyst",
        "13": "threewritingtechnicalspecific",
    }
    key = str(g_status)
    y_status = status_mapping.get(key)

    if not y_status:
        logging.error("Conversion from Gandiva to Yandex status failed.")

    return y_status


def extract_text_from_html(
        html: str
) -> str:
    """Extracts plain text from an HTML document.

    :param html: The HTML document to extract text from
    :return: The extracted text
    """
    soup = BeautifulSoup(html, "html.parser")
    return soup.get_text()


def html_to_yandex_format(
        html: str
) -> str:
    """Converts HTML content to a Yandex-friendly text format."""
    soup = BeautifulSoup(html, "html.parser")

    replace_breaks_with_newlines(soup)
    convert_links_to_markdown(soup)
    replace_images_with_links(soup)

    add_newlines_after_blocks(soup)

    return soup.get_text().strip()


def replace_breaks_with_newlines(
        soup: BeautifulSoup
) -> None:
    """Replaces <br> tags with newlines in the soup object."""
    for br in soup.find_all("br"):
        br.replace_with("\n")


def convert_links_to_markdown(
        soup: BeautifulSoup
) -> None:
    """Converts <a> tags to markdown-style links."""
    for a in soup.find_all("a"):
        href = a.get('href', '')
        href = prepend_host_if_relative(href)
        cleaned_text = clean_link_text(a.get_text(strip=True))
        a.replace_with(f"[{cleaned_text}]({href})")


def prepend_host_if_relative(
        url: str
) -> str:
    """Prepends GANDIVA_HOST to the URL if it is a relative link."""
    return f"{GANDIVA_HOST}{url}" if not re.match(r'^https?://', url) else url


def clean_link_text(
        text: str
) -> str:
    """Removes square brackets and strips spaces from the link text."""
    return re.sub(r'[\[\]]', '', text).strip()


def replace_images_with_links(
        soup: BeautifulSoup
) -> None:
    """Replaces <img> tags with markdown-style image links."""
    img_count = 1
    for img in soup.find_all("img"):
        src = img.get('src', '')
        src = prepend_host_if_relative(src)
        img.replace_with(f"[Картинка {img_count}]({src})\n")
        img_count += 1


def add_newlines_after_blocks(
        soup: BeautifulSoup
) -> None:
    """Adds newlines after block-level elements like <p>."""
    for p in soup.find_all("p"):
        p.insert_after("\n")


def markdown_to_html(
        markdown_text: str
) -> str:
    """Converts markdown text to HTML format."""
    html_text = markdown.markdown(markdown_text)
    html_text = prepend_host_to_relative_links(html_text, YANDEX_HOST)
    html_text = convert_raw_urls_to_html(html_text)
    return html_text


def prepend_host_to_relative_links(
        html_text: str,
        host: str
) -> str:
    """Prepend host to relative URLs in <a> and <img> tags."""
    soup = BeautifulSoup(html_text, "html.parser")

    for tag in soup.find_all(['a', 'img']):
        href = tag.get('href', '') if tag.name == 'a' else tag.get('src', '')
        if not re.match(r'^https?://', href):
            tag[tag.name == 'a' and 'href' or 'src'] = host + href

    return str(soup)


def convert_raw_urls_to_html(
        text: str
) -> str:
    """Convert raw URLs to HTML links."""
    url_pattern = r'(?<!["\'])\bhttps?://[^\s<>"]+'
    return re.sub(url_pattern, wrap_url_in_link, text)


def wrap_url_in_link(
        match: re.Match[str]
) -> str:
    """Wrap raw URL in an HTML link."""
    raw_url = match.group(0)
    return f'<a href="{raw_url}">{raw_url}</a>'


def convert_gandiva_to_yandex_date(
        gandiva_date: str
) -> str:
    parsed_date = datetime.datetime.fromisoformat(gandiva_date)
    return parsed_date.strftime('%Y-%m-%d')


async def perform_http_request(
        method: str, url: str,
        headers: dict[str, str] | None = None,
        body: str | None = None,
        session: aiohttp.ClientSession | None = None
) -> ResponseType:

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
        session: aiohttp.ClientSession,
        method: str,
        url: str,
        headers: dict[str, str] | None = None,
        body: str | None = None
) -> ResponseType:
    """Makes an HTTP request and returns the response data or status."""

    async with session.request(method, url, headers=headers, data=body) as response:
        status_code = response.status
        content_type = response.headers.get('Content-Type', '')

        if status_code in {200, 201}:
            get_data = data_handler_factory(content_type)
            data = await get_data(response)
            return data

        if status_code == 204:
            return True

        logging.error(
            f"Request to {url} failed with status {status_code}: {response.reason}")
        return None


def data_handler_factory(
        content_type: str
) -> Callable[..., Awaitable[Union[str, dict[str, Any], bytes]]]:
    if 'application/json' in content_type or 'text' in content_type:
        return parse_json_data
    return read_binary_data


async def parse_json_data(
        response: aiohttp.ClientResponse
) -> Union[str, dict[str, Any]]:
    try:
        json_data: dict[str, Any] = await response.json()
        return json_data
    except Exception as e:
        logging.error(f"Failed to parse JSON: {e}")
        text = await response.text()
        return text


async def read_binary_data(
        response: aiohttp.ClientResponse
) -> bytes:
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


def extract_department_analysts(
        csv_file: str
) -> dict[str, dict[str, str]]:
    """Parses a CSV file to extract department names, activity directions, and analyst emails."""

    department_analyst_mapping: dict[str, dict[str, str]] = {}

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


def map_department_nd_to_user_id(
    department_analyst_mapping: dict[str, dict[str, str]],
    user_list: list[dict[str, str]]
) -> dict[tuple[str, str], str]:
    """Maps department names and activity directions to Yandex user IDs."""

    department_user_mapping: dict[tuple[str, str], str] = {}

    email_to_uid_mapping = {user['email']: user['uid'] for user in user_list}

    for department, details in department_analyst_mapping.items():
        analyst_email = str(details['yandex_analyst_mail']).strip()
        activity_direction = str(details['НД'])
        user_id = email_to_uid_mapping.get(analyst_email)

        if user_id:
            department_user_mapping[(
                department.strip(), activity_direction)] = user_id
        else:
            logging.warning((
                f"No user found with email {analyst_email} for department "
                f"{department} and НД {activity_direction}"))

    return department_user_mapping


def extract_it_users(
        csv_file_path: str
) -> dict[str, str]:
    """
    Reads a CSV file at the given path and extracts a mapping
    of Yandex email addresses to Gandiva email addresses.

    :param csv_file_path: The path to the CSV file
    :return: A dictionary mapping Yandex email addresses to Gandiva email addresses
    """
    it_users_mapping: dict[str, str] = {}

    with open(csv_file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file, delimiter=';')

        for row in reader:
            yandex_email = row['yandex_mail']
            gandiva_email = row['gandiva_mail']
            it_users_mapping[yandex_email] = gandiva_email

    return it_users_mapping


def extract_it_users_from_excel(
    excel_sheets: dict[str, pd.DataFrame]
) -> dict[str, str]:
    """
    Reads the 'it_users' sheet from the Excel file and extracts a mapping
    of Yandex email addresses to Gandiva email addresses.

    :param excel_sheets: Dictionary where keys are sheet names and values are DataFrames.
    :return: Dictionary mapping Yandex email addresses to Gandiva email addresses.
    """
    it_users_mapping: dict[str, str] = {}

    if 'it_users' not in excel_sheets:
        raise ValueError("Sheet 'it_users' not found in the Excel file.")

    dataframe = excel_sheets['it_users']

    if not {'yandex_mail', 'gandiva_mail'}.issubset(dataframe.columns):
        raise ValueError(
            "Required columns ('yandex_mail', 'gandiva_mail') are missing in the sheet.")

    for _, row in dataframe.iterrows():  # type: ignore
        row: pd.Series[str]
        yandex_email = row['yandex_mail']
        gandiva_email = row['gandiva_mail']
        it_users_mapping[yandex_email] = gandiva_email

    return it_users_mapping


async def map_emails_to_ids(
    it_users_dict: dict[str, str],
    y_users: list[dict[str, str]]
) -> dict[str, str]:
    """
    Maps Yandex email addresses to Gandiva email addresses using the provided
    dictionary. If a Yandex user is not found for a given email, a warning is
    logged and the mapping is skipped.

    :param it_users_dict: Dictionary where keys are Yandex email addresses and
        values are Gandiva email addresses.
    :param ya_users: list of Yandex user dictionaries.
    :return: Dictionary where keys are Yandex user IDs and values are Gandiva
        email addresses.
    """
    mapped_dict: dict[str, str] = {}
    for yandex_email, gandiva_mail in it_users_dict.items():
        y_user = next(
            (u for u in y_users if u.get('email') == yandex_email),
            None
        )
        if not y_user:
            logging.warning(f"No Yandex user found for email {yandex_email}")
            continue

        y_user_id = y_user.get('uid')
        if not y_user_id:
            logging.warning(
                f"No Yandex user ID found for email {yandex_email}")
            continue

        mapped_dict[y_user_id] = gandiva_mail

    return mapped_dict


def extract_unique_gandiva_users(
        g_tasks: list[dict[str, Any]]
) -> dict[str, str]:
    """
    Extracts unique email and ID pairs from Gandiva tasks for both Initiator and Contractor.

    :param g_tasks: list of Gandiva tasks, each containing dictionaries with
    'Initiator' and 'Contractor' keys.
    :return: Dictionary with unique email as the key and ID as the value.
    """
    unique_users: dict[str, str] = {}

    # Iterate over each task
    for task in g_tasks:
        # Extract Initiator details if present
        initiator = task.get('Initiator')
        if initiator:
            initiator_email = initiator.get('Login')
            initiator_id = initiator.get('Id')
            if initiator_email and initiator_id:
                unique_users[initiator_email] = str(initiator_id)

        # Extract Contractor details if present
        contractor = task.get('Contractor')
        if contractor:
            contractor_email = contractor.get('Login')
            contractor_id = contractor.get('Id')
            if contractor_email and contractor_id:
                unique_users[contractor_email] = contractor_id

    return unique_users


def map_it_uids_to_g_ids(
        it_uids: dict[str, str],
        g_users: dict[str, str]
) -> dict[str, str]:
    """
    Maps Yandex user IDs to Gandiva user IDs based on email matching.

    :param it_uids: Dictionary where keys are Yandex user IDs and values are emails.
    :type it_uids: dict[int, str]
    :param g_users: Dictionary where keys are emails and values are Gandiva user IDs.
    :type g_users: dict[str, int]
    :return: Dictionary where keys are Yandex user IDs and values are Gandiva user IDs.
    :rtype: dict[int, int]
    """
    mapped_ids: dict[str, str] = {}

    for yandex_uid, email in it_uids.items():
        if email in g_users:
            gandiva_id = g_users[email]
            mapped_ids[yandex_uid] = gandiva_id

    return mapped_ids


def extract_task_id_from_summary(
        summary: str
) -> str | None:
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


def extract_task_ids_from_summaries(
        y_tasks: list[dict[str, str]],
) -> dict[str, str]:
    """
    Extracts task IDs from the 'summary' field of each task in y_tasks and detects duplicates.

    :param y_tasks: list of task dictionaries containing 'summary' and 'key' fields.
    :return: A dictionary with task_id as keys and ya_task_key as values.
    """
    task_info_dict: dict[str, str] = {}
    seen_task_ids: set[str] = set()

    for task in y_tasks:
        summary: str = task.get('summary', '')

        ya_task_key = task.get('key')
        if not ya_task_key:
            continue

        task_id: str | None = extract_task_id_from_summary(summary)

        if task_id:
            if task_id in seen_task_ids:
                logging.warning(f"Duplicate task ID found: {task_id}")
            else:
                task_info_dict[task_id] = ya_task_key
                seen_task_ids.add(task_id)

    return task_info_dict


def extract_gandiva_task_id_from_task(
        task: dict[str, str],
        field_id_gandiva_task_id: str
) -> str | None:
    """
    Extracts the Gandiva task ID from a single task dictionary.

    :param task: A task dictionary containing the GandivaTaskId.
    :param field_id_gandiva_task_id: The field in Yandex, which contains gandiva task id.
    :return: The Gandiva task ID or None if no task ID is found.
    """
    return task.get(field_id_gandiva_task_id)


def extract_task_ids_from_gandiva_task_id(
    y_tasks: list[dict[str, str]],
    field_id_gandiva_task_id: str,
) -> dict[str, str]:
    """
    Extracts task IDs from the 'GandivaTaskId'
    field of each task in y_tasks and detects duplicates.

    :param y_tasks: list of task dictionaries containing 'GandivaTaskId' and 'key' fields.
    :param field_id_gandiva_task_id: The field in Yandex, which contains gandiva task id.
    :return: A dictionary with gandiva_task_id as keys and ya_task_key as values.
    """
    task_info_dict: dict[str, str] = {}
    seen_gandiva_task_ids: set[str] = set()

    for task in y_tasks:
        gandiva_task_id = extract_gandiva_task_id_from_task(
            task, field_id_gandiva_task_id)
        ya_task_key: str | None = task.get('key')

        if not (gandiva_task_id and ya_task_key):
            logging.debug(f"Invalid task: {task}")
            continue
        if gandiva_task_id in seen_gandiva_task_ids:
            logging.warning(
                f"Duplicate Gandiva task ID found: {gandiva_task_id}")
        else:
            task_info_dict[gandiva_task_id] = ya_task_key
            seen_gandiva_task_ids.add(gandiva_task_id)

    return task_info_dict


def find_unmatched_tasks(
    y_tasks: list[dict[str, str | None]],
    extracted_task_ids: set[str]
) -> list[dict[str, str | None]]:
    """
    Finds tasks whose IDs could not be extracted from the 'summary' field.

    :param y_tasks: list of task dictionaries containing 'summary' fields.
    :param extracted_task_ids: Set of task IDs that were successfully extracted.
    :return: list of tasks (summaries) that were not found during extraction.
    """
    unmatched_tasks: list[dict[str, str | None]] = []

    for task in y_tasks:
        summary = task.get('summary')
        if not summary:
            logging.warning(f"Invalid task: {task}")
            continue

        match = re.match(r'^(\d+)', summary)
        if not match:
            unmatched_tasks.append(task)
            continue
        task_id = match.group(1)

        if task_id not in extracted_task_ids:
            unmatched_tasks.append(task)

    return unmatched_tasks


def extract_observers_from_detailed_task(
    detailed_task: dict[str, Any]
) -> list[str]:
    """
    Extracts observer IDs from a detailed task.

    :param detailed_task: The detailed task object (a dictionary)
    which contains an "Observers" field.
    :return: A list of observer IDs. If no observers are found, returns an empty list.
    """
    observers: list[dict[str, Any]] = detailed_task.get(
        "Observers", [])

    # Extract observer IDs if they exist
    observer_ids = [str(observer.get('Id'))
                    for observer in observers
                    if 'Id' in observer]

    return observer_ids


def task_exists_in_list(
        g_tasks: list[dict[str, Any]],
        task_id: str
) -> bool:
    """
    Check if a task with the specified ID exists in the list of tasks.

    :param g_tasks: list of task dictionaries, each containing an 'ID' field.
    :param task_id: The task ID to search for.
    :return: True if the task exists, False otherwise.
    """
    return any(task.get('Id') == task_id for task in g_tasks)


def g_addressee_exists(
    addressees: list[dict[str, dict[str, str]]],
    addressee_id: str
) -> bool:
    """
    Check if an addressee with the given ID exists in the list of addressees.

    :param addressees: List of addressee dictionaries, each containing a "User" field
                      with an "Id" field.
    :param addressee_id: The ID of the addressee to search for.
    :return: True if the addressee exists, False otherwise.
    """
    if not addressee_id:
        return False
    return any(addressee['User']['Id'] == addressee_id for addressee in addressees)


def remove_mentions(
        text: str
) -> str:
    """
    Removes @xxxxx mentions from the given text.

    :param text: The text from which to remove mentions.
    :return: The text with all mentions removed.
    """
    return re.sub(r'@\S+', '', text).strip()


def id_in_summonees_exists(
    y_author_id: str,
    y_summonees: list[dict[str, str | None]]
) -> bool:
    """
    Check if y_author_id is present in the 'id' field of summonees.

    :param y_author_id: The author ID to check for.
    :param y_summonees: A list of dictionaries representing the summonees.
    :return: True if y_author_id is found in summonees, False otherwise.
    """
    for summonee in y_summonees:
        if summonee.get("id") == y_author_id:
            return True
    return False


def extract_existing_comments_from_gandiva(
        g_comments: list[dict[str, Any]]
) -> tuple[dict[str, str], dict[str, str]]:
    existing_g_comments: dict[str, str] = {}
    comment_texts_in_gandiva: dict[str, str] = {}

    def process_comment(
            comment: dict[str, Any]
    ) -> None:
        g_text = comment["Text"]
        if match := re.match(r"\[(\d+)\]", g_text):
            y_comment_id = match.group(1)
            g_comment_id = comment.get("Id")

            if isinstance(y_comment_id, str) and isinstance(g_comment_id, str):
                existing_g_comments[y_comment_id] = g_comment_id
                comment_texts_in_gandiva[y_comment_id] = g_text

        for answer in comment.get("Answers", []):
            process_comment(answer)

    for g_comment in g_comments:
        process_comment(g_comment)

    return existing_g_comments, comment_texts_in_gandiva


def is_g_comment_author_this(
        g_comment: dict[str, Any],
        author_id: str
) -> bool:
    author: dict[str, Any] | None = g_comment.get('Author')
    return bool(author
                and (id := author.get('Id'))
                and id == author_id)


def format_attachments(
        g_attachments: list[dict[str, Any]]
) -> str:
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


def get_next_year_datetime() -> str:
    """
    Returns the next year's January 1st in the format: "YYYY-MM-DDTHH:MM:SS+03:00"
    """
    current_year: int = datetime.datetime.now().year
    next_year: int = current_year + 1
    next_year_datetime: str = f"{next_year}-01-01T00:00:00+03:00"

    return next_year_datetime


def read_excel_from_bytes(
        excel_bytes: bytes
) -> dict[str, pd.DataFrame] | None:
    try:
        excel_io = io.BytesIO(excel_bytes)
        return pd.read_excel(excel_io, sheet_name=None)  # type: ignore
    except Exception as e:
        print(f"Error reading Excel from bytes: {e}")
        return None


def extract_department_analysts_from_excel(
    excel_sheets: dict[str, pd.DataFrame]
) -> dict[str, dict[str, str]]:
    """Extract departments, their corresponding activity direction (НД),
    and analyst emails from the given Excel sheet."""

    department_analyst_dict: dict[str, dict[str, str]] = {}

    if 'department_analyst_nd' not in excel_sheets:
        raise ValueError(
            "Sheet 'department_analyst_nd' not found in the Excel file.")

    df = excel_sheets['department_analyst_nd']

    if not {'department', 'НД', 'yandex_analyst_mail'}.issubset(df.columns):
        raise ValueError(
            "Required columns ('department', 'НД', 'yandex_analyst_mail') "
            "are missing in the sheet.")

    for _, row in df.iterrows():  # type: ignore
        row: pd.Series[str]
        department = row['department']
        nd = row['НД']
        analyst_email = row['yandex_analyst_mail']

        department_analyst_dict[department] = {
            'НД': nd,
            'yandex_analyst_mail': analyst_email
        }

    return department_analyst_dict


def load_config(
        path: str
) -> configparser.ConfigParser:
    """
    Loads and returns a configuration from the specified file path.

    :param path: The path to the configuration file.
    :return: A ConfigParser object containing the configuration.
    """
    config = configparser.ConfigParser()
    config.read(path)
    return config


ConfigObject = load_config('config.ini')


def get_config() -> ConfigParser:
    return ConfigObject
