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
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)

    # Add handler to the root logger
    logging.getLogger().addHandler(handler)

    # Set logging level
    logging.getLogger().setLevel(logging.INFO)

def filter_and_group_tasks_by_new_status(gandiva_tasks: list, yandex_tasks: list) -> dict:
    """
    Filters tasks where the Gandiva task has transitioned to a new status compared to the Yandex task,
    and groups the tasks by their current Gandiva status.
    """
    grouped_tasks = {}

    for g_task in gandiva_tasks:
        # Step 1: Get the Gandiva task ID and status
        g_task_id = g_task['Id']
        g_status = g_task['Status']

        # Step 2: Get the Yandex task based on Gandiva task ID
        y_task = next((task for task in yandex_tasks if task.get('unique') == str(g_task_id)), None)
        if not y_task:
            continue

        # Step 3: Get the current status of the Yandex task
        y_status = y_task.get('status').get('key')

        # Step 4: Convert statuses using helper functions
        current_g_status = get_transition_from_gandiva_status(g_status)[:-4]
        current_g_status_step = get_ya_status_step(current_g_status)
        current_ya_status_step = get_ya_status_step(y_status)

        # Step 5: Compare the Gandiva status and Yandex status
        if current_g_status_step > current_ya_status_step:
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
        3: "fiveapprovaloftheTORMeta",
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

if __name__ == '__main__':
    html_comment = '<p>Доработка выполнена в базе Srvr=&quot;192.168.50.226&quot;;Ref=&quot;erp_106&quot;.<br />Техническое описание изменений:<br />1. Расширение САКСЭС<br />1.1. Документ.ЗаказКлиента.ФормаДокумента(2603, 3) - В процедуру &quot;СК_РассчитатьДоставку&quot; добавлены изменения<br />1.2. Документ.ЗаказКлиента.ФормаДокумента(11055, 1) - Добавлена новая процедура &quot;СК_ЗаписьДокументаБезЕгоПроведения&quot;<br />1.3. Документ.ЗаказКлиента.МодульОбъекта(205, 3) - Закомментирован вызов процедуры &quot;СК_ПроверитьЗаполнениеСегментаПартнера&quot;<br />1.4. Документ.ЗаказКлиента.МодульОбъекта(822, 1) - Добавлен вызов процедуры &quot;СК_ПроверитьЗаполнениеСегментаПартнера&quot;</p>'
    html_comment = '<p><a href=\"/Resources/Attachment/22893ba9-efc9-4971-8471-0cd137f13ad7\">[ Файл: 110634 Разбор ФТ.docx]</a></p><p><a href=\"/Resources/Attachment/f56d10c1-6941-4224-86b7-8a404d0fc058\">[ Файл: 110634 ТЗ Запись нового заказа клиента перед расчетом доставки калькулятором.docx]</a></p>'
    comment = html_to_yandex_format(html_comment)