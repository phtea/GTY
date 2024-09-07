import csv
from bs4 import BeautifulSoup
import re

GANDIVA_HOST = "https://gandiva.s-stroy.ru"

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

def get_transition_from_gandiva_status(gandiva_status):
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
    return status_to_transition.get(gandiva_status)

def get_ya_status_step(ya_status):
    status_to_step = {
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
        4: "fourinformationrequiredMeta",
        5: "onecancelledMeta",
        6: "threewritingtechnicalspecificMeta",
        8: "acceptanceintheworkbaseMeta",
        10: "twowaitingfortheanalystMeta",
        11: "twowaitingfortheanalystMeta",
        12: "twowaitingfortheanalystMeta",
        13: "threewritingtechnicalspecificMeta"
    }
    return status_to_transition.get(gandiva_status)

def get_ya_status_step(ya_status):
    status_to_step = {
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

if __name__ == '__main__':
    html_comment = '<p>Доработка выполнена в базе Srvr=&quot;192.168.50.226&quot;;Ref=&quot;erp_106&quot;.<br />Техническое описание изменений:<br />1. Расширение САКСЭС<br />1.1. Документ.ЗаказКлиента.ФормаДокумента(2603, 3) - В процедуру &quot;СК_РассчитатьДоставку&quot; добавлены изменения<br />1.2. Документ.ЗаказКлиента.ФормаДокумента(11055, 1) - Добавлена новая процедура &quot;СК_ЗаписьДокументаБезЕгоПроведения&quot;<br />1.3. Документ.ЗаказКлиента.МодульОбъекта(205, 3) - Закомментирован вызов процедуры &quot;СК_ПроверитьЗаполнениеСегментаПартнера&quot;<br />1.4. Документ.ЗаказКлиента.МодульОбъекта(822, 1) - Добавлен вызов процедуры &quot;СК_ПроверитьЗаполнениеСегментаПартнера&quot;</p>'
    html_comment = '<p><a href=\"/Resources/Attachment/22893ba9-efc9-4971-8471-0cd137f13ad7\">[ Файл: 110634 Разбор ФТ.docx]</a></p><p><a href=\"/Resources/Attachment/f56d10c1-6941-4224-86b7-8a404d0fc058\">[ Файл: 110634 ТЗ Запись нового заказа клиента перед расчетом доставки калькулятором.docx]</a></p>'
    comment = html_to_yandex_format(html_comment)
    print(comment)