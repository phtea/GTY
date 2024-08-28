# t.me/SuccessYaTrackerBot

import json
import logging
from urllib.parse import urlencode

import requests
from telegram import Update
from telegram.ext import (
    Application,
    CallbackContext,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

# Get sensitive data from dotenv
import os
import dotenv
DOTENV_PATH = os.path.join(os.path.dirname(__file__), '.env')
if os.path.exists(DOTENV_PATH):
    dotenv.load_dotenv(DOTENV_PATH)


# TODO: обработать незаполненные значения в .env
TG_BOT_TOKEN        = os.environ.get("TG_BOT_TOKEN")
YA_X_CLOUD_ORG_ID   = os.environ.get("YA_X_CLOUD_ORG_ID")
YA_ACCESS_TOKEN     = os.environ.get("YA_ACCESS_TOKEN")
YA_CLIENT_ID        = os.environ.get("YA_CLIENT_ID")
YA_CLIENT_SECRET    = os.environ.get("YA_CLIENT_SECRET")
YA_REFRESH_TOKEN    = os.environ.get("YA_REFRESH_TOKEN")


YA_OAUTH_TOKEN_URL  = "https://oauth.yandex.ru/token"
YA_INFO_TOKEN_URL   = "https://login.yandex.ru/info"

# Conversation states
CREDS = 1

# Define a few command handlers. These usually take the two arguments update and context.

# Conversation handlers
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Пожалуйста, введите логин, чтобы проверить статус задач:"
    )
    return CREDS


async def login(update: Update, context: CallbackContext) -> str:
    """pokidovdv"""

    login = update.message.text.strip()

    if login:
        context.user_data["login"] = login
        await update.message.reply_html(f"Логин принят: {login}.")

        return ConversationHandler.END

    else:
        await update.message.reply_text("Пользователь с таким логином не найден")
        return CREDS


async def cancel(update: Update, context: CallbackContext) -> int:
    """Cancel the conversation."""
    await update.message.reply_text("The conversation has been canceled.")
    return ConversationHandler.END


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


async def tasks(
    update: Update,
    context: CallbackContext,
    access_token: str,
) -> None:
    "Gathers user tasks using credentials from login"
    logging.info("Executing fetch_tasks function")

    if "login" not in context.user_data:
        logging.info("User login not found in context.user_data")
        await update.message.reply_text(
            f"Tasks are currently unavailable. But I have your login: {context.user_data.get('login')}!"
        )
        return

    login = context.user_data["login"]
    logging.info(f"Fetching tasks for user: {login}")

    tasks = await _fetch_tasks(access_token, login)

    # None
    if tasks is None:
        msg = "Не удалось получить задачи от Yandex.Tracker."
        logging.error(msg)
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=msg,
        )
        return

    logging.info(f"Tasks successfully fetched for user ({login}): {tasks}")

    # []
    if not tasks:
        msg = "У вас нет ни одной активной задачи"
        logging.info(msg)
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=msg,
        )
        return

    # [tasks...]
    for task in tasks:
        task_text = (
            f"Task Number: {task.get('key')}\n"
            f"Title: {task.get('summary')}\n"
            f"Description: {task.get('description')}\n"
            f"Status: {task.get('statusType', {}).get('value')}"
        )

        await context.bot.send_message(chat_id=update.effective_chat.id, text=task_text)
        # TODO: добавить пагинацию (постраничный вывод) задач


async def _fetch_tasks(access_token, login):
    payload = json.dumps(
        {
            "filter": {
                "assignee": login,
            },
        }
    )
    logging.info(f"_fetch_tasks payload: {payload}")

    response = requests.post(
        "https://api.tracker.yandex.net/v2/issues/_search?expand=transitions",
        headers={
            "Host": "api.tracker.yandex.net",
            "Authorization": f"OAuth {access_token}",
            "Content-Type": "application/json",
            "X-Cloud-Org-Id": YA_X_CLOUD_ORG_ID,
        },
        data=payload,
    )
    logging.info(f"_fetch_tasks code: {response.status_code}")
    logging.info(f"_fetch_tasks text: {response.text}")

    if response.status_code != 200:
        return None

    return response.json()

def main() -> None:
    """Start the bot."""

    yandex_info_response = get_yandex_account_info(YA_ACCESS_TOKEN)
    check_yandex_access_token(yandex_info_response)

    # Create the Application and pass it your bot's token.
    application = (
        Application.builder()
        .token(TG_BOT_TOKEN)
        .build()
    )

    # Create the ConversationHandler with states and handlers
    conversation_handler = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            CREDS: [MessageHandler(filters.TEXT & ~filters.COMMAND, login)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    # Add the ConversationHandler to the application
    application.add_handler(conversation_handler)

    # Add fetch tasks handler to show current tasks details
    async def tasks_caller(*args, **kwargs):
        await tasks(*args, access_token=YA_ACCESS_TOKEN, **kwargs)

    application.add_handler(CommandHandler("tasks", tasks_caller))

    # Run the bot until the user presses Ctrl-C
    application.run_polling(allowed_updates=Update.ALL_TYPES)



def check_yandex_access_token(yandex_info_response) -> str:
    """Recursive way to check if access token works.

    Returns only when login was successful"""

    if yandex_info_response.status_code == 200:
        print("Welcome, " + json.loads(yandex_info_response.text)['login'] + "!")
        return
    else:
        yandex_access_token = get_yandex_access_token_refresh(YA_REFRESH_TOKEN)
        yandex_info_response = get_yandex_account_info(yandex_access_token)
        check_yandex_access_token(yandex_info_response)

def get_yandex_account_info(yandex_access_token) -> requests.Response:
    """Gets yandex account info by giving out access_token
    Usually used to check if access_token is still relevant or not
    """

    yandex_info_response = requests.post(
        YA_INFO_TOKEN_URL,
        data={
            "oauth_token": yandex_access_token,
            "format": "json",
        },
    )
    
    return yandex_info_response

def get_yandex_access_token_captcha() -> str:

    # Handling code retrieved manually from yandex.
    # Sounds like bs but yet required.
    yandex_id_metadata = {}
    while not yandex_id_metadata.get("access_token"):
        yandex_captcha_code = input(f"Code (get from https://oauth.yandex.ru/authorize?response_type=code&client_id={YA_CLIENT_ID}): ")
        yandex_captcha_response = requests.post(
            YA_OAUTH_TOKEN_URL,
            data={
                "grant_type": "authorization_code",
                "code": yandex_captcha_code,
                "client_id": YA_CLIENT_ID,
                "client_secret": YA_CLIENT_SECRET,
            },
        )

        logging.info(f"yandex captcha code: {yandex_captcha_response.status_code}")
        logging.info(f"yandex captcha response: {yandex_captcha_response.text}")

        yandex_id_metadata = json.loads(yandex_captcha_response.text)

    yandex_access_token = yandex_id_metadata["access_token"]
    return yandex_access_token

def get_yandex_access_token_refresh(refresh_token) -> dict:
    """ Refresh access_token using refresh_token,
        update global variables and
        save changes to .env file
    """


    payload = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": YA_CLIENT_ID,
        "client_secret": YA_CLIENT_SECRET
    }
    # payload needs to be url encoded
    payload_url_encoded = urlencode(payload)
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
    }

    response = requests.request("POST", YA_OAUTH_TOKEN_URL, headers=headers, data=payload_url_encoded)

    if response.status_code != 200:
        logging.error(f"Статус ошибки ответа запроса: {response.status_code}")
        return None


    logging.info(f"yandex refresh code response: {response.text}")
    device_creds    = json.loads(response.text)

    access_token    = device_creds['access_token']
    refresh_token   = device_creds['refresh_token']

    global YA_ACCESS_TOKEN
    YA_ACCESS_TOKEN                 = access_token
    os.environ["YA_ACCESS_TOKEN"]   = access_token
    dotenv.set_key(DOTENV_PATH, "YA_ACCESS_TOKEN", os.environ["YA_ACCESS_TOKEN"])


    global YA_REFRESH_TOKEN
    YA_REFRESH_TOKEN                = refresh_token
    os.environ["YA_REFRESH_TOKEN"]  = refresh_token
    dotenv.set_key(DOTENV_PATH, "YA_REFRESH_TOKEN", os.environ["YA_REFRESH_TOKEN"])

    return access_token

# TODO: добавить обновление OAuth-токена через refresh_token

if __name__ == "__main__":
    main()
