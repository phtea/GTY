# t.me/SuccessYaTrackerBot

# Import our modules
import modules.gandiva_api as gandiva_api
import modules.yandex_api as yandex_api

import json
import logging
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
import asyncio

# Get sensitive data from dotenv
import os
import dotenv
DOTENV_PATH = os.path.join(os.path.dirname(__file__), '.env')
if os.path.exists(DOTENV_PATH):
    dotenv.load_dotenv(DOTENV_PATH)
TG_BOT_TOKEN        = os.environ.get("TG_BOT_TOKEN")

# TODO: обработать незаполненные значения в .env

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

async def show_tasks(
    update: Update,
    context: CallbackContext,
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

    tasks = await yandex_api.get_tasks_by_login(login)

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

async def main() -> None:
    """Start the bot."""

    await yandex_api.check_yandex_access_token()

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
        await show_tasks(*args, access_token=yandex_api.YA_ACCESS_TOKEN, **kwargs)

    application.add_handler(CommandHandler("tasks", tasks_caller))

    # Run the bot until the user presses Ctrl-C
    application.run_polling(allowed_updates=Update.ALL_TYPES)



if __name__ == "__main__":
    asyncio.run(main())