import os
import logging
import asyncio
from telebot.async_telebot import AsyncTeleBot
from telebot.types import ReplyKeyboardRemove
from telebot import custom_filters

import gandiva_api as gandiva_api
import yandex_api as yandex_api
import db_module as db
import aiohttp

# Load environment variables
from dotenv import load_dotenv
load_dotenv()
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
# Handle not enough data in .env
if not TG_BOT_TOKEN:
    raise ValueError("No TG_BOT_TOKEN found in environment variables. Please check your .env file.")

# Define the database URL
DB_URL = 'sqlite:///project.db'  # Using SQLite for simplicity

# Create the database and tables
DB_ENGINE = db.create_database(DB_URL)
DB_SESSION = db.get_session(DB_ENGINE)

# Initialize the bot
bot = AsyncTeleBot(TG_BOT_TOKEN)

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# In-memory user data storage
user_data = {}

# Start command handler
@bot.message_handler(commands=['start'])
async def cmd_start(message):
    await bot.send_message(message.chat.id, "Пожалуйста, введите логин, чтобы проверить статус задач:")
    user_data[message.chat.id] = {}  # Initialize user data for the chat

# Login handler
@bot.message_handler(func=lambda message: message.chat.id in user_data and 'login' not in user_data[message.chat.id])
async def process_login(message):
    user_data[message.chat.id]['login'] = message.text.strip()
    await bot.send_message(message.chat.id, f"Логин принят: {message.text.strip()}.")

# Task fetching handler
@bot.message_handler(commands=['tasks'])
async def fetch_tasks(message):
    if message.chat.id not in user_data or 'login' not in user_data[message.chat.id]:
        await bot.send_message(message.chat.id, "Не удалось найти логин. Пожалуйста, начните с команды /start.")
        return

    login = user_data[message.chat.id]['login']
    logging.info(f"Fetching tasks for user: {login}")

    tasks = await yandex_api.get_page_of_tasks(login=login)

    if tasks is None:
        await bot.send_message(message.chat.id, "Не удалось получить задачи от Yandex.Tracker.")
        return

    if not tasks:
        await bot.send_message(message.chat.id, "У вас нет ни одной активной задачи.")
        return

    for task in tasks:
        task_text = (
            f"Task Number: {task.get('key')}\n"
            f"Title: {task.get('summary')}\n"
            f"Description: {task.get('description')}\n"
            f"Status: {task.get('statusType', {}).get('value')}"
        )
        await bot.send_message(message.chat.id, task_text)

# Cancel command handler
@bot.message_handler(commands=['cancel'])
async def cancel_handler(message):
    if message.chat.id in user_data:
        del user_data[message.chat.id]  # Remove user data
    await bot.send_message(message.chat.id, "Диалог отменен.", reply_markup=ReplyKeyboardRemove())

# Main function to start the bot
async def main():
    # await yandex_api.check_access_token(yandex_api.YA_ACCESS_TOKEN)
    await gandiva_api.get_access_token(gandiva_api.GAND_LOGIN, gandiva_api.GAND_PASSWORD)
    tasks = await gandiva_api.get_all_tasks()
    # tasks = tasks[:10]
    await yandex_api.add_or_edit_tasks(tasks, "TEA")
    # await yandex_api.batch_move_tasks_status(tasks)
    # tasks_db = DB_SESSION.query(db.Task).all()
    # for task in tasks_db:
    #     fields_to_print = f"Task ID: {task.task_id_yandex}, Status: {task.status}"
    #     if task.assignee:
    #         if task.assignee.yandex_login:
    #             fields_to_print = fields_to_print + f", Assignee: {task.assignee.yandex_login}"
    #     print(fields_to_print)
    return
    # task_ids = [task['Id'] for task in tasks]
    print("---------------------------------------\n\n")
    # res = await yandex_api.get_all_tasks()
    # print(f"\n----------------------------------\n{ids}")
    # await yandex_api.batch_edit_tasks({"type": "improvement"}, issues=ids)
    # logging.info("Bot started.")
    # await bot.polling()

if __name__ == "__main__":
    asyncio.run(main())