import os
import logging
import asyncio
from telebot.async_telebot import AsyncTeleBot
from telebot.types import ReplyKeyboardRemove
from telebot import custom_filters

import gandiva_api
import yandex_api
import db_module as db
import utils

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

async def move_tasks_to_new_status_bulk(g_tasks, ya_tasks):
    grouped_ya_tasks = utils.filter_and_group_tasks_by_new_status(gandiva_tasks=g_tasks, yandex_tasks=ya_tasks)
    if not grouped_ya_tasks:
        logging.info("All statuses are already up-to-date!")
    await yandex_api.move_groups_tasks_status(grouped_ya_tasks)

async def sync_gandiva_comments(g_tasks, sync_mode: int):
    """Synchronizes comments between services.
    sync_mode can be 1 or 2:
    1 - sync all comments
    2 - sync only comments for programmers"""
    g_tasks_ids = utils.extract_task_ids(g_tasks)
    # TODO: ONLY FOR NOW!! [:10]
    tasks_comments = await gandiva_api.get_comments_for_tasks(g_tasks_ids[:10])
    
    for task_id, comments in tasks_comments.items():
        yandex_task = db.find_task_by_gandiva_id(session=DB_SESSION, task_id_gandiva=task_id)
        yandex_task_id = yandex_task.task_id_yandex
        
        for comment in comments:
            # Extract required fields
            g_comment_id = comment['Id']
            text_html = comment['Text']
            text = utils.html_to_yandex_comment(text_html)
            author = comment['Author']
            author_name = f"{author['FirstName']} {author['LastName']}"
            addressees = comment.get('Addressees', [])

            if sync_mode == 2:
                # Send only if one of the addressees is GANDIVA_PROGRAMMER_ID
                send_comment = False
                for addressee in addressees:
                    if addressee['User']['Id'] == gandiva_api.GAND_PROGRAMMER_ID:
                        send_comment = True
                        break
                if not send_comment:
                    continue  # Skip comment if the programmer is not found
            
            # Send the comment if sync_mode is not 2 or if GANDIVA_PROGRAMMER_ID is found
            await yandex_api.add_comment(yandex_task_id=yandex_task_id, comment=text, g_comment_id=str(g_comment_id), author_name=author_name)

async def sync_services():
    await yandex_api.check_access_token(yandex_api.YA_ACCESS_TOKEN)
    await gandiva_api.get_access_token(gandiva_api.GAND_LOGIN, gandiva_api.GAND_PASSWORD)
    g_tasks = await gandiva_api.get_all_tasks()
    await yandex_api.add_or_edit_tasks(g_tasks, "TEA")
    ya_tasks = await yandex_api.get_all_tasks()
    await move_tasks_to_new_status_bulk(g_tasks, ya_tasks)
    g_tasks_ids = utils.extract_task_ids(g_tasks)

# Main function to start the bot
async def main():
    # await sync_services()
    await gandiva_api.get_access_token(gandiva_api.GAND_LOGIN, gandiva_api.GAND_PASSWORD)
    g_tasks = await gandiva_api.get_all_tasks()
    
    await sync_gandiva_comments(g_tasks, 1)





    return    
    await bot.polling()



if __name__ == "__main__":
    asyncio.run(main())