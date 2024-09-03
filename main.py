import os
import logging
import asyncio
from telebot.async_telebot import AsyncTeleBot
from telebot.types import ReplyKeyboardRemove
from telebot import custom_filters

import modules.gandiva_api as gandiva_api
import modules.yandex_api as yandex_api

# Load environment variables
from dotenv import load_dotenv

load_dotenv()
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")

# Handle not enough data in .env
if not TG_BOT_TOKEN:
    raise ValueError("No TG_BOT_TOKEN found in environment variables. Please check your .env file.")

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

    tasks = await gandiva_api.get_all_tasks_by_filter()
    task_ids = [task['Id'] for task in tasks]
    await yandex_api.add_tasks(tasks, "TEA")
    await yandex_api.batch_move_tasks_status(tasks)
    # res = await yandex_api.get_all_tasks()
    # print(f"\n----------------------------------\n{ids}")
    # await yandex_api.batch_edit_tasks({"type": "improvement"}, issues=ids)
    
    

    # logging.info("Bot started.")
    # await bot.polling()

if __name__ == "__main__":
    asyncio.run(main())