import asyncio
import aiohttp
import logging
# String manipulations
import urllib.parse

import os
import dotenv
import json

DOTENV_PATH = os.path.join(os.path.dirname(__file__), os.pardir)
DOTENV_PATH = os.path.join(DOTENV_PATH, '.env')
if os.path.exists(DOTENV_PATH):
    dotenv.load_dotenv(DOTENV_PATH)

GAND_ACCESS_TOKEN   = os.environ.get("GAND_ACCESS_TOKEN")
GAND_REFRESH_TOKEN   = os.environ.get("GAND_REFRESH_TOKEN")
GAND_PASSWORD       = os.environ.get("GAND_PASSWORD")
GAND_LOGIN          = os.environ.get("GAND_LOGIN")

# TODO: before each request add token regeneration if token expired

# Handle not enough data in .env
if not (GAND_PASSWORD and GAND_LOGIN):
    raise ValueError("""Not enough data found in environment variables. Please check your .env file:
                     GAND_PASSWORD
                     GAND_LOGIN""")

HOST = "https://api-gandiva.s-stroy.ru"

async def get_access_token(username, password):
    """Updates GAND_ACCESS_TOKEN and GAND_REFRESH_TOKEN from login+password
    
    Returns access_token"""
    endpoint = "/Token"
    url = HOST + endpoint

    body = {
        "grant_type": "password",
        "username": username,
        "password": password
    }
    body = urllib.parse.urlencode(body)

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, data=body) as response:
            response_json = await response.json()
            access_token = response_json.get('access_token')
            refresh_token = response_json.get('refresh_token')

            # Update global tokens and environment variables
            global GAND_ACCESS_TOKEN
            GAND_ACCESS_TOKEN = access_token
            os.environ["GAND_ACCESS_TOKEN"] = access_token
            dotenv.set_key(DOTENV_PATH, "GAND_ACCESS_TOKEN", access_token)

            global GAND_REFRESH_TOKEN
            GAND_REFRESH_TOKEN = refresh_token
            os.environ["GAND_REFRESH_TOKEN"] = refresh_token
            dotenv.set_key(DOTENV_PATH, "GAND_REFRESH_TOKEN", refresh_token)

            return access_token


async def refresh_access_token(refresh_token):
    """Gets and updates access_token + refresh_token
    
    Returns access_token"""
    endpoint = "/Token"
    url = HOST + endpoint

    body = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }
    body = urllib.parse.urlencode(body)

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, data=body) as response:
            response_json = await response.json()
            access_token = response_json.get('access_token')
            refresh_token = response_json.get('refresh_token')

            # Update global tokens and environment variables
            global GAND_ACCESS_TOKEN
            GAND_ACCESS_TOKEN = access_token
            os.environ["GAND_ACCESS_TOKEN"] = access_token
            dotenv.set_key(DOTENV_PATH, "GAND_ACCESS_TOKEN", access_token)

            global GAND_REFRESH_TOKEN
            GAND_REFRESH_TOKEN = refresh_token
            os.environ["GAND_REFRESH_TOKEN"] = refresh_token
            dotenv.set_key(DOTENV_PATH, "GAND_REFRESH_TOKEN", refresh_token)
            print('All good')

            return access_token

#  We pass session to functions which are going to be reused often 
# (using a single session for multiple requests is more effective)
async def get_task_by_id(session, request_id):
    headers = {
        "Content-type": "application/x-www-form-urlencoded",
        "Authorization": f"Bearer {GAND_ACCESS_TOKEN}"
    }

    url = f"{HOST}/api/Requests/{request_id}"

    async with session.get(url, headers=headers) as response:
        if response.status in [200, 201]:
            return await response.json()
        else:
            print(f"Failed to fetch task {request_id}: {response.status} - {await response.text()}")
            return False

async def get_page_of_tasks_by_filter(session, page_number):
    headers = {
        "Content-type": "application/json",
        "Authorization": f"Bearer {GAND_ACCESS_TOKEN}"
    }
    endpoint = "/api/Requests/Filter"
    url = HOST + endpoint
    filter_departments = [2]
    filter_categories = [32]

    filter_data = {
        "Departments": filter_departments,
        "Categories": filter_categories
    }

    body = {
        "Filtering": filter_data,
        "BaseFilter": 0,
        "Page": page_number,
        "Size": 100,
        "Sorting": 0,
        "Descending": False
    }

    body = json.dumps(body)

    async with session.post(url, headers=headers, data=body) as response:
        if response.status in [200, 201]:
            logging.info(f"Page {page_number} fetched.")
            return await response.json()
        else:
            logging.error(f"Failed to fetch page {page_number}: {response.status} - {await response.text()}")
            return False

async def get_all_tasks_by_filter():
    all_requests = []
    
    async with aiohttp.ClientSession() as session:
        # Fetch the first page to get the total count and number of pages
        first_page_data = await get_page_of_tasks_by_filter(session, 1)
        if not first_page_data:
            return all_requests  # Return an empty list if the first page request fails

        total_requests = first_page_data['Total']
        all_requests.extend(first_page_data['Requests'])

        # Calculate the total number of pages
        total_pages = (total_requests // 100) + 1

        # Create a list of tasks for all remaining pages
        tasks = [
            get_page_of_tasks_by_filter(session, page_number)
            for page_number in range(2, total_pages + 1)
        ]

        # Run all tasks concurrently
        responses = await asyncio.gather(*tasks)

        # Collect the requests from all the pages
        for response in responses:
            if response:
                all_requests.extend(response['Requests'])

    return all_requests

import time # using for timing functions
async def main():
    await get_access_token(GAND_LOGIN, GAND_PASSWORD)
    # response = await get_page_of_requests_by_filter(1)
    # response = await get_request_by_id(1002)
    start_time = time.time()
    response = await get_all_tasks_by_filter()
    print("--- %s seconds ---" % (time.time() - start_time))
    # print(response)


if __name__ == '__main__':
    # get_access_token(GAND_LOGIN, GAND_PASSWORD)
    asyncio.run(main())