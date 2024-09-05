import asyncio
import aiohttp
import logging
# String manipulations
import urllib.parse

import os
import dotenv
import json
import csv

# Load environment variables
DOTENV_PATH = ".env"
dotenv.load_dotenv()

GAND_ACCESS_TOKEN   = os.environ.get("GAND_ACCESS_TOKEN")
GAND_REFRESH_TOKEN   = os.environ.get("GAND_REFRESH_TOKEN")
GAND_PASSWORD       = os.environ.get("GAND_PASSWORD")
GAND_LOGIN          = os.environ.get("GAND_LOGIN")

# Dictionary for caching user departments
user_department_cache = {}

# TODO: before each request add token regeneration if token expired

# Handle not enough data in .env
if not (GAND_PASSWORD and GAND_LOGIN):
    raise ValueError("""Not enough data found in environment variables. Please check your .env file:
                     GAND_PASSWORD
                     GAND_LOGIN""")
HOST = "https://api-gandiva.s-stroy.ru"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

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
            logging.info('Successfully refreshed Gandiva token')

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

async def get_department_by_user_id(session, user_id):

    # Check if the result is already cached
    if user_id in user_department_cache:
        logging.info(f"Returning cached department for user {user_id}")
        return user_department_cache[user_id]

    headers = {
        "Content-type": "application/x-www-form-urlencoded",
        "Authorization": f"Bearer {GAND_ACCESS_TOKEN}"
    }
    endpoint = f"api/Users/{user_id}"
    url = f"{HOST}/{endpoint}"

    async with session.get(url, headers=headers) as response:
        if response.status == 200:
            user_data = await response.json()
            logging.info(f"Succesfully gathered user {user_id}")

            department = user_data.get("Department")
            
            # Cache the department value
            user_department_cache[user_id] = department

            return department
        else:
            logging.error(f"Error for user {user_id} <{response.status}>")
            return None
        
async def get_departments_for_users(user_ids):
    async with aiohttp.ClientSession() as session:
        tasks = [get_department_by_user_id(session, user_id) for user_id in user_ids]
        departments = await asyncio.gather(*tasks)
    unique_departments = list(set(departments))
    return unique_departments     

async def get_page_of_tasks(session, page_number):
    headers = {
        "Content-type": "application/json",
        "Authorization": f"Bearer {GAND_ACCESS_TOKEN}"
    }
    endpoint = "/api/Requests/Filter"
    url = HOST + endpoint
    filter_departments  = [2]
    filter_categories   = [32]
    filter_statuses     = [4, 5, 6, 8, 10, 11, 12, 13]

    filter_data = {
        "Departments": filter_departments,
        "Categories": filter_categories,
        "Statuses": filter_statuses
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

# Define the function to get comments for a specific task
async def get_task_comments(task_id, session=None):
    """
    Asynchronously retrieves comments for a specific task by its ID.

    Parameters:
    - task_id (int): The ID of the task to retrieve comments for.
    - session (aiohttp.ClientSession, optional): An optional aiohttp session object to use for the request.
                                                 If None, an exception will be raised.

    Returns:
    - list: A list of comments associated with the task, or an empty list if there are no comments or an error occurs.

    Raises:
    - ValueError: If session is None.

    Example Usage:
    >>> comments = await get_task_comments(task_id=12345, session=session)
    """
    if session is None:
        raise ValueError("Session must be provided.")

    # Set up the headers
    headers = {
        "Content-type": "application/json",
        "Authorization": f"Bearer {GAND_ACCESS_TOKEN}"
    }

    # Construct the URL for the GET request
    endpoint = f"/api/Requests/{task_id}/Comments"
    url = HOST + endpoint

    try:
        # Make the HTTP GET request using the session
        async with session.get(url, headers=headers) as response:
            if response.status in [200, 201]:
                logging.info(f"Comments for task {task_id} fetched successfully.")
                return await response.json()
            else:
                logging.error(f"Failed to fetch comments for task {task_id}: {response.status} - {await response.text()}")
                return []  # Return an empty list in case of failure
    except Exception as e:
        logging.error(f"Exception occurred while fetching comments for task {task_id}: {str(e)}")
        return []  # Return an empty list if an exception occurs

# Define the function to get comments for a list of task IDs concurrently
async def get_comments_for_tasks(task_ids: list[int]):
    """
    Asynchronously retrieves comments for a list of tasks by their IDs.

    Parameters:
    - task_ids (list): A list of task IDs to retrieve comments for.

    Returns:
    - dict: A dictionary where the keys are task IDs and the values are lists of comments associated with those tasks.
            If a task fails to retrieve comments, its value will be an empty list.
    
    Example Usage:
    >>> task_comments = await get_comments_for_tasks([12345, 67890])
    """
    task_comments = {}

    async with aiohttp.ClientSession() as session:
        # Create a list of tasks for fetching comments for each task ID
        tasks = [
            get_task_comments(task_id, session)
            for task_id in task_ids
        ]

        # Run all tasks concurrently
        responses = await asyncio.gather(*tasks)

        # Collect the comments for each task ID
        for task_id, comments in zip(task_ids, responses):
            task_comments[task_id] = comments

    return task_comments

async def get_all_tasks():
    all_requests = []
    
    async with aiohttp.ClientSession() as session:
        # Fetch the first page to get the total count and number of pages
        first_page_data = await get_page_of_tasks(session, 1)
        if not first_page_data:
            return all_requests  # Return an empty list if the first page request fails

        total_requests = first_page_data['Total']
        all_requests.extend(first_page_data['Requests'])

        # Calculate the total number of pages
        total_pages = (total_requests // 100) + 1
        logging.info(f"Found {total_pages} pages of tasks in Gandiva.")

        # Create a list of tasks for all remaining pages
        tasks = [
            get_page_of_tasks(session, page_number)
            for page_number in range(2, total_pages + 1)
        ]

        # Run all tasks concurrently
        responses = await asyncio.gather(*tasks)

        # Collect the requests from all the pages
        for response in responses:
            if response:
                all_requests.extend(response['Requests'])

    return all_requests

def get_all_unique_initiators(data):
    # Extract unique Initiator IDs
    unique_initiator_ids = {request['Initiator']['Id'] for request in data}

    # Convert to a list (if needed)
    unique_initiator_ids_list = list(unique_initiator_ids)

    # Output the unique IDs
    return unique_initiator_ids_list


def save_list_to_csv(data_list, filename):
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        for item in data_list:
            writer.writerow([item])  # Write each item in a new row

import time # using for timing functions
async def main():

    await get_access_token(GAND_LOGIN, GAND_PASSWORD)

        # response = await get_page_of_tasks_by_filter(page_number=1, session=session)
        # res = await get_department_by_user_id(session=session, user_id=138)


    start_time = time.time()
    # await get_all_tasks_by_filter()
    task_ids = [852, 853, 4382]
    tasks_comments = await get_comments_for_tasks(task_ids)
    
    # user_ids = get_all_unique_initiators(response_tasks)

    # dep_ids = await get_departments_for_users(user_ids)

    # print(dep_ids)

    print("--- %s seconds ---" % (time.time() - start_time))

if __name__ == '__main__':
    # get_access_token(GAND_LOGIN, GAND_PASSWORD)
    asyncio.run(main())