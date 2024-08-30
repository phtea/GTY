## README.md

# TODO
- [] обработать незаполненные значения в .env
- [] добавить модуль пагинации (постраничный вывод) задач
- [] выделить функционал по API Yandex в отдельный модуль

# SuccessYaTrackerTGBot

SuccessYaTrackerTGBot is a Telegram bot designed to interact with Yandex.Tracker, allowing users to fetch and display tasks assigned to them. The bot is built using Python, the Telegram API, and the Yandex OAuth API for authentication and task management.

## Features

- Users can log in with their Yandex account to check their tasks.
- The bot fetches tasks from Yandex.Tracker using the user's login credentials.
- Handles OAuth authentication with Yandex, including access token refresh.

## Requirements

- Python 3.7+
- Telegram bot token
- Yandex OAuth credentials

## Installation

### 1. Clone the repository

```bash
git clone https://github.com/phtea/SuccessYaTrackerTGBot.git
cd SuccessYaTrackerTGBot
```

### 2. Create a virtual environment (optional but recommended)

```bash
python -m venv venv
source venv/bin/activate  # On Windows use `venv\Scripts\activate`
```

### 3. Install required Python packages

First, make sure you have `pip` installed. Then, run:

```bash
pip install -r requirements.txt
```

### 4. Set up environment variables using `.env`

The bot uses the `python-dotenv` package to load environment variables from a `.env` file. This file should contain your sensitive data, such as the Telegram bot token and Yandex OAuth credentials.

#### Create a `.env` file in the root directory of the project:

```bash
touch .env
```

#### Add the following variables to your `.env` file:

```env
TG_BOT_TOKEN=your-telegram-bot-token
YA_X_CLOUD_ORG_ID=your-yandex-cloud-org-id
YA_ACCESS_TOKEN=your-yandex-access-token
YA_CLIENT_ID=your-yandex-client-id
YA_CLIENT_SECRET=your-yandex-client-secret
YA_REFRESH_TOKEN=your-yandex-refresh-token
```

- **TG_BOT_TOKEN**: Your Telegram bot token.
- **YA_X_CLOUD_ORG_ID**: Your Yandex Cloud organization ID.
- **YA_ACCESS_TOKEN**: Your Yandex OAuth access token.
- **YA_CLIENT_ID**: Your Yandex OAuth client ID.
- **YA_CLIENT_SECRET**: Your Yandex OAuth client secret.
- **YA_REFRESH_TOKEN**: Your Yandex OAuth refresh token.

### 5. Run the bot

To start the bot, run:

```bash
python main.py
```

The bot will start polling for updates. You can interact with it on Telegram by sending commands.

## Usage

- Start a conversation with the bot by typing `/start`.
- The bot will ask for your Yandex login to fetch your tasks.
- Use the `/tasks` command to display your tasks from Yandex.Tracker.

## Refreshing Yandex OAuth Token

If your Yandex OAuth token expires, the bot will attempt to refresh it automatically using the refresh token. Make sure your `.env` file is correctly configured with all required Yandex OAuth credentials.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for more details.

---

This is a basic setup for the SuccessYaTrackerTGBot. Adjust the `.env` file settings and Python dependencies as needed for your specific environment.
