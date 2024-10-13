## README.md

# GTY (Gandiva To Yandex)

GTY is a service designed to automate synchronization process between Gandiva and Yandex Tracker services.
The project is developed for Success company (IT Department). 

## Requirements

- Python 3.7+
- Telegram bot token
- Yandex OAuth credentials
- Gandiva credentials

## Installation

### 1. Clone the repository

```bash
git clone https://github.com/phtea/GTY.git
cd GTY
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

### 4. Set up environment variables using `config.ini`

The bot uses config.ini file to load sensitive and settings variables.

#### Create a `config.ini` file in the root directory of the project:

```bash
touch config.ini
```

#### Add the following variables to your `config.ini` file based on `config.ini.template` file:

```ini
config.ini.template
; Template of configuration file

[Settings]
sync_mode           = 2
queue               = QUEUE
board_id            = 10
interval_minutes    = 5
to_get_followers    = False
use_summaries       = False

...
```

### 5. Run the bot

To start the bot, run:

```bash
pythonw main.py
```
---
Feel free to contribute to the project by creating a PR! :)
