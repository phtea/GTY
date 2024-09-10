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

#### Add the following variables to your `config.ini` file:

```ini
; Configuration file

[Settings]
sync_mode           = 1
queue               = TEA
board_id            = 52
interval_minutes    = 5

[Gandiva]
login                   = your_login
password                = your_password
programmer_id           = your_programmer_id
max_concurrent_requests = 10

[Yandex]
x_cloud_org_id  = your_x_cloud_org_id
oauth_token     = your_oauth_token
client_id       = your_client_id
client_secret   = your_client_secret

[Database]
url = sqlite:///project.db

[YandexCustomFieldIds]
gandiva                 = your_gandiva_field_id
initiator               = your_initiator_field_id
initiator_department    = your_initiator_department_field_id
```

### 5. Run the bot

To start the bot, run:

```bash
python main.py
```

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for more details.

---

This is a basic setup for the GTY. Adjust the `config.ini` file settings and Python dependencies as needed for your specific environment.
