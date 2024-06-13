# Nasdaq Database

## Requirements

- pandas
- sqlalchemy
- mysql-connector-python
- nasdaq-data-link
- python-dotenv

## Setup

1. Install the requirements:
    ```bash
    pip install -r requirements.txt
    ```

2. Configure the `.env` file with your MySQL database credentials.

    Create .env file in the root folder
    
    Example:
    ```
    NASDAQ_API = '***'
    MYSQL_HOST = 'localhost'
    MYSQL_USER = 'root'
    MYSQL_PASSWORD = '***'
    MYSQL_DB = 'nasdaqdata'
    ```
## Running the Scripts

To download and store data, use the following command:
```bash
python -m src.utils




