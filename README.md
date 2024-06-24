# Nasdaq Database

## Requirements

- pandas
- sqlalchemy
- mysql-connector-python
- nasdaq-data-link
- python-dotenv
  - And check more in requirements.txt file.
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
```

To set all local tables to update automatically, please run:

```sh
python -m src.update
```

To get the list of all URLs of data tables, use the following command (Please refer to notebooks/Notebook1.ipynb for running details):

```sh
python -m src.get_url
```

To get the .csv file containing all table_codes, use the following command (Please refer to notebooks/Notebook1.ipynb for running details):

```sh
python -m src.websc
```

To generate the info dictionary of all data_tables, use the following command:

```sh
python -m src.info_dictionary
```
