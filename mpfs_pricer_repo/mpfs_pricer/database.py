import os
import psycopg2
import logging

DEFAULT_USER = "pricer_read_only"
DEFAULT_HOST = ""
DEFAULT_PASSWORD = ""


def get_db_config_from_env(db_name=None):
    host = os.environ.get("DB_HOST", DEFAULT_HOST)
    database = os.environ.get("DB_NAME", db_name)
    port = os.environ.get("DB_PORT", "5432")
    user = os.environ.get("DB_USER", DEFAULT_USER)
    password = os.environ.get("DB_PASSWORD", DEFAULT_PASSWORD)
    if not database or not password or not user or not host:
        logging.error(
            "You need to set the following environment variables to be able to access the database: DB_HOST, DB_NAME, DB_PORT (optional - defaults to 5432), DB_USER, DB_PASSWORD")
        quit(1)

    return {
        "host": host,
        "database": database,
        "port": port,
        "user": user,
        "password": password,
    }


def get_db(db_config):
    conn = psycopg2.connect(
        f"host={db_config['host']} dbname={db_config['database']} user={db_config['user']} port={db_config['port']} password={db_config['password']}")
    return conn
