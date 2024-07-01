import os
from dotenv import load_dotenv

basedir = os.path.abspath(os.path.dirname(__file__))

load_dotenv(os.path.join(basedir, ".env"))


class Config:
    DB_PASSWORD = os.environ.get('DB_PASSWORD')
    DB_USERNAME = os.environ.get('DB_USERNAME')
    DB_MYSQL_PASSWORD = os.environ.get('DB_MYSQL_PASSWORD')
    DB_MYSQL_USERNAME = os.environ.get('DB_MYSQL_USERNAME')
    DB_MYSQL_DBNAME = os.environ.get('DB_MYSQL_DBNAME')
