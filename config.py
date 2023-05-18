import os
from dotenv import load_dotenv


# Подгружаем из переменных окружения, локально будут храниться токены в файл .env
load_dotenv()
database = os.getenv('database')
user = os.getenv('user')
password = os.getenv('password')
