
import os
from os.path import join, dirname
from dotenv import load_dotenv

# Create .env file path.
dotenv_path = join(dirname(__file__), ".env")

# Load file from the path.
load_dotenv(dotenv_path)

# Accessing variables.
TOKEN = os.getenv("TOKEN")
HOST = os.getenv("HOST")
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
DB = os.getenv("DB")
