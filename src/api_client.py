import os
import requests
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

API_KEY = os.getenv('HEVY_API_KEY')
BASE_URL_WORKOUTS = os.getenv('BASE_URL_WORKOUTS')
BASE_URL_ROUTINES = os.getenv('BASE_URL_ROUTINES')

params = {
    'page': 1,
    'pageSize': 10
}

headers = {
    'accept': 'application/json',
    'api-key': API_KEY
}

def get_data(url: str):
    # Fetch data from a given Hevy API endpoint
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f'Error fetching data: {response.status_code}')
        return None