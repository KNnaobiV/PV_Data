import configparser

import requests

cfg = configparser.ConfigParser()
cfg.read_file("config.cfg")
SYSTEM_ID = cfg.get("API", "SYSTEM_ID", fallback=None)
KEY = cfg.get("API", "KEY", fallback=None)

def get_country_system_ids(country):
    """
    Get all system ids for the specified country

    Parameters
    ----------
    country: str
    """
    

url = f"https://pvoutput.org/list.jsp?sid={SYSTEM_ID}"
headers = {
    "X-Pvoutput-Apikey": KEY,
    "X-Pvoutput-SystemId": SYSTEM_ID
    }
response = requests.get(url)