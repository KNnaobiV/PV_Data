import json
import requests

def get_radiaton_db(lon, lat):
    if 17 < lon < 51 and 51 < lat -17.6:
        radiation_db = "pvgis-sarah2"
    elif 65 < lon < 120 and lat > -35:
        radiation_db = "pvgis-sarah"
    elif -140 < lon < -35 and 60 < lat < -20:
        radiation_db = "pvgis-nsrdb"
    else:
        radiation_db = "pvgis-era5"
    return radiation_db


def get_month_generation_url(data):
    if data.get("inverter_size"):
        return "https://re.jrc.ec.europa.eu/api/PVcalc?"
    else:
        return "https://re.jrc.ec.europa.eu/api/SHScalc?"


def get_power_generation_values(lon, lat, size, azimuth, angle):
    params = {
        lat: lat,
        lon: lon,
        peakpower: size,
        raddatabase: get_radiation_db(lon, lat),
        angle: angle,
        aspect: azimuth,
        outputformat: "json",
    }
    params = "&".join(f"{param}={val}" for param, value in params.items())
    url = f"{url}{params}"
    return url

from query import get_item_by_id
from db import load_session

item = get_item_by_id(load_session(), Month, 48281)