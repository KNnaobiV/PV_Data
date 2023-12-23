import os
import glob
from operator import itemgetter

from scrapy.crawler import CrawlerProcess, CrawlerRunner
from scrapy.utils.project import get_project_settings
from twisted.internet import reactor

from definitions import get_countries, SCRAPPER_ROOT_DIR
from models import Country
from items import CountryItem
from pipelines import DataPipeline
from vernay.utils import query as q
from vernay.utils import load_session

import pandas as pd

from spiders.pvoutput_spiders import (
    DailyPowerGenerationSpider, AggregatePowerGenerationSpider,
    CountrySystemsSpider, SystemInfoSpider, SystemLocationSpider,
    GetCountries
)


__all__ = [
    "get_systems_in_country",
    "get_systems_in_all_country",
    "get_system_info",
]


COUNTRY_OUT_BASE = os.path.join(SCRAPPER_ROOT_DIR, "output", "countries")
SYSTEM_FILES = os.path.join(COUNTRY_OUT_BASE, "**/systems.csv")
SYSTEM_FILES = [sys_file for sys_file in glob.glob(SYSTEM_FILES)]


DURATIONS = ["weekly", "monthly", "yearly"]

def get_countries():
    session = load_session()
    pipeline = DataPipeline(session)
    runner = CrawlerProcess(get_project_settings())
    runner.crawl(GetCountries, pipeline=pipeline)
    runner.start()


def get_systems_in_country(country):
    """
    Runs the crawler for all the systems in the country.

    :param country: name of the country
    """
    session = load_session()
    country = q.get_id_from_name(session, Country, country)
    country_id = country.sid
    process = CrawlerProcess(get_project_settings())
    process.crawl(CountrySystemsSpider, id=country_id)
    process.start()
    # country_pipeline = DataPipeline()
    # country_pipeline.add_country()


def get_systems_in_all_countries():
    """
    Runner for crawling all the systems and saving their data to 
    specified output folder as 'systems.csv' under the folder of the 
    available countries.

    Note
    ----
    The output folder is created in the specified root directory.    
    """
    countries = get_countries()
    for country in countries.keys():
        country_id = countries[country]
        process = CrawlerProcess(get_project_settings())
        process.crawl(CountrySystemsSpider, id=country_id, country=country)
    process.start()


def get_system_info():
    """
    Runs the crawler to get information on a system.
    The system information is saved as a csv file under a folder with 
    the system name in the system's country folder.

    Note
    ----
    The csv files are saved as 'd.csv', 'w.csv', 'm.csv', 'y.csv'. These 
    represent the daily, weekly, monthly and yearly data respectively.

    Parameters
    ----------
    id: str or int
        system id
    sid: str or int
        system sid
    name, str:
        system name
    durations: list
        List durations. Durations be daily, weekly, monthly or
    """
    df_list = []
    for file in SYSTEM_FILES:
        df = pd.read_csv(file)
        df_list.append(df)
    df = pd.concat(df_list)
    systems = df.to_dict(orient="records")
    process = CrawlerProcess(get_project_settings())
    pipeline = DataPipeline()

    for system in systems:
        country, name, id, sid = itemgetter(
            "country", "name", "id", "sid")(system)
        durations = DURATIONS + ["daily"]
    # process.configure()
        process.crawl(
            SystemInfoSpider, 
            sid=sid, 
            country_name=str(country), 
            system_name=name
        )
        process.crawl(
            SystemLocationSpider, 
            sid=sid, 
            id=id, 
            country_name=str(country),
        )
        duration_dict = {
            "weekly": "w",
            "monthly": "m",
            "yearly": "y",
        }
        for duration in durations:
            if duration.lower() == "daily":
                process.crawl(
                    DailyPowerGenerationSpider, 
                    id=id, 
                    sid=sid,
                    country_name=str(country), 
                    system_name=name
                )
            elif duration in DURATIONS:
                process.crawl(
                    AggregatePowerGenerationSpider,
                    id=id, 
                    sid=sid, 
                    country_name=str(country),
                    system_name=name,
                    duration=duration,
                )
            else:
                raise ValueError(
                    f"Cannot select duration {duration}."
                    " Duration can either be daily, weekly, monthly, or yearly."
                )
    process.start()
    # process.start(stop_after_crawl=False)

# get_systems_in_country("Switzerland")

def get_systems():
    countries = get_countries()
    for country, id in countries.items():
        process = CrawlerProcess(get_project_settings())
        process.crawl(CountrySystemsSpider, id=id, country=country)
    process.start()

def get_infos(file):
    df = pd.read_csv(file)
    systems = df.to_dict(orient="records")
    for system in systems:
        name, id, sid = system["name"], system["id"], system["sid"]
        durations = DURATIONS + ["daily"]
        process = get_system_info(id=id, sid=sid, name=name, durations=durations)
    process.start()


# get_system_info()
# get_systems_in_country(" Switzerland")
get_countries()