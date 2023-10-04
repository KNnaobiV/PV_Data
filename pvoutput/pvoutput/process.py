import os
import glob
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

import sys
sys.path.append(".")

from definitions import get_countries, SCRAPPER_ROOT_DIR
from pipelines import PostgresPipeline

import pandas as pd

from spiders.pvoutput_spiders import (
    DailyPowerGenerationSpider, AggregatePowerGenerationSpider,
    CountrySystemsSpider, SystemInfoSpider, SystemLocationSpider
)

COUNTRY_OUT_BASE = os.path.join(SCRAPPER_ROOT_DIR, "output", "countries")
SYSTEM_FILES = os.path.join(COUNTRY_OUT_BASE, "**/systems.csv")
SYSTEM_FILES = [sys_file for sys_file in glob.glob(SYSTEM_FILES)]


DURATIONS = ["weekly", "monthly", "yearly"]

def get_systems_in_country(country):
    """
    Runs the crawler for all the systems in the country.

    Parameters
    ----------
    country: str
        name of the country
    """
    countries = get_countries()
    country_id = countries[country]
    process = CrawlerProcess(get_project_settings())
    process.crawl(CountrySystemsSpider, id=country_id, country=country)
    process.start()
    country_pipeline = PostgresPipeline()
    #country_pipeline.add_country()


def get_systems_in_all_countries():
    countries = get_countries()
    for country in countries.keys():
        country_id = countries[country]
        process = CrawlerProcess(get_project_settings())
        process.crawl(CountrySystemsSpider, id=country_id, country=country)
    process.start()

get_systems_in_all_countries()

def get_system_info():
    """
    Runs the crawler to get information on a system.

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
    pd.concat(df_list)
    systems = df.to_dict(orient="records")
    process = CrawlerProcess(get_project_settings())
    pipeline = PostgresPipeline()

    for system in systems:
        name, id, sid = system["name"], system["id"], system["sid"]
        durations = DURATIONS + ["daily"]
    # process.configure()
        process.crawl(SystemInfoSpider, sid=sid, name=name)
        process.crawl(SystemLocationSpider, sid=sid, id=id, name=name)
        duration_dict = {
            "weekly": "w",
            "monthly": "m",
            "yearly": "y",
        }
        for duration in durations:
            if duration.lower() == "daily":
                process.crawl(DailyPowerGenerationSpider, id=id, sid=sid, name=name)
            elif duration in DURATIONS:
                # duration_val = duration_dict[duration]
                process.crawl(
                    AggregatePowerGenerationSpider,
                        id=id, sid=sid, duration=duration, name=name#duration_val
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


# get_system_info(file)
# get_infos(file)