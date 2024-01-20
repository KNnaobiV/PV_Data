import glob
import itertools as it
from operator import itemgetter
import os

from scrapy.crawler import CrawlerProcess, CrawlerRunner
from scrapy.utils.project import get_project_settings

# from definitions import get_countries, SCRAPPER_ROOT_DIR
from models import Country, System
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


# COUNTRY_OUT_BASE = os.path.join(SCRAPPER_ROOT_DIR, "output", "countries")
# SYSTEM_FILES = os.path.join(COUNTRY_OUT_BASE, "**/systems.csv")
# SYSTEM_FILES = [sys_file for sys_file in glob.glob(SYSTEM_FILES)]


DURATIONS = ["weekly", "monthly", "yearly"]

def get_countries():
    session = load_session()
    # pipeline = DataPipeline(session)
    runner = CrawlerProcess(get_project_settings())
    runner.crawl(GetCountries, session=session)
    runner.start()


def get_systems_in_country(sid, name):
    """
    Runs the crawler for all the systems in the country.

    :param country: name of the country
    """
    process = CrawlerProcess(get_project_settings())
    process.crawl(CountrySystemsSpider, sid=sid, country=name)
    process.start(stop_after_crawl=False)


def get_systems_in_all_countries():
    """
    Runner for crawling all the systems and saving their data to 
    specified output folder as 'systems.csv' under the folder of the 
    available countries.

    Note
    ----
    The output folder is created in the specified root directory.    
    """
    try:
        session = load_session()
        countries = session.query(Country).all()[10:]
        for country in countries:
            # get_systems_in_country(country.sid, country.name)
            process = CrawlerProcess(get_project_settings())
            process.crawl(CountrySystemsSpider, sid=country.sid, country=country.name, session=session)
        process.start()
    finally:
        session.close()

def get_system_info():
    """
    some some
    """
    """ df_list = []
    for file in SYSTEM_FILES:
        df = pd.read_csv(file)
        df_list.append(df)
    df = pd.concat(df_list)
    systems = df.to_dict(orient="records")
    process = CrawlerProcess(get_project_settings())
    pipeline = DataPipeline()"""

    try:
        session = load_session()
        systems = session.query(System).all()[:20]
        process = CrawlerProcess(get_project_settings())
        for sys in systems:
            sys_name, sys_id, sys_sid = sys.name, sys.id, sys.sid
            country = session.query(Country).filter_by(sid=sys.country_sid).first()
            country_name = country.name

            process.crawl(
                SystemInfoSpider, 
                session=session,
                sid=sys_sid, 
                country_name=str(country_name), 
                system_name=sys_name
            )
            # process = CrawlerProcess(get_project_settings())
            process.crawl(
                SystemLocationSpider,
                session=session,
                sid=sys_sid, 
                id=sys_id, 
                country_name=str(country_name),
            )

            # process.crawl(
            #     DailyPowerGenerationSpider, 
            #     session=session,
            #     sid=sys_sid, 
            #     id=sys_id, 
            #     country_name=str(country_name),
            #     system_name=sys_name
            # )
            durations = ["monthly", "yearly"]
            for duration in durations:
                process.crawl(
                    AggregatePowerGenerationSpider,
                    session=session,
                    sid=sys_sid, 
                    id=sys_id, 
                    country_name=str(country_name),
                    system_name=sys_name,
                    duration=duration,
                )
        process.start()
    finally:
        session.close()
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
# get_countries()
# get_systems_in_all_countries()
get_system_info()