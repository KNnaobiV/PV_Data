import glob
import itertools as it
from operator import itemgetter
import os

from scrapy.crawler import CrawlerProcess, CrawlerRunner
from scrapy.utils.project import get_project_settings

# from definitions import get_countries, SCRAPPER_ROOT_DIR
from models import Country, System, Daily, Weekly, Monthly, Yearly
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
    "get_countries",
    "get_info_for_all_systems",
    "get_power_generation_info_for_all_systems",
    "get_systems_in_country",
    "get_systems_in_all_country",
    "get_system_info",
]



def get_countries():
    session = load_session()
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
    process.start()


def get_systems_in_all_countries():
    """
    Runs crawler for all systems in all countries saved to the the db.    
    """
    try:
        session = load_session()
        countries = session.query(Country).all()
        for country in countries:
            process = CrawlerProcess(get_project_settings())
            process.crawl(CountrySystemsSpider, sid=country.sid, country=country.name, session=session)
        process.start()
    finally:
        session.close()


def get_info_for_all_systems():
    """
    Runs crawler to information and location data for all 
    systems saved to the db.
    """
    try:
        session = load_session()
        systems = session.query(System).all()
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
            process = CrawlerProcess(get_project_settings())
            process.crawl(
                SystemLocationSpider,
                session=session,
                sid=sys_sid, 
                id=sys_id, 
                country_name=str(country_name),
            )
        process.start()
    finally:
        session.close()


def get_power_generation_info_for_all_systems():
    """
    Runs crawler to get power generation for all systems saved to the db.
    """
    try:
        session = load_session()
        systems = session.query(System).all()
        process = CrawlerProcess(get_project_settings())
        for sys in systems:
            sys_name, sys_id, sys_sid = sys.name, sys.id, sys.sid
            country = session.query(Country).filter_by(sid=sys.country_sid).first()
            country_name = country.name

            process.crawl(
                DailyPowerGenerationSpider, 
                session=session,
                sid=sys_sid, 
                id=sys_id, 
                country_name=str(country_name),
                system_name=sys_name
            )
            durations = ["weekly", "monthly", "yearly"]
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
