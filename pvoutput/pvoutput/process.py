from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from .definitions import get_countries

from spiders.pvoutput_spiders import (
    DailyPowerGenerationSpider, AggregatePowerGenerationSpider,
    CountrySpider, SystemInfoSpider, SystemLocationSpider
)

DURATIONS = ["month", "week", "year"]

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
    process.crawl(CountrySpider, id=country_id, country=country)
    process.start()


def get_system_info(id, sid, durations):
    """
    Runs the crawler to get information on a system.

    Parameters
    ----------
    id: str
        system id
    sid: str
        system sid
    durations: list
        List durations. Durations be daily, weekly, monthly or
    """
    process = CrawlerProcess(get_project_settings())
    pipeline = SystemPipeline(id, sid)
    process.configure()
    process.crawl(SystemInfoSpider, id=id, sid=sid, )
    process.crawl(SystemLocationSpider, sid=sid)
    duration_dict = {
        "weekly": "w",
        "monthly": "m",
        "yearly": "y",
    }
    for duration in durations:
        if duration not in DURATIONS:
            raise ValueError(
                f"Cannot select duration {duration}."
                " Duration can either be daily, weekly, monthly, or yearly."
            )
        if duration.lower() == "daily":
            process.crawl(DailyPowerGenerationSpider(id=id, sid=sid))
        else:
            duration_val = duration_dict[duration]
            process.crawl(
                AggregatePowerGenerationSpider(
                    id=id, sid=sid, duration=duration_val
                )
            )
    process.start()
