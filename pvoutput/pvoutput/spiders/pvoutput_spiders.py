import os
import re
import shutil
from contextlib import suppress

import scrapy
from ..definitions import SCRAPPER_ROOT_DIR

class CountrySpider(scrapy.Spider):
    """
    Gets all the system ids for a country
    """
    name = "country"

    # def __init__(self, id, country):
    #     self.id = id
    #     self.country = country
    
    # start_urls = [
    #     f"https://pvoutput.org/list.jsp?sid={self.id}"
    # ]

    start_urls = [
        "https://pvoutput.org/ladder.jsp?f=1&country=224"
    ]

    def parse(self, response):
        system_ids = []
        table_rows = response.css(".e2, .o2")
        for row in table_rows:
            country = row.xpath(".//td[4]//text()").get()
            if country.strip().lower() != "switzerland": #self.country:
                continue

            tag = row.css("a:first-child")
            link = tag.xpath("@href").get()
            system_id = re.search(r"sid=(\d+)", link)
            if system_id:
                system_ids.append(system_id.group(1))
        
        next_link = response.xpath("//a[contains(text(), 'Next')]")
        if next_link:
            next_href = next_link[0].attrib["href"]
            next_href = f"https://pvoutput.org/ladder.jsp{next_href}"
            yield scrapy.Request(next_href, self.parse)
        else:
            with open("out.txt", "w")as text_file:
                for system_id in system_ids:
                    text_file.write(system_id + "\n")


class GetCountries(scrapy.Spider):
    """
    Gets all countries and corresponding ids and appends them to 
    "static/countries.txt".
    """
    name = "get_countries"
    start_urls = [
        f"https://pvoutput.org/ladder.jsp?f=1&country={i}" for i in range(257)
    ]

    def __init__(self):
        if not os.path.exists(os.path.join(SCRAPPER_ROOT_DIR, "static")):
            os.mkdir(os.path.join(SCRAPPER_ROOT_DIR, "static"))
        self.out_file = os.path.join(SCRAPPER_ROOT_DIR, "static", "countries.txt")
        if os.path.exists(self.out_file):
            os.remove(self.out_file)
    
    def parse(self, response):
        # should I suppress or raise?
        with suppress(IndexError):
            url = response.request.url
            country_id = re.search(r"country=(\d+)", url)
            if country_id:
                country_id = country_id.group(1)

            table_rows = response.css(".e2, .o2")
            country = table_rows[0].xpath("//td[4]//text()").get()

            with open(self.out_file, "a") as out_file:
                out_file.write(f"{country.strip()}:{country_id}\n")


class SystemLocation(scrapy.Spider):
    # def __init__(self, url):
    #     self.url = url

    name = "system_location_spider"
    start_urls = [
        "https://pvoutput.org/listmap.jsp?sid=34873"
    ]
    
    def parse(self, response):
        location = {}
        script_map_info = response.xpath(
            "//script[contains(., 'var mymap = L.map')]").get()
        location_info = re.search(
            r"center:\s*\[([\d.-]+),\s*([\d.-]+)\]", script_map_info
        )
        if location_info:
            location["latitude"] = location_info.group(1)
            location["longitude"] = location_info.group(2)
        print(location)



class DaySpider(scrapy.Spider):
    def __init__(self, url):
        self.url = url

    name = "day"
    start_urls = [
        "https://pvoutput.org/ladder.jsp?f=1&country=257"
    ]

class HourSpider(scrapy.Spider):
    def __init__(self, url):
        self.url = url

    name = "hour"
    start_urls = [
        "https://pvoutput.org/ladder.jsp?f=1&country=257"
    ]
    
class MonthSpider(scrapy.Spider):
    def __init__(self, url):
        self.url = url

    name = "month"
    start_urls = [
        "https://pvoutput.org/ladder.jsp?f=1&country=257"
    ]


class YearSpider(scrapy.Spider):
    def __init__(self, url):
        self.url = url

    name = "year"
    start_urls = [
        "https://pvoutput.org/ladder.jsp?f=1&country=257"
    ]

class WeekSpider(scrapy.Spider):
    def __init__(self, url):
        self.url = url

    name = "week"
    start_urls = [
        "https://pvoutput.org/ladder.jsp?f=1&country=257"
    ]