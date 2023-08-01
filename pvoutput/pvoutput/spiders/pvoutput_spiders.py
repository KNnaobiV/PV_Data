import os
import re
import shutil
from contextlib import suppress

import scrapy
import pandas as pd
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


class AggregatePowerGenerationSpider(scrapy.Spider):
    name = "aggregate_power_generation_spider"
    start_urls = [
        # "https://pvoutput.org/aggregate.jsp?id=38078&sid=34873&v=0&t=w",
        "https://pvoutput.org/aggregate.jsp?id=38078&sid=34873&v=0&t=m",
        # "https://pvoutput.org/aggregate.jsp?id=38078&sid=34873&v=0&t=y"
    ]

    def __init__(self):
        self.items = []

    def parse(self, response):
        table = response.xpath("//table[@id='tb']//tr")
        for index, tr in enumerate(table):
            if index >= 2:
                item = {
                    "Month": tr.xpath("td[1]//text()").extract(),
                    "Generated": tr.xpath("td[2]//text()").extract(),
                    "Efficiency": tr.xpath("td[3]//text()").extract(),
                    "Exported": tr.xpath("td[4]//text()").extract(),
                    "FIT Credit": tr.xpath("td[5]//text()").extract(),
                    "Low": tr.xpath("td[6]//text()").extract(),
                    "High": tr.xpath("td[7]//text()").extract(),
                    "Average": tr.xpath("td[8]//text()").extract(),
                    "Comments": tr.xpath("td[9]//text()").extract(),
                }
                self.items.append(item)
            
        next_link = response.xpath("//a[contains(text(), 'Next')]")
        if next_link:
            next_href = next_link[0].attrib["href"]
            next_href = f"https://pvoutput.org/aggregate.jsp{next_href}"
            yield scrapy.Request(next_href, self.parse)
        else:
            df = pd.DataFrame(
                self.items, 
                columns=[
                    "Month", "Generated", "Efficiency", "Exported", 
                    "FIT Credit", "Low", "High", "Average", "Comments"
                ]
            )
            print(df)


class DailyPowerGenerationSpider(scrapy.Spider):
    name = "daily_power_generation_spider"
    start_urls = [
        "https://pvoutput.org/list.jsp?id=38078&sid=34873&v=0",
    ]
    
    def __init__(self):
        self.items = []

    def parse(self, response):
        table = response.xpath("//table[@id='tb']//tr")
        for index, tr in enumerate(table):
            if index >= 2:
                item = {
                    "Date": tr.xpath("td[1]//text()").extract(),
                    "Generated": tr.xpath("td[2]//text()").extract(),
                    "Efficiency": tr.xpath("td[3]//text()").extract(),
                    "Exported": tr.xpath("td[4]//text()").extract(),
                    "Peak Power": tr.xpath("td[5]//text()").extract(),
                    "Peak Time": tr.xpath("td[6]//text()").extract(),
                    "Conditions": tr.xpath("td[7]//text()").extract(),
                    "Temperature": tr.xpath("td[8]//text()").extract(),
                    "Comments": tr.xpath("td[9]//text()").extract(),
                }
                self.items.append(item)
            
        next_link = response.xpath("//a[contains(text(), 'Next')]")
        if next_link:
            next_href = next_link[0].attrib["href"]
            next_href = f"https://pvoutput.org/list.jsp{next_href}"
            yield scrapy.Request(next_href, self.parse)
        else:
            df = pd.DataFrame(
                self.items, 
                columns=[
                    "Date", "Generated", "Efficiency", "Exported", 
                    "Peak Power", "Peak Time", "Conditions", "Tempertature", 
                    "Comments"
                ]
            )
            print(df)

class SystemInfoSpider(scrapy.Spider):
    # def __init__(self, url):
    #     self.url = url

    name = "system_info_spider"
    start_urls = [
        "https://pvoutput.org/display.jsp?sid=34873"
    ]

    def parse(self, response):
        info_div = response.css("div.corner")
        info_table = response.xpath("//table/tr")
        info = info_table.xpath("./td[2]/input/@value").extract()
        system_info = {
            "Name": info_div.css("b::text").extract(),
            "Number Of Panels": info[0],
            "Panel Max Power": info[1],
            "Size": info[2],
            "Panel Brand/Model": info[3],
            "Orientation": info[4],
            "Number Of Inverters": info[5],
            "Inverter Brand/Model": info[6],
            "Inverter Size": info[7],
            "Inverter Model": info[8],
            "Installation Date": info[9],
            "Shading": info[10],
            "Tilt": info[11],
            "Comments": info[12],
        }
        print(system_info)


# Get system information, store the info and dfs with the system name, adding the system country