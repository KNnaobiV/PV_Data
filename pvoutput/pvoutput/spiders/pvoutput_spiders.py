import json
import os
import re
import shutil
from contextlib import suppress
from pprint import pprint

import scrapy
from scrapy.crawler import CrawlerProcess
import pandas as pd
import sys
cwd = os.getcwd()
path = os.path.dirname(os.path.join(cwd, "pv_scrapper", "src", "pvoutput", "pvoutput", "spiders"))
sys.path.append(path)
from definitions import SCRAPPER_ROOT_DIR
from items import *
from utils import get_or_create_dir

COUNTRIES_DIR = os.path.join(SCRAPPER_ROOT_DIR, "ouput", "countries")


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
            self.countries = {}
            self.countries[country.strip()] = country_id
        yield self.countries       


class CountrySystemsSpider(scrapy.Spider):
    """
    Gets all the system ids for a country
    """
    name = "country_systems_spider"

    def __init__(self, id=224, country="Switzerland"):
        self.id = id
        self.country = country.lower()
        self.systems = []

        self.country_dir = get_or_create_dir(
            SCRAPPER_ROOT_DIR, "output", "countries", self.country
        )

    def start_requests(self):
        yield scrapy.Request(f"https://pvoutput.org/ladder.jsp?f=1&country={self.id}")

    def parse(self, response):
        country_item = CountryItem()
        system_item = SystemItem()
        country = None
        table_rows = response.css(".e2, .o2")
        for row in table_rows:
            country = row.xpath(".//td[4]//text()").get()
            country = country.strip().lower()
            if country != self.country:
                continue
            tag = row.css("a:first-child")
            name = row.css("a:first-child::text").get()
            link = tag.xpath("@href").get()
            with suppress(AttributeError, IndexError):
                sid = re.search(r"sid=(\d+)", link).group(1)
                id = re.search(r"id=(\d+)", link).group(1)
                self.systems.append({
                    "country": self.country,
                    "name": name,
                    "sid": sid,
                    "id": id,
                })
        
        next_link = response.xpath("//a[contains(text(), 'Next')]")
        if next_link:
            next_href = next_link[0].attrib["href"]
            next_href = f"https://pvoutput.org/ladder.jsp{next_href}"
            yield scrapy.Request(next_href, self.parse)
        else:
            country_item["sid"] = self.id
            country_item["name"] = country
            systems_file = os.path.join(self.country_dir, "systems.csv")
            df = pd.DataFrame(self.systems)
            df.to_csv(systems_file)
            for system in self.systems:
                for name, ids in system.items():
                    system_item["country"] = country
                    system_item["name"] = name
                    system_item["id"] = ids["id"]
                    system_item["sid"] = ids["sid"]
                yield system_item


class SystemLocationSpider(scrapy.Spider):
    """
    Gets the country, name and location(latitude and longitude) of a 
    system.
    """
    name = "system_location_spider"
    def __init__(self, id, sid, country_name):
        self.sid = sid
        self.SYSTEM_DIR = get_or_create_dir(COUNTRIES_DIR, country_name)


    def start_requests(self):
        yield scrapy.Request(f"https://pvoutput.org/listmap.jsp?sid={self.sid}")


    def parse(self, response):
        item = LocationItem()
        location = {}
        p_tag = response.xpath("(//p[@class='nowrap'])[2]")
        system_name = response.xpath("//b[@class='large']//text()").get()
        country = p_tag.css("a:last-child::text").get()
        script_map_info = response.xpath(
            "//script[contains(., 'var mymap = L.map')]").get()
        location_info = re.search(
            r"center:\s*\[([\d.-]+),\s*([\d.-]+)\]", script_map_info
        )

        self.SYSTEM_DIR = get_or_create_dir(
            COUNTRY_DIR, country.lower(), system_name.lower()
        )

        if not location_info:
            return
        
        item["country"] = country
        item["name"] = system_name
        item["latitude"] = location_info.group(1)
        item["longitude"] = location_info.group(2)
        item["sid"] = self.sid

        location["country"] = country
        location["name"] = system_name
        location["latitude"] = location_info.group(1)
        location["longitude"] = location_info.group(2)
        latitude = location["latitude"]
        longitude = location["longitude"]
        
        df = pd.DataFrame(location, index=[1])
        save_file = os.path.join(self.SYSTEM_DIR, "location.csv")
        df.to_csv(save_file)
       
        # location = json.dumps(location)
        # yield {
        #     "system_sid": self.sid, 
        #     "latitude": latitude, 
        #     "longitude": longitude,
        # }
        yield item


class AggregatePowerGenerationSpider(scrapy.Spider):
    """
    Gets the monthly, weekly or yearly power generated by a system.
    """
    def __init__(self, id, sid, country_name, system_name, duration="monthly"):
        self.items = []
        self.id = id
        self.sid = sid
        self.country_name = country_name
        self.system_name = system_name
        self.SYSTEM_DIR = get_or_create_dir(
            COUNTRIES_DIR, self.country_name, self.system_name
        )
        duration_dict = {
            "weekly": "w",
            "monthly": "m",
            "yearly": "y",
        }
        self.duration = duration_dict[duration]
    
        if duration.lower() == "weekly":
            self.name = "weekly_power_generation_spider"
        elif duration.lower() == "monthly":
            self.name = "monthly_power_generation_spider"
        elif duration.lower() == "yearly":
            self.name = "yearly_power_generation_spider"
        else:
            raise ValueError("Duration can only be weekly, monthly or yearly")

    
    def start_requests(self):
        yield scrapy.Request(f"https://pvoutput.org/aggregate.jsp?id={self.id}&sid={self.sid}&t={self.duration}")
    

    def parse(self, response):
        item = PowerGeneratedItem()
        system_name = response.xpath("//b[@class='large']//text()").get()
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
            save_file = os.path.join(self.SYSTEM_DIR, f"{self.duration}.csv")
            df.to_csv(save_file)
            df_as_json = df.to_json()
            item["sid"] = self.sid
            item["id"] = self.id
            item["duration"] = self.duration
            item["power_generated_info"] = df_as_json
            # yield item
            # yield {"system_sid": self.sid, f"{duration.lower()}_df": df_as_json}


class DailyPowerGenerationSpider(scrapy.Spider):
    """
    Gets the daily power generated by a system.
    """
    def __init__(self, id, sid, country_name, system_name):
        self.items = []
        self.id = id
        self.sid = sid
        self.country_name = country_name
        self.system_name = system_name
        self.SYSTEM_DIR = get_or_create_dir(
            COUNTRIES_DIR, self.country_name, self.system_name
        )
    
    name = "daily_power_generation_spider"
    
    def start_requests(self):
        yield scrapy.Request(f"https://pvoutput.org/list.jsp?id={self.id}&sid={self.sid}")
        

    def parse(self, response):
        item = PowerGeneratedItem()
        system_name = response.xpath("//b[@class='large']//text()").get()
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
            save_file = os.path.join(self.SYSTEM_DIR, "daily.csv")
            df.to_csv(save_file)
            df_as_json = df.to_json()
            item["sid"] = self.sid
            item["id"] = self.id
            item["duration"] = "daily"
            item["power_generated_info"] = df_as_json
            yield item
            # yield {"system_sid": self.sid, "daily_df": df_as_json}


class SystemInfoSpider(scrapy.Spider):
    """
    Gets all information about a system.
    """
    def __init__(self, sid, country_name, system_name):
        self.sid = sid
        self.country_name = country_name
        self.system_name = system_name
        self.SYSTEM_DIR = get_or_create_dir(
            COUNTRIES_DIR, self.country_name, self.system_name
        )
        

    name = "system_info_spider"

    def start_requests(self):
        yield scrapy.Request(f"https://pvoutput.org/display.jsp?sid={self.sid}")
    # start_urls = [
    #     f"https://pvoutput.org/display.jsp?sid={self.sid}"
    # ]

    def parse(self, response):
        item = SystemInfoItem()
        info_div = response.css("div.corner")
        info_table = response.xpath("//table/tr")
        info = info_table.xpath("./td[2]/input/@value").extract()
        system_info = {
            "Name": info_div.css("b::text").extract(),
            "NumberOfPanels": info[0],
            "PanelMaxPower": info[1],
            "Size": info[2],
            "PanelBrand": info[3],
            "Orientation": info[4],
            "NumberOfInverters": info[5],
            "InverterBrand": info[6],
            "InverterSize": info[7],
            "Postcode": info[8],
            "InstallationDate": info[9],
            "Shading": info[10],
            "Tilt": info[11],
            "Comments": info[12],
        }
        
        df = pd.DataFrame.from_dict(system_info)
        save_file = os.path.join(self.SYSTEM_DIR, "info.csv")
        df.to_csv(save_file)
        item["sid"] = self.sid
        item["info"] = system_info
        # system_info = json.dump(system_info)
        # yield {"system_sid": self.sid, "info":system_info}
        yield item