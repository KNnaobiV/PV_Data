from __future__ import absolute_import
from datetime import datetime
import json
import os
import re
import shutil
from contextlib import suppress

import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.loader import ItemLoader
import pandas as pd
import sys
cwd = os.getcwd()
path = os.path.dirname(os.path.join(cwd, "pv_scrapper", "src", "pvoutput", "pvoutput", "spiders"))
sys.path.append(path)
# from definitions import SCRAPPER_ROOT_DIR
from items import *
from pipelines import DataPipeline
from vernay.utils import get_or_create_dir

# COUNTRIES_DIR = os.path.join(SCRAPPER_ROOT_DIR, "output", "countries")

def strip_whitespace(value):
    if isinstance(value, str):
        return value.strip()


def get_period(period):
    if isinstance(period, list):
            period = period[0]
    return period


def day_period_to_date(period):
    period = get_period(period)
    period = datetime.strptime(period, "%d/%m/%y")
    return period.date()


def format_time(time):
    time = datetime.strptime(time, "%I:%M%p").time()
    return time

def format_isocalendar(period):
    formatted_period = period + "-1"
    dt = datetime.datetime.strptime(formatted_period, "%Y-%W-%w")
    calendar = dt.isocalendar()
    return calendar


def month_str_to_dt(period):
    period = period.split(" ")
    period[0], period[1] = period[0] + " 1", " 20" + period[1]
    period = "".join(period)
    dt = datetime.datetime.strptime(period, "%b %d %Y")
    return dt

def format_energy_generated(energy_gen):
    if energy_gen.endswith("kWh"):
        return energy_gen
    elif energy_gen.endswith("MWh"):
        energy_gen = f"{float(energy_gen[:-3].replace(',', '')) * 1000}kWh"
    elif energy_gen.endswith("Wh"):
        energy_gen = f"{float(energy_gen[:-2].replace(',', '')) / 100}kWh"
    return energy_gen


def format_energy_gen_rate(energy_gen_rate):
    if energy_gen_rate.endswith("kWh/kW"):
        return energy_gen_rate
    elif energy_gen_rate.endswith("MWh/kW"):
        energy_gen_rate = f"{float(energy_gen_rate[:-6].replace(',', '')) * 1000}kWh/kW"
    elif energy_gen_rate.endswith("Wh/kW"):
        energy_gen_rate = f"{float(energy_gen_rate[:-5].replace(',', '')) / 100}kWh/kW"
    return energy_gen_rate


class GetCountries(scrapy.Spider):
    """
    Crawls all countries and gets their name and sid
    """
    name = "get_countries_spider"
    start_urls = [
        f"https://pvoutput.org/ladder.jsp?f=1&country={i}" for i in range(257)
    ]

    def parse(self, response):
        # loader = ItemLoader(item=CountryItem(), response=response)
        item = CountryItem()
        with suppress(IndexError):
            url = response.request.url
            country_id = re.search(r"country=(\d+)", url)
            if country_id:
                country_id = country_id.group(1)
                # loader.add_value("sid", country_id)
            

            table_rows = response.css(".e2, .o2")
            country = table_rows[0].xpath("//td[4]//text()").get()
            if not country:
                country = f"country_{sid}"
            
            item["name"] = strip_whitespace(country)
            item["sid"] = country_id
        pipeline = DataPipeline(item)
        pipeline.process_item()
            # loader.add_value("name", country)
        # yield loader.load_item()


class CountrySystemsSpider(scrapy.Spider):
    """
    Gets all the system ids for a country
    """
    name = "country_systems_spider"

    def __init__(self, sid, country, session):
        self.country_sid = sid
        self.country_name = country.lower()
        self.systems = []
        self.session = session


    def start_requests(self):
        yield scrapy.Request(f"https://pvoutput.org/ladder.jsp?f=1&country={self.country_sid}")

    def parse(self, response):
        system_item = SystemItem()
        country = None
        table_rows = response.css(".e2, .o2")
        for row in table_rows:
            country = row.xpath(".//td[4]//text()").get()
            country = country.strip().lower()
            if country != self.country_name:
                continue
            tag = row.css("a:first-child")
            name = row.css("a:first-child::text").get()
            link = tag.xpath("@href").get()
            with suppress(AttributeError, IndexError):
                sid = re.search(r"sid=(\d+)", link).group(1)
                id = re.search(r"id=(\d+)", link).group(1)
                self.systems.append({
                    "country": self.country_name,
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
            # systems_file = os.path.join(self.country_dir, "systems.csv")
            # df = pd.DataFrame(self.systems)
            # df.to_csv(systems_file)
            for system in self.systems:
                if not system["sid"]:
                    continue
                system_item["country_sid"] = self.country_sid
                system_item["name"] = strip_whitespace(system["name"])
                system_item["id"] = system["id"]
                system_item["sid"] = system["sid"]
                pipeline = DataPipeline(system_item, self.session)
                pipeline.process_item()
                yield system_item


class SystemLocationSpider(scrapy.Spider):
    """
    Gets the country, name and location(latitude and longitude) of a 
    system.
    """
    name = "system_location_spider"
    def __init__(self, id, sid, country_name, session):
        self.sid = sid
        # self.SYSTEM_DIR = get_or_create_dir(COUNTRIES_DIR, country_name)
        self.session = session


    # @classmethod
    # def update_settings(cls, settings):
    #     super().update_settings(settings)
    #     settings.set("TELNETCONSOLE_ENABLED", False, priority="spider")


    def start_requests(self):
        yield scrapy.Request(f"https://pvoutput.org/listmap.jsp?sid={self.sid}")


    def parse(self, response):
        item = SystemItem()
        # location = {}
        p_tag = response.xpath("(//p[@class='nowrap'])[2]")
        system_name = response.xpath("//b[@class='large']//text()").get()
        country = p_tag.css("a:last-child::text").get()
        script_map_info = response.xpath(
            "//script[contains(., 'var mymap = L.map')]").get()
        location_info = re.search(
            r"center:\s*\[([\d.-]+),\s*([\d.-]+)\]", script_map_info
        )

        # self.SYSTEM_DIR = get_or_create_dir(
        #     COUNTRY_DIR, country.lower(), system_name.lower()
        # )

        if not location_info:
            return
        
        item["latitude"] = location_info.group(1)
        item["longitude"] = location_info.group(2)
        item["sid"] = self.sid

        # location["country"] = country
        # location["name"] = system_name
        # location["latitude"] = location_info.group(1)
        # location["longitude"] = location_info.group(2)
        # latitude = location["latitude"]
        # longitude = location["longitude"]
        
        # df = pd.DataFrame(location, index=[1])
        # save_file = os.path.join(self.SYSTEM_DIR, "location.csv")
        # df.to_csv(save_file)
       
        # location = json.dumps(location)
        # yield {
        #     "system_sid": self.sid, 
        #     "latitude": latitude, 
        #     "longitude": longitude,
        # }
        pipeline = DataPipeline(item, self.session)
        pipeline.process_item()
        yield item


class AggregatePowerGenerationSpider(scrapy.Spider):
    """
    Gets the monthly, weekly or yearly power generated by a system.
    """
    def __init__(self, id, sid, country_name, system_name, duration, session):
        self.items_list = []
        self.id = id
        self.sid = sid
        self.country_name = country_name
        self.system_name = system_name
        self.session = session
        # self.SYSTEM_DIR = get_or_create_dir(
        #     COUNTRIES_DIR, self.country_name, self.system_name
        # )
        duration_dict = {
            "weekly": "w",
            "monthly": "m",
            "yearly": "y",
        }
        self.duration = duration_dict[duration]
    
        if duration.lower() == "weekly":
            self.name = "weekly_power_generation_spider"
            self.item = WeeklyItem()
        elif duration.lower() == "monthly":
            self.name = "monthly_power_generation_spider"
            self.item = MonthlyItem()
        elif duration.lower() == "yearly":
            self.name = "yearly_power_generation_spider"
            self.item = YearlyItem()
        else:
            raise ValueError("Duration can only be weekly, monthly or yearly")

    
    @classmethod
    def update_settings(cls, settings):
        super().update_settings(settings)
        settings.set("TELNETCONSOLE_ENABLED", False, priority="spider")


    def start_requests(self):
        yield scrapy.Request(f"https://pvoutput.org/aggregate.jsp?id={self.id}&sid={self.sid}&t={self.duration}")
    

    def parse(self, response):
        system_name = response.xpath("//b[@class='large']//text()").get()
        table = response.xpath("//table[@id='tb']//tr")
        for index, tr in enumerate(table):
            if index >= 2:
                s = tr.xpath("td[2]//text()").extract()
                item = {
                    "period": tr.xpath("td[1]//text()").extract(),
                    "generated": tr.xpath("td[2]//text()").extract(),
                    "efficiency": tr.xpath("td[3]//text()").extract(),
                    "exported": tr.xpath("td[4]//text()").extract(),
                    "fit_credit": tr.xpath("td[5]//text()").extract(),
                    "low": tr.xpath("td[6]//text()").extract(),
                    "high": tr.xpath("td[7]//text()").extract(),
                    "average": tr.xpath("td[8]//text()").extract(),
                    "comments": tr.xpath("td[9]//text()").extract(),
                }
                for key, value in item.items():
                    if isinstance(value, list):
                        if value:
                            item[key] = value[0]
                self.items_list.append(item)
            
        next_link = response.xpath("//a[contains(text(), 'Next')]")
        if next_link:
            next_href = next_link[0].attrib["href"]
            next_href = f"https://pvoutput.org/aggregate.jsp{next_href}"
            yield scrapy.Request(next_href, self.parse)
        else:
            # df = pd.DataFrame(
            #     self.items_list, 
            #     columns=[
            #         "Month", "Generated", "Efficiency", "Exported", 
            #         "FIT Credit", "Low", "High", "Average", "Comments"
            #     ]
            # )
            # save_file = os.path.join(self.SYSTEM_DIR, f"{self.duration}.csv")
            # # df.to_csv(save_file)
            for item in self.items_list:
                try:
                    period = get_period(item["period"])
                except (ValueError, TypeError):
                    continue
                if isinstance(self.item, YearlyItem):
                    self.item["year"] = datetime.strptime(period, "%Y").year
                elif isinstance(self.item, MonthlyItem):
                    dt = month_str_to_dt(period)
                    self.item["month"] = dt.month
                    self.item["year"] = dt.year
                elif isinstance(self.item, WeeklyItem):
                    calendar = format_isocalendar(period)
                    self.item["week"] = calendar.week
                    self.item["year"] = calendar.year
        
                self.item["system_sid"] = self.sid
                self.item["generated"] = format_energy_generated(item["generated"])
                self.item["efficiency"] = format_energy_gen_rate(item["efficiency"])
                self.item["exported"] = format_energy_generated(item["exported"])
                self.item["fit_credit"] = item["fit_credit"]
                self.item["low"] = format_energy_generated(item["low"])
                self.item["high"] = format_energy_generated(item["high"])
                self.item["average"] = format_energy_generated(item["average"])

            pipeline = DataPipeline(self.item, self.session)
            pipeline.process_item()
            # yield item
            # yield {"system_sid": self.sid, f"{duration.lower()}_df": df_as_json}


class DailyPowerGenerationSpider(scrapy.Spider):
    """
    Gets the daily power generated by a system.
    """
    def __init__(self, id, sid, country_name, system_name, session):
        self.items_list = []
        self.id = id
        self.sid = sid
        self.country_name = country_name
        self.system_name = system_name
        # self.SYSTEM_DIR = get_or_create_dir(
        #     COUNTRIES_DIR, self.country_name, self.system_name
        # )
        self.session = session
    
    name = "daily_power_generation_spider"
    

    @classmethod
    def update_settings(cls, settings):
        super().update_settings(settings)
        settings.set("TELNETCONSOLE_ENABLED", False, priority="spider")

    
    def start_requests(self):
        yield scrapy.Request(f"https://pvoutput.org/list.jsp?id={self.id}&sid={self.sid}")
        

    def parse(self, response):
        self.item = DailyItem()
        system_name = response.xpath("//b[@class='large']//text()").get()
        table = response.xpath("//table[@id='tb']//tr")
        for index, tr in enumerate(table):
            if index >= 2:
                a = tr.xpath("td[2]//text()").extract()
                item = {
                    "date": tr.xpath("td[1]//text()").extract(),
                    "generated": tr.xpath("td[2]//text()").extract(),
                    "efficiency": tr.xpath("td[3]//text()").extract(),
                    "exported": tr.xpath("td[4]//text()").extract(),
                    "peak_power": tr.xpath("td[5]//text()").extract(),
                    "peak_time": tr.xpath("td[6]//text()").extract(),
                    "conditions": tr.xpath("td[7]//text()").extract(),
                    "temperature": tr.xpath("td[8]//text()").extract(),
                    "comments": tr.xpath("td[9]//text()").extract(),
                }
                for key, value in item.items():
                    if isinstance(value, list):
                        if value:
                            item[key] = value[0]
                self.items_list.append(item)
            
        next_link = response.xpath("//a[contains(text(), 'Next')]")
        if next_link:
            next_href = next_link[0].attrib["href"]
            next_href = f"https://pvoutput.org/list.jsp{next_href}"
            yield scrapy.Request(next_href, self.parse)
        else:
            # df = pd.DataFrame(
            #     self.items_list, 
            #     columns=[
            #         "Date", "Generated", "Efficiency", "Exported", 
            #         "Peak Power", "Peak Time", "Conditions", "Tempertature", 
            #         "Comments"
            #     ]
            # )
            # save_file = os.path.join(self.SYSTEM_DIR, "daily.csv")
            # df.to_csv(save_file)
            for item in self.items_list:
                try:
                    self.item["date"] = day_period_to_date(item["date"])
                except (ValueError, TypeError):
                    continue
                self.item["system_sid"] = self.sid
                self.item["generated"] = format_energy_generated(item["generated"])
                self.item["efficiency"] = format_energy_gen_rate(item["efficiency"])
                self.item["exported"] = format_energy_generated(item["exported"])
                self.item["peak_power"] = item["peak_power"]
                self.item["peak_time"] = format_time(item["peak_time"])
                self.item["conditions"] = item["conditions"]
            pipeline = DataPipeline(self.item, self.session)
            pipeline.process_item()
            yield item
            # yield {"system_sid": self.sid, "daily_df": df_as_json}


class SystemInfoSpider(scrapy.Spider):
    """
    Gets all information about a system.
    """
    def __init__(self, sid, country_name, system_name, session):
        self.sid = sid
        self.country_name = country_name
        self.system_name = system_name
        # self.SYSTEM_DIR = get_or_create_dir(
        #     COUNTRIES_DIR, self.country_name, self.system_name
        # )
        self.session = session

    name = "system_info_spider"

    @classmethod
    def update_settings(cls, settings):
        super().update_settings(settings)
        settings.set("TELNETCONSOLE_ENABLED", False, priority="spider")



    def start_requests(self):
        yield scrapy.Request(f"https://pvoutput.org/display.jsp?sid={self.sid}")
    # start_urls = [
    #     f"https://pvoutput.org/display.jsp?sid={self.sid}"
    # ]

    def parse(self, response):
        item = SystemItem()
        info_div = response.css("div.corner")
        info_table = response.xpath("//table/tr")
        info = info_table.xpath("./td[2]/input/@value").extract()
        system_info = {
            # "name": info_div.css("b::text").extract(),
            "number_of_panels": info[0],
            "panel_max_power": info[1],
            "size": info[2],
            "panel_brand": info[3],
            "orientation": info[4],
            "number_of_inverters": info[5],
            "inverter_brand": info[6],
            "inverter_size": info[7],
            "post_code": info[8],
            "installation_date": info[9],
            "shading": info[10],
            "tilt": info[11],
            "comments": info[12],
        }
        
        df = pd.DataFrame.from_dict(system_info)
        save_file = os.path.join(self.SYSTEM_DIR, "info.csv")
        # df.to_csv(save_file)
        for attr, val in system_info.items():
            setattr(item, attr, val)
        item["sid"] = self.sid
        # system_info = json.dump(system_info)
        # yield {"system_sid": self.sid, "info":system_info}
        pipeline = DataPipeline(item, self.session)
        pipeline.process_item()
        yield item