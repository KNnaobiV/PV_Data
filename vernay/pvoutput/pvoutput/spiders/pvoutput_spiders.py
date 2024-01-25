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
from items import *
from pipelines import DataPipeline
from vernay.utils import get_or_create_dir


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
    try:
        time = datetime.strptime(time, "%I:%M%p").time()
        return time
    except ValueError:
        return

def format_isocalendar(period):
    formatted_period = period + "-1"
    dt = datetime.strptime(formatted_period, "%Y-%W-%w")
    calendar = dt.isocalendar()
    return calendar


def month_str_to_dt(period):
    period = period.split(" ")
    period[0], period[1] = period[0] + " 1", " 20" + period[1]
    period = "".join(period)
    dt = datetime.strptime(period, "%b %d %Y")
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
    if energy_gen_rate.lower().endswith("kwh/kw"):
        return energy_gen_rate
    elif energy_gen_rate.lower().endswith("mwh/kw"):
        energy_gen_rate = f"{float(energy_gen_rate[:-6].replace(',', '')) * 1000}kWh/kW"
    elif energy_gen_rate.lower().endswith("wh/kw"):
        energy_gen_rate = f"{float(energy_gen_rate[:-5].replace(',', '')) / 100}kWh/kW"
    return energy_gen_rate



def format_power(power):
    if power.lower().endswith("w"):
        power = int(power[:-1])
    elif power.lower.endswith("kw"):
        power = int(power[:-2]) * 1000
    elif power.lower().endswith("mw"):
        power = int(power[:-2]) * 1000000
    return power


def format_tilt(tilt):
    return float(tilt[:-7])


def process_list(lst):
    if isinstance(lst[0], list):
        return process_list(lst[0])
    else:
        return lst[0]

    
def process_dict(dct):
    for key, val in dct.items():
        if isinstance(val, list):
            process_dict[key] = process_list(val)
        else:
            dct[key] = val
    return dct


class GetCountries(scrapy.Spider):
    """
    Crawls all countries and gets their name and sid
    """
    name = "get_countries_spider"
    start_urls = [
        f"https://pvoutput.org/ladder.jsp?f=1&country={i}" for i in range(257)
    ]

    def __init__(self, session):
        self.session = session

    def parse(self, response):
        item = CountryItem()
        with suppress(IndexError):
            url = response.request.url
            country_id = re.search(r"country=(\d+)", url)
            if country_id:
                country_id = country_id.group(1)
            else:
                return
            

            table_rows = response.css(".e2, .o2")
            country = table_rows[0].xpath("//td[4]//text()").get()
            if not country:
                country = f"country_{sid}"
            
            item["name"] = strip_whitespace(country)
            item["sid"] = country_id
        pipeline = DataPipeline(item, self.session)
        pipeline.process_item()


class CountrySystemsSpider(scrapy.Spider):
    """
    Gets all the system ids for a country
    """
    name = "country_systems_spider"
    
    @classmethod
    def update_settings(cls, settings):
        super().update_settings(settings)
        settings.set("TELNETCONSOLE_ENABLED", False, priority="spider")


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
        self.session = session


    @classmethod
    def update_settings(cls, settings):
        super().update_settings(settings)
        settings.set("TELNETCONSOLE_ENABLED", False, priority="spider")


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

        if not location_info:
            return
        
        item["latitude"] = location_info.group(1)
        item["longitude"] = location_info.group(2)
        item["sid"] = self.sid

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
                            item[key] = process_list(value)
                    else:
                        item[key] = value
                self.items_list.append(item)
            
        next_link = response.xpath("//a[contains(text(), 'Next')]")
        if next_link:
            next_href = next_link[0].attrib["href"]
            next_href = f"https://pvoutput.org/aggregate.jsp{next_href}"
            yield scrapy.Request(next_href, self.parse)
        else:
            for item in self.items_list:
                try:
                    period = get_period(item["period"])
    
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
                except (ValueError, TypeError):
                    continue
        
                self.item["sid"] = self.sid
                self.item["generated"] = format_energy_generated(item["generated"])
                self.item["efficiency"] = format_energy_gen_rate(item["efficiency"])
                self.item["exported"] = format_energy_generated(item["exported"])
                self.item["fit_credit"] = item["fit_credit"]
                self.item["low"] = format_energy_generated(item["low"])
                self.item["high"] = format_energy_generated(item["high"])
                self.item["average"] = format_energy_generated(item["average"])

                pipeline = DataPipeline(self.item, self.session)
                pipeline.process_item()


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
        self.session = session
    
    name = "daily_power_generation_spider"
    

    @classmethod
    def update_settings(cls, settings):
        super().update_settings(settings)
        settings.set("TELNETCONSOLE_ENABLED", False, priority="spider")

    
    def start_requests(self):
        yield scrapy.Request(f"https://pvoutput.org/list.jsp?id={self.id}&sid={self.sid}")
        

    def parse(self, response):
        system_name = response.xpath("//b[@class='large']//text()").get()
        table = response.xpath("//table[@id='tb']//tr")
        for index, tr in enumerate(table):
            if index >= 2:
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
            for item in self.items_list:
                self.item = DailyItem()
                try:
                    self.item["date"] = day_period_to_date(item["date"])
                except (ValueError, TypeError):
                    continue
                self.item["sid"] = self.sid
                self.item["generated"] = format_energy_generated(item["generated"])
                self.item["efficiency"] = format_energy_gen_rate(item["efficiency"])
                self.item["exported"] = format_energy_generated(item["exported"])
                self.item["peak_power"] = item["peak_power"]
                self.item["peak_time"] = format_time(item["peak_time"])
                self.item["conditions"] = item["conditions"]
                pipeline = DataPipeline(self.item, self.session)
                pipeline.process_item()


class SystemInfoSpider(scrapy.Spider):
    """
    Gets all information about a system.
    """
    def __init__(self, sid, country_name, system_name, session):
        self.sid = sid
        self.country_name = country_name
        self.system_name = system_name
        self.session = session

    name = "system_info_spider"

    @classmethod
    def update_settings(cls, settings):
        super().update_settings(settings)
        settings.set("TELNETCONSOLE_ENABLED", False, priority="spider")



    def start_requests(self):
        yield scrapy.Request(f"https://pvoutput.org/display.jsp?sid={self.sid}")


    def parse(self, response):
        item = SystemItem()
        info_div = response.css("div.corner")
        info_table = response.xpath("//table/tr")
        info = info_table.xpath("./td[2]/input/@value").extract()
        system_info = {
            "number_of_panels": info[0],
            "panel_max_power": format_power(info[1]),
            "size": format_power(info[2]),
            "panel_brand": info[3],
            "orientation": info[4],
            "number_of_inverters": info[5],
            "inverter_brand": info[6],
            "inverter_size": format_power(info[7]),
            "post_code": info[8],
            "installation_date": info[9],
            "shading": info[10],
            "tilt": format_tilt(info[11]),
            "comments": info[12],
        }
        
        for attr, val in system_info.items():
            item[attr] = val
        item["sid"] = self.sid
        pipeline = DataPipeline(item, self.session)
        pipeline.process_item()
        yield item