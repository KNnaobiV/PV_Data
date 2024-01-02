# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from scrapy.item import Item, Field
from scrapy.loader.processors import MapCompose, TakeFirst # deprecated fix

__all__ = [
    "CountryItem",
    "LocationItem",
    "SystemItem",
    "SystemInfoItem",
    "DailyItem",
    "MonthlyItem",
    "WeeklyItem",
    "YearlyItem"
]

def strip_whitespace(value):
    if isinstance(value, str):
        return value.strip()

class CountryItem(Item):
    sid = Field()
    name = Field(input_processor=strip_whitespace)


class SystemItem(Item):
    country_sid = Field()
    name = Field(input_processor=strip_whitespace)
    id = Field()
    sid = Field()


class LocationItem(Item):
    system_sid = Field()
    latitude = Field()
    longitude = Field()


class SystemInfoItem(Item):
    sid = Field()
    info = Field()


class DailyItem(Item):
    system_sid = Field()
    date = Field()
    generated = Field()
    efficiency = Field()
    exported = Field()
    peak_power = Field()
    peak_time = Field()
    conditions = Field(input_processor=strip_whitespace)


class MonthlyItem(Item):
    system_sid = Field()
    year = Field()
    month = Field()
    generated = Field()
    efficiency = Field()
    exported = Field()
    fit_credit = Field()
    low = Field()
    high = Field()
    average = Field()


class WeeklyItem(Item):
    system_sid = Field()
    year = Field()
    week = Field()
    generated = Field()
    efficiency = Field()
    exported = Field()
    fit_credit = Field()
    low = Field()
    high = Field()
    average = Field()


class YearlyItem(Item):
    system_sid = Field()
    year = Field()
    generated = Field()
    efficiency = Field()
    exported = Field()
    fit_credit = Field()
    low = Field()
    high = Field()
    average = Field()
