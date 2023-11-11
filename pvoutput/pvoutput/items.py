# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from scrapy.item import Item, Field
from scrapy.loader.processors import MapCompose, TakeFirst

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


class CountryItem(Item):
    sid = Field()
    name = Field()
    # system_ids = Field()

class SystemItem(Item):
    country = Field()
    name = Field()
    id = Field()
    sid = Field()

class LocationItem(Item):
    sid = Field()
    country = Field()
    latitude = Field()
    longitude = Field()
    name = Field()


class SystemInfoItem(Item):
    sid = Field()
    info = Field()


class DailyItem(Item):
    date = Field()
    generated = Field()
    efficiency = Field()
    exported = Field()
    peak_power = Field()
    peak_time = Field()
    conditions = Field()


class MonthlyItem(Item):
    period = Field()
    generated = Field()
    efficiency = Field()
    exported = Field()
    fit_credit = Field()
    low = Field()
    high = Field()
    average = Field()


class WeeklyItem(Item):
    period = Field()
    generated = Field()
    efficiency = Field()
    exported = Field()
    fit_credit = Field()
    low = Field()
    high = Field()
    average = Field()


class YearlyItem(Item):
    period = Field()
    generated = Field()
    efficiency = Field()
    exported = Field()
    fit_credit = Field()
    low = Field()
    high = Field()
    average = Field()