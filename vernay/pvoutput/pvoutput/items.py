# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from scrapy.item import Item, Field
from scrapy.loader.processors import MapCompose, TakeFirst # deprecated fix

__all__ = [
    "CountryItem",
    "SystemItem",
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
    latitude = Field()
    longitude = Field()
    number_of_panels= Field()
    panel_max_power = Field()
    size = Field()
    panel_brand = Field()
    orientation = Field()
    number_of_inverters = Field()
    inverter_brand = Field()
    inverter_size = Field()
    post_code = Field()
    installation_date = Field()
    shading = Field()
    tilt = Field()
    comments = Field()


class DailyItem(Item):
    sid = Field()
    date = Field()
    generated = Field()
    efficiency = Field()
    exported = Field()
    peak_power = Field()
    peak_time = Field()
    conditions = Field(input_processor=strip_whitespace)


class MonthlyItem(Item):
    sid = Field()
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
    sid = Field()
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
    sid = Field()
    year = Field()
    generated = Field()
    efficiency = Field()
    exported = Field()
    fit_credit = Field()
    low = Field()
    high = Field()
    average = Field()
