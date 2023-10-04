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
    "PowerGeneratedItem"
]


class CountryItem(Item):
    sid = Field()
    name = Field()
    system_ids = Field()


class LocationItem(Item):
    sid = Field()
    country = Field()
    latitude = Field()
    longitude = Field()
    name = Field()


class SystemInfoItem(Item):
    sid = Field()
    info = Field()


class SystemItem(Item):
    name = Field()
    id = Field()
    sid = Field()


class PowerGeneratedItem(Item):
    sid = Field()
    id =  Field()
    duration = Field()
    power_generated_info = Field()