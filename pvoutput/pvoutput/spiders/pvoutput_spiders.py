import re

import scrapy

class CountrySpider(scrapy.Spider):
    name = "country"
    start_urls = [
        #f"https://pvoutput.org/ladder.jsp?f=1&country={self.id}"
        "https://pvoutput.org/ladder.jsp?f=1&country=257"
    ]

    def __init__(self, url=None, id=None):
        self.url = url
        self.id = id


    #get all the urls and check if the country is the same with the expected

    def parse(self, response):
        table_rows = response.css(".e2, .o2")
        sids = []
        for row in table_rows:
            tag = row.css("a:first-child")
            link = tag.xpath("@href").get()
            result = re.search(r"sid=(\d+)", link)
            if result:
                result = result.group(1)

class GetCountries(scrapy.Spider):
    name = "state"
    start_urls = [
        f"https://pvoutput.org/ladder.jsp?f=1&country={i}" for i in range(257)
    ]

    def parse(self, response):
        try:
            url = response.request.url
            country_id = re.search(r"country=(\d+)", url)
            if country_id:
                country_id = country_id.group(1)

            table_rows = response.css(".e2, .o2")
            country = table_rows[0].xpath("//td[4]//text()").get()
            print(f"{country.strip()}:{country_id}")
        except IndexError:
            raise

class WeekSpider(scrapy.Spider):
    def __init__(self, url):
        self.url = url

    name = "month"
    start_urls = [
        "https://pvoutput.org/ladder.jsp?f=1&country=257"
    ]
    

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