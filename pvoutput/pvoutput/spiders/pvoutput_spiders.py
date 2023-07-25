import scrapy

class CountrySpider(scrapy.Spider):
    name = "month"
    start_urls = [
        "https://pvoutput.org/ladder.jsp?f=1&country=257"
    ]

    def __init__(self, url):
        self.url = url


    #get all the urls and check if the country is the same with the expected

    def parse_


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