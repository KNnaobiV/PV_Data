import os

SCRAPPER_ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
def get_countries():
    country_text_file = os.path.join(SCRAPPER_ROOT_DIR, "static", "out.txt")
    with open(country_text_file, "r") as country_file:
        country_dict = {}
        lines = country_file.readlines()
        for line in lines:
            line = line.rstrip()
            country, id = line.split(":")[0], line.split(":")[1]
            country_dict[country] = id
    return country_dict