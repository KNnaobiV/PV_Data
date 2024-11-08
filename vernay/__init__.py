from vernay.definitions import ROOT_DIR
from vernay.definitions import main as definitions

from vernay.pvoutput.pvoutput import process as scrapper

def scrape_all():
    scrapper.get_countries()
    scrapper.get_systems_in_all_countries()
    scrapper.get_info_for_all_systems()
    scrapper.get_power_generation_info_for_all_systems()

# scrape_country = scrapper.get_countries()
# scrape_systems = scrapper.get_systems_in_all_countries()
# scrape_system_info = scrapper.get_info_for_all_systems()
# scraper_power_generation = scrapper.get_power_generation_info_for_all_systems()