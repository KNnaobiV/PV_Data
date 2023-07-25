"""Scrapes Data from PVOUTPUT"""
import configparser
import os
from pathlib import Path
import requests
from bs4 import BeautifulSoup as bs
import pandas as pd

from definitions import CONFIG_PATH


def main():
    save_table_as_csv()

cfg = configparser.ConfigParser()
cfg.read(CONFIG_PATH)
OUTPUT_DIR = cfg.get("PROJECT", "output_dir")

#creating a set of webpages
w = [
    'https://pvoutput.org/aggregate.jsp?id=74611&sid=71729&v=0&t=m', 
    'https://pvoutput.org/list.jsp?id=51500&sid=46834', 
    'https://pvoutput.org/list.jsp?id=53465&sid=48697', 
    'https://pvoutput.org/list.jsp?id=53465&sid=48699', 
    'https://pvoutput.org/list.jsp?id=84471&sid=74894', 
    'https://pvoutput.org/list.jsp?id=84471&sid=77747', 
    'https://pvoutput.org/list.jsp?id=84471&sid=77748', 
    'https://pvoutput.org/list.jsp?id=53465&sid=48698', 
    'https://pvoutput.org/list.jsp?id=53465&sid=48702', 
    'https://pvoutput.org/list.jsp?id=53465&sid=48701'
]

 #create new folder: PVs
if not os.path.exists('PVs'):
    os.makedirs('PVs')

# downloading the contents of the webpage
for x in w:
    r = requests.get(x)

    # creating a beautiful soup object
    soup = bs(r.content, 'html5lib')
    
    # getting table
    table = soup.find('table', id = 'tb')

    # creating dataframe
    df_list = pd.read_html(str(table))

    # getting name
    name = soup.find('title')
    filename = (name.string + ".csv").replace(" ", "_")
    filename = filename.replace("|", "")
    for df in df_list:
        if not os.path.exists(os.path.join(OUTPUT_DIR, "PVs")):
            os.mkdir(os.path.join(OUTPUT_DIR, "PVs"))
        df.to_csv(os.path.join(OUTPUT_DIR, "PVs", filename))

def get_filename_from_title(soup):
    """Returns the name of the pv system,"""
    name = soup.find("title")
    filename = (name.string + ".csv").replace(" ", "_")
    filename = filename.replace("|", "")
    return filename

def df_to_csv(df, filename):
    """
    Saves the df as the specified filename in the output directory.
    
    Parameters
    ----------
    df: pd.DataFrame
    
    filename: str
        name with which the csv file is to be saved,
    """
    if not os.path.exists(os.path.join(OUTPUT_DIR, "PVs")):
        os.mkdir(os.path.join(OUTPUT_DIR, "PVs"))
    df.to_csv(os.path.join(OUTPUT_DIR, "PVs", filename))

def save_table_as_csv():
    """
    Reads the table in the html file and saves it as csv file.
    """
    counter = 0
    soup = bs(request.content, "html5lib")
    # maybe I should use a try except block instead
    # url should have a way of passing the page number using counter
    filename = get_filename_from_title(soup)

    while True:
        response = request.get("https://example.com") # add url variable
        if response.status_code == 200:
            table = soup.find("table", id="tb")
            if df:
                df.append(pd.read_html(str(table)))
            else:
                df = pd.read_html(str(table))
        else:
            break
        counter += 1
    df_to_csv(df, filename)