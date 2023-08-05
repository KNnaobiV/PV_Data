# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import configparser

import psycopg2
from itemadapter import ItemAdapter


class PvoutputPipeline:
    def process_item(self, item, spider):
        return item

class PostgresPipeline:
    def __init__(self, postgres_uri, postgres_db):
        self.cfg = configparser.ConfigParser()
        self.cfg.read_file("config.cfg")
        connection = psycopg2.connection(
            database=self.cfg.get("postgres", "db"),
            user=self.cfg.get("postgres", "user"),
            password=self.cfg.get("postgres", "pwd"),
            host=self.cfg.get("postgres", "host"),
            port=self.cfg.get("postgres", "port")
        )


class BaseCountryPipeline(PostgresPipeline):
    @classmethod
    def get_country_id(cls, country_name):
        cursor = connection.cursor()
        query = "SELECT id FROM country WHERE name= %s;"
        cursor.execute(query, (country_name,))
        country_id = cursor.fetchone()
        cursor.close()
        return country_id[0] if country_id else None

    
class BaseSystemPipeline(PostgresPipeline):
    @classmethod
    def get_system_id(cls, system_name):
        cursor = connection.cursor()
        query = "SELECT id FROM system WHERE name=%s;"
        cursor.execute(query, (system_name))
        system_id = cursor.fetchone()
        cursor.close()
        return system_id[0] if system_id else None


class GetCountriesPipeline(PostgresPipeline):
    def process_item(self, item, spider):
        cursor = connection.cursor()
        query = """
            INSERT INTO countries (name, id, sid)
            VALUES (%s, %s, %s)
        """
        for country in item:
            try:
                values = (
                    country["name"], country["id"], country["sid"]
                )
                cursor.execute(query, values)
                connection.commit()
                cursor.close()
            except Exception as e:
                raise


class CountrySystemsPipeline(BaseCountryPipeline):
    def process_item(self, item, spider):
        cursor = connection.cursor()
        query = """
            INSERT INTO countries (name, id, sid, country_id)
            VALUES (%s, %s, %s, %f)
        """
        for country in item:
            try:
                country_id = self.get_country_id(item["country"])
                values = (
                    country["name"], country["id"], 
                    country["sid"], country_id
                )
                cursor.execute(query, values)
                connection.commit()
                cursor.close()
            except Exception as e:
                raise


class SystemLocationPipeline(BaseCountryPipeline):
    def process_items(self, item, spider):
        cursor = connection.cursor()
        query = """
            INSERT INTO location (name, latitude, longitude)
            VALUES (%s, %s, %s)
        """
        country_id = self.get_country_id(item["country"])
        values = (item["name"], item["latitude"], item["longitude"])
        connection.execute(query, values)
        connection.commit()
        cursor.close()
        

class SystemInfoPipeline(BaseSystemPipeline):
    def process_item(self, item, spider):
        cursor = connection.cursor()
        query = """
            INSERT INTO system_info (
                number_of_panels, panel_max_power, size, 
                panel_brand_model, orientation, number_of_inverters,
                inverter_brand_model, inverter_size, postcode, 
                installation_date, shading, tilt, comments, system_id
                )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %d, %s, %s, %s, %s, %d)
        """
        system_id = self.get_system_id(item["Name"])
        values = (
            item["Number Of Panels"], item["Panel Max Power"],item["Size"], 
            item["Panel Brand"], item["Orientation"], 
            item["Number Of Inverters"], item["Inverter Brand"],
            item["Inverter Size"], item["Postcode"], item["Installation Date"],
            item["Shading"], item["Tilt"], item["Comments"], system_id
        )
        connection.execute(query, values)
        connection.commit()
        connection.close()


class DailyPowerGenerationPipeline(BaseSystemPipeline):
    def process_item(self, item, spider):
        cursor = connection.cursor()
        query = """
            INSERT INTO daily (df, system_id)
            VALUES (%j, %d)
        """
        system_id = self.get_system_id(item["name"])
        values = (item["df"], system_id)
        connection.execute(query, values)
        connection.commit()
        cursor.close()

    
class AggregatePowerGenerationPipeline(BaseSystemPipeline):
    def process_item(self, item, spider):
        cursor = connection.cursor()
        duration = spider.name[:-24]
        query = (
            f"""INSERT INTO duration_table {duration} (df, system_id)"""
            """VALUES (%j, %d)"""
        )
        system_id = self.get_system_id(item["name"])
        values = (item["df"], system_id)
        cursor.execute(query, values)
        cursor.commit()
        cursor.close()

    
    def open_spider(self, spider):
        cursor = connection.cursor()
        pass
    def close_spider(self, spider):
        connection.close()
    def process_country_data(self, item):
        cursor = connection.cursor()
        query = """
            INSERT INTO country (name, latitude, longitude)
            VALUES (%s, %s, %s)
            ON CONFLICT (name) DO NOTHING
        """
        VALUES = (
            item["country"], item["latitude"], item["longitude"]
        )
        cursor.execute(query, values)
        connection.commit()
        cursor.close()
  
    def process_system_data(self, item):
        cursor = connection.cursor()
        query = """
            INSERT INTO system_info (sid, country_id)
            VALUES (%s, %s);
        """
        values = (
            self.sid,
            self.get_country_id(item["country"]),
        )
        cursor.execute(query, values)
        connection.commit()
        cursor.close()

    def process_daily_power_generation_data(self, item):
        pass