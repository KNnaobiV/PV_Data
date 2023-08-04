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
    collection_name = "system_items"

    def __init__(self, postgres_uri, postgres_db):
       self.cfg = configparser.ConfigParser()
       self.cfg.read_file("config.cfg")

    def open_spider(self, spider):
        self.connection = psycopg2.connection(
            database=self.cfg.get("postgres", "db"),
            user=self.cfg.get("postgres", "user"),
            password=self.cfg.get("postgres", "pwd"),
            host=self.cfg.get("postgres", "host"),
            port=self.cfg.get("postgres", "port")
        )
        self.cursor = self.connection.cursor()

    def close_spider(self, spider):
        self.connection.close()

    def process_item(self, item, spider):
        table_name = item.table_name
        item_data = dict(item)

        keys = ", ".join(item_data.keys())
        values = ", ".join(item_data.values())
        insert_query = f"INSERT INTO {table_name} ({keys}) VALUES ({values})"

        try:
            self.cursor.execute(insert_query, list(item_data.values()))
            self.connection.commit()
        except Exception as e:
                self.connection.rollback()
                raise e

        return item
