# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import configparser
import logging


from itemadapter import ItemAdapter
from sqlalchemy.orm import relationship, sessionmaker
import sys
sys.path.append(".")
from models import db_connect, Country, System
from items import *

def create_instance_with_sid(model, **kwargs):
    return model(**kwargs)


def get_one_or_create(
    session,
    model,
    create_method="",
    create_method_kwargs=None,
    **kwargs):
    try:
        return session.query(model).filter_by(**kwargs).one(), False
    except NoResultFound:
        kwargs.update(create_method_kwargs or {})
        created = getattr(model, create_method, model)(**kwargs)
        try:
            session.add(created)
            session.flush()
            return created, True
        except IntegrityError:
            session.rollback()
            return session.query(model).filter_by(**kwargs).one(), False

class PostgresPipeline:
    def __init__(self):
        engine = db_connect()
        Session = sessionmaker(bind=engine)
        self.session = Session()
    
    
    def add_country(self, country_item):
        """
        Parameters
        ----------
        country_item: item
            Item containing information about countries.
        """
        country, created = get_one_or_create(
            session, 
            Country, 
            sid=power_gen_info["sid"]
        )
        country.id = country_item["id"]
        country.name = country_item["name"]
        country.sid = country_item["sid"]
        

    def add_location_info(self, location_info):
        """
        Parameters
        ----------
        location_info: item
            Item containing detailed information on the system.
        """
        system, created = get_one_or_create(
            session, 
            System, 
            sid=power_gen_info["id"]
        )
        system.country = location_info["country"]
        system.latitude = location_info["latitude"]
        system.longitude = location_info["longitude"]
        system.name = location_info["name"]
        system.sid = location_info["sid"]

        country = session.query(Country).filter_by(name=country.name)
        if country: #check if the country exists
            system.country = country
        else:
            system.country = item.country


    def add_power_generaton_info(self, power_gen_info):
        """
        Parameters
        ----------
        power_gen_info: item
            Item containing information on the system's power generation.
        """
        system, created = get_one_or_create(
            session, 
            System, 
            sid=power_gen_info["id"]
        )
        duration = power_gen_info["duration"]
        system.id = power_gen_info["id"]
        system.sid = power_gen_info["sid"]
        info = power_gen_info["power_generated_info"]
        setattr(system, duration, info)

    def add_system_info(self, system_item):
        """
        Parameters
        ----------
        system_item: item
            Item containing basic information on the system.
        """
        system, created = get_one_or_create(
            session, 
            System, 
            sid=power_gen_info["id"]
        )
        system.info = system_item["info"]
        system.sid = system_item["sid"]


    def process_item(self, item, spider):
        if isinstance(item, CountryItem):
            self.add_country(item)
        elif isinstance(item, LocationItem):
            self.add_location_info(item)
        elif isinstance(item, PowerGeneratedItem):
            self.add_power_generated_info(item)
        elif isinstance(item, SystemInfoItem):
            self.add_system_info(item)
        try:
            session.add(item)
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()
        return item










    def insert_row(self, table, cols):
        """
        Creates a new row in a table.
        
        Parameters
        ----------
        table: str
            Table name
        cols: dict
            Dictionary of column names and values
        """
        cursor = self.connection.cursor()
        columns = ", ".join(values.keys())
        val_placeholder = ", ".join(['%s'] * len(cols))
        query = f"INSERT INTO {table} ({columns}) VALUES ({val_placeholder});"
        try:
            cursor.execute(query, list(cols.values()))
            connection.commit()
        except psycopg2.Error as e:
            raise
        cursor.close()
        connection.close()


    def row_exists(self, table, pk):
        """
        Checks if a row exists in a table using primary keys.
        
        Parameters
        ----------
        table: str
            Table name
        pk: str or int
            The rows primary key
        
        Returns
        -------
        bool
        """
        cursor = self.connection.cursor()
        query = f"SELECT * FROM {table} WHERE pk = {pk};"
        cursor.execute(query)
        row = cursor.fetchone()

        if existing_row:
            cursor.close()
            return True
    

    def update_cell_by_pk(self, table, col, value, pk):
        """
        Update specified cell in the table by PK.

        Parameters
        ----------
        table: str
            Name of the table to update.
        col: str
            Name of the column to update.
        value: str
            New value to be set for the cell.
        pk: int
            PrimaryKey of the row to be updated.
        """
        cursor = self.connection.cursor()
        update = f"UPDATE {table} SET {col} = %s WHERE pk = %s;"
        try:
            cursor.execute(update, (values, pk))
            connection.commit()
        except psycopg2.Error as e:
            print(e)
        cursor.close()
        self.connection.close()


class BaseSystemPipeline(PostgresPipeline):
    def __init__(self):
        super().__init__()
        cols = {
            
            "daily": "",
            "location": "",
            "id": "VARCHAR(7)",
            "info": "",
            "monthly": "",
            "name": "VARCHAR(50)",
            "sid": "VARCHAR(7)",
            "weekly": "",
            "yearly": "",
            "country_id": "INT FOREIGN KEY REFERENCES Country(Country_pk)"
        }
        self.create_table(name="System", cols=cols)

    @classmethod
    def get_system_id(cls, system_name):
        cursor = self.connection.cursor()
        query = "SELECT id FROM system WHERE name=%s;"
        cursor.execute(query, (system_name))
        system_id = cursor.fetchone()
        cursor.close()
        self.connection.close()
        return system_id[0] if system_id else None


class BaseCountryPipeline(PostgresPipeline):
    def __init__(self):
        super().__init__()
        cols = {
            "pk": "SERIAL PRIMARY KEY",
            "id": "VARCHAR(7)",
            "sid": "VARCHAR(7)",
            "name": "VARCHAR(75) NOT NULL",
        }
        self.create_table(country="Country", cols=cols)

    @classmethod
    def get_country_id(cls, country_name):
        cursor = self.connection.cursor()
        query = "SELECT id FROM country WHERE name= %s;"
        cursor.execute(query, (country_name,))
        country_id = cursor.fetchone()
        cursor.close()
        self.connection.close()
        return country_id[0] if country_id else None

    def process_item(self, item, spider):
        duration = spider.name[:-24]
        if self.row_exists(table="System", pk="name"):
            self.update_cell_by_pk("System", duration, item["df"], "pk")
        else:
            cols = {"duration": duration, "df": items["df"]}
            self.insert_row("System", cols)
        """cursor = self.connection.cursor()
        query = (
            INSERT INTO System {duration} (df, system_id)
            VALUES (%s, %s)
        )
        system_id = self.get_system_id(item["name"])
        values = (item["df"], system_id)
        cursor.execute(query, values)
        cursor.commit()
        cursor.close()
        self.connection.close()"""