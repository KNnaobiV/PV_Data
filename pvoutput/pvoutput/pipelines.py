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
from models import db_connect


class PostgresPipeline:
    def __init__(self):
        engine = db_connect()
        Session = sessionmaker(bind=engine)
        self.session = Session()
        
    
    def add_country(self, country):
        """
        country: item
            country item
        """
        country = Country(
            name=country["name"],
            sid=country["sid"],
            id=country["id"],
        )

    
    def add_system(self, system):
        """
        system: item
            system item
        """
        system = System(
            name=system["name"],
            sid=system["sid"],
            id=system["id"],
            country=system["country"],
        )
        country = session.query(Country).filter_by(name=country.name)
        if country: #check if the country exists
            system.country = country
        else:
            system.country = item.country # country from the item to be passed
        
        
        self.session.add(system)
        self.session.commit()
        self.session.close()

    def add_system_location(self, system_info):
        """
        system_location: item
            system location
        """
        system_location = SystemLocation(
            name=system_location["name"],
            latitude=system_location["latitude"],
            longitude=system_location["longitude"],
            system=system_location["system"],    
        )
        self.session.add(system_location)
        self.session.commit()
        self.session.close()

    def process_item(self, item, spider):
        if isinstance(item, Country):
            self.add_country(item)
        elif isinstance(item, System):
            self.add_system(item)
        elif isinstance(item, SystemLocation):
            self.add_system_location(item)

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