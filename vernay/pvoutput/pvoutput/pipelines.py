# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import logging

from sqlalchemy.orm import relationship, Session
from sqlalchemy.exc import NoResultFound, IntegrityError
import sys
sys.path.append(".")
from models import *
from items import *
from settings import DATABASE_URL

__all__ = [
    "DataPipeline",
]

class DataPipeline:
    def __init__(self, item):
        self.DB_URL = DATABASE_URL
        engine = db_connect(self.DB_URL)
        self.session = Session(bind=engine)
        create_tables(engine)
        models   = {
            "CountryItem": Country,
            "SystemItem": System,
            "LocationItem": Location,
            "SystemInfoItem": SystemInfo,
            "DailyItem": Daily,
            "WeeklyItem": Weekly,
            "MonthlyItem": Monthly,
            "YearlyItem": Yearly
        }
        items = (
            CountryItem,  
            SystemItem, 
            LocationItem, 
            DailyItem, 
            MonthlyItem, 
            WeeklyItem, 
            YearlyItem
        )
        if isinstance(item, items):
            self.model = models[item.__class__.__name__]

        self.item = item

    def create_item_with_sid(self, **kwargs):
        """
        Creates model instance using the kwargs provided.

        Parameters
        ----------
        kwargs: dict
            keyword arguments representing field names and values.

        Returns
        -------
        model instance

        Raises
        ------
        KeyError: If sid is not in kwargs.
        """
        sid = kwargs.get("sid")
        if not sid:
            raise KeyError("'sid' not in kwargs.")
        return self.model(**kwargs)


    def get_or_create_item(self, defaults=None, **kwargs):
        """
        Retrieves an item from the db by filtering the using the kwargs.
        Creates a new item if the item does not exist.

        Parameters
        ----------
        defaults: dict, optional
            default values for creating new instances
        kwargs: dict
            filter conditions for querying the db.
        
        Returns
        -------
        tuple:
            model instance and boolean. True if a new instance was created
            and False if it was retrieved from the db.
        """
        instance = self.session.query(self.model).filter_by(**kwargs).one_or_none()
        if instance:
            return instance, False
        else:
            kwargs |= defaults or {}
            instance = self.model(**kwargs)
            try:
                self.session.add(instance)
                self.session.commit()
            except Exception:
                self.session.rollback()
                instance = self.session.query(self.model).filter_by(**kwargs).one()
                return instance, False
            else:
                return instance, True
    

    def save_item(self):
        """
        Saves the session item to the db.
        """
        model_item, _ = self.get_or_create_item(**self.item)
        for attr, val in self.item.items():
            setattr(model_item, attr, val)
        self.session.add(model_item)
        self.session.commit()


    def process_item(self):
        try:
            item_id = self.item["sid"]
        except KeyError:
            item_id = self.item["id"]
        except KeyError:
            raise
        try:
            self.save_item()
        except:
            self.session.rollback()
            raise
        finally:
            self.session.close()
        return self.item
        