# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from contextlib import suppress
import logging
import pdb

from sqlalchemy.orm import relationship, Session
from sqlalchemy.exc import NoResultFound, IntegrityError
import sys
sys.path.append(".")
from models import *
from items import *
from vernay.utils import get_engine, load_session, save_item

__all__ = [
    "DataPipeline",
]

class DataPipeline:
    def __init__(self, item, session):
        engine = get_engine()
        self.session = session
        create_tables(engine)
        models   = {
            "CountryItem": Country,
            "SystemItem": System,
            "DailyItem": Daily,
            "WeeklyItem": Weekly,
            "MonthlyItem": Monthly,
            "YearlyItem": Yearly
        }
        items = (
            CountryItem, 
            SystemItem, 
            DailyItem, 
            WeeklyItem, 
            MonthlyItem, 
            YearlyItem
        )
        if isinstance(item, items):
            self.model = models[item.__class__.__name__]

        self.item = item


    def create_item(self, **kwargs):
        sid = kwargs.get("sid")
        if not sid:
            raise KeyError("'sid' not in kwargs.")
        new_item = self.model(**kwargs)
        self.session.add(new_item)


    def update_item(self, instance, **kwargs):
        sid = kwargs.pop("sid")
        for attr, val in kwargs.items():
            setattr(instance, attr, val)
            self.session.flush()


    def save_item(self):
        sid = self.item.get("sid", None)
        with suppress(KeyError):
            instance = self.session.query(self.model).filter_by(sid=sid).first() # why can't I do a get by sid?
            if instance:
                self.update_item(instance, **self.item)
            else:
                self.create_item(**self.item)
            self.session.commit()
            print(f"saved {self.item}")


    def process_item(self):
        # with suppress(IntegrityError):
        try:
            self.save_item()
        except:
            self.session.rollback()
            raise
        return self.item
        