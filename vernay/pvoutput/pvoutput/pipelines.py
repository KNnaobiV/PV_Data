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
from pvoutput.pvoutput.models import *
from pvoutput.pvoutput.items import *
# from models import *
# from items import *
from utils import *
# from vernay.utils import get_engine, load_session, save_item

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
        self.filter_columns = self.get_filter_columns()


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

    def get_filter_columns(self):
        filter_columns, extra_filters = ["sid"], None
        class_name = self.item.__class__.__name__
        if class_name == "YearlyItem":
            extra_filters = ["year"]
        elif class_name == "MonthlyItem":
            extra_filters = ["year", "month"]
        elif class_name == "WeeklyItem":
            extra_filters = ["year", "week"]
        elif class_name == "DailyItem":
            extra_filters = ["date"]
        if extra_filters:
            filter_columns.extend(extra_filters)
        return filter_columns


    def save_item(self):
        filter_dict = {}
        for field in self.filter_columns:
            filter_dict[field] = self.item.get(field, None)
        with suppress(KeyError):
            instance = self.session.query(self.model).filter_by(**filter_dict).first() # why can't I do a get by sid?
            if instance:
                self.update_item(instance, **self.item)
                print(f"updated {self.item}")
            else:
                self.create_item(**self.item)
                print(f"created {self.item}")
            self.session.commit()
            


    def process_item(self):
        try:
            self.save_item()
        except:
            self.session.rollback()
            raise
        return self.item
        