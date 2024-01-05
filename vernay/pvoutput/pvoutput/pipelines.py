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
            SystemInfoItem, 
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
        self.session.commit()
        # return self.model(**kwargs)


    def get_item_by_sid(self, sid):
        return self.session.query(self.model).filter_by(sid=sid).one_or_none()


    def get_or_create_item(self, defaults=None, **kwargs):
        sid = kwargs.get("sid")
        instance = self.get_item_by_sid(sid)
        instance = self.session.query(self.model).filter_by(**kwargs).one_or_none()
        if instance:
            return instance, False
        else:
            kwargs |= defaults or {}
            instance = self.create_item_with_sid(**kwargs)
            try:
                self.session.add(instance)
                self.session.commit()
            except Exception:
                self.session.rollback()
                # instance = self.session.query(self.model).filter_by(**kwargs).one()
                return instance, False
            else:
                return instance, True
    def update_item(self, **kwargs):
        sid = kwargs.pop("sid")
        instance = self.get_item_by_sid(sid)
        for attr, val in self.item.items():
            setattr(instance, attr, val)
        self.session.commit()


    def save_item(self, **kwargs):
        sid = kwargs.get("sid")
        instance = self.session.query(self.model).filter_by(sid=sid).first() # why can't I do a get by sid?
        if instance:
            self.update_item(**kwargs)
        else:
            self.create_item(*kwargs)


    def old_save_item(self):
        model_item, _ = self.get_or_create_item(**self.item)
        for attr, val in self.item.items():
            setattr(model_item, attr, val)
        self.session.add(model_item)
        self.session.commit()
        # create item if it doesn't exist, else update


    def process_item(self):
        try:
            self.save_item()
        except:
            self.session.rollback()
            raise
        return self.item
        