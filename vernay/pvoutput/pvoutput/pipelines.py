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


    def create_item_with_sid(self, **kwargs):
        sid = kwargs.get("sid")
        if not sid:
            raise KeyError("'sid' not in kwargs.")
        return self.model(**kwargs)

    def filter_by_sid(self, sid):
        return self.session.query(self.model).filter_by(sid=sid).one_or_none()
        
    def get_or_create_item(self, defaults=None, **kwargs):
        sid = kwargs.get("sid")
        instance = self.filter_by_sid(sid)
        # instance = self.session.query(self.model).filter_by(**kwargs).one_or_none()
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
                # instance = self.session.query(self.model).filter_by(**kwargs).one()
                return instance, False
            else:
                return instance, True
    
    def update_item(self, **kwargs):
        sid = kwargs.get("sid")
        instance = self.filter_by_sid(sid)
        try:
            # update instance logic
            self.session.add(instance)
            self.session.commit()
        except Exception:
            self.session.rollback()

    def save_item(self):
        model_item, _ = self.get_or_create_item(**self.item)
        for attr, val in self.item.items():
            setattr(model_item, attr, val)
        self.session.add(model_item)
        self.session.commit()


    def process_item(self):
        try:
            self.save_item()
        except:
            self.session.rollback()
            raise
        return self.item
        