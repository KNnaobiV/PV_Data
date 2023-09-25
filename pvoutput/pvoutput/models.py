import configparser
import logging

from sqlalchemy import (
    create_engine, Column, ForeignKey, Integer, JSON, String, DateTime,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

from scrapy.utils.project import get_project_settings

# add logger

Base = declarative_base()

DATABASE_URL = "postgresql://username:password@localhost/dbname"

engine = create_engine(DATABASE_URL)

def db_connect():
    """
    Perform database connection using db settins from settings.py.

    Returns
    -------
    engine: sqlalchemy engine instance
    """
    engine = create_engine(get_project_settings()).get("CONNECTION_STRING")
    return engine


def create_table(engine):
    Base.metadata.create_all(engine)


class Country(Base):
    __tablename__ = "country"

    sid = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    systems = Column(Integer, ForeignKey("country.sid"), nullable=False)


class System(Base):
    __tablename__ = "system"

    sid = Column(Integer, primary_key=True)
    id = Column(Integer)
    info = relationship(JSON)
    daily = Column(JSON)
    weekly = Column(JSON)
    monthly = Column(JSON)
    yearly = Column(JSON)
    latitude = Column(String)
    longitude = Column(String)
    country = Column(String, nullable=False)

Base.metadata.create_all(engine)