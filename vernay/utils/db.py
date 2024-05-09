__all__= [
    "get_db_url",
    "get_engine", 
    "load_session", 
    "load_table"
]

import configparser
from contextlib import contextmanager
import operator
import os

from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.exc import NoResultFound
from vernay.definitions import  ROOT_DIR
from vernay.pvoutput.pvoutput.settings import DATABASE_URL

# write query for retrieving item from db


def get_db_url():
    cfg =  configparser.ConfigParser()
    cfg.read(os.path.join(ROOT_DIR, "config.cfg"))
    username = cfg.get("DB", "user", fallback=None)
    password = cfg.get("DB", "pwd", fallback=None)
    dbname = cfg.get("DB", "dbname", fallback=None)
    db_url = f"postgresql://{username}:{password}@localhost/{dbname}"
    return db_url
    

def get_engine():
    "Returns an instance of the database engine."
    db_url = get_db_url()
    return create_engine(db_url)


@contextmanager
def load_session():
    """Returns an instance of the session."""
    engine = get_engine()
    connection = engine.connect()
    session = scoped_session(
        sessionmaker(
            autocommit=False,
            autoflush=True,
            bind=engine,
    ))
    yield session
    session.close()
    connection.close()


def load_table(table_name):
    """
    Loads a db table
    
    :param table_name: string representation of the table
    :return table: table instance
    """
    metadata = MetaData()
    engine = get_engine()
    table = Table(table_name, metadata, autoload=True, autoload_with=engine)
    return table

