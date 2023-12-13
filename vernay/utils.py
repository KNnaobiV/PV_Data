import configparser
import operator
import os

from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import NoResultFound
from settings import DATABASE_URL
from vernay.definitions import  ROOT_DIR
from vernay.pvoutput.pvoutput.settings import DATABASE_URL

# write query for retrieving item from db
__all__= [
    "get_or_create_dir", "get_engine", "load_session", "load_table",
]

def get_db_url():
    cfg =  configparser.ConfigParser()
    cfg.read(os.path.join(ROOT_DIR, "config.cfg"))
    username = cfg.get("DB", "user", fallback=None)
    password = cfg.get("DB", "pwd", fallback=None)
    dbname = cfg.get("DB", "dbname", fallback=None)
    db_url = f"postgresql://{username}:{password}@localhost/{dbname}"
    return db_url

def get_or_create_dir(base, *args):
    """
    Creates nested directories in the base directory depending on 
    additional arguments.

    :param base: str or path object
        the base directory of the new directory to be created.
    :param *args:
        optional arguments for nested directories to be created.

    :return dirname: str or path object
    """
    dirname = os.path.join(base)
    if not os.path.exists(dirname):
        raise NotADirectoryError(f"{base} is not a directory.")
    if args:
        for arg in args:
            try:
                dirname = os.path.join(dirname, arg.lower())
                if not os.path.exists(dirname):
                    os.mkdir(dirname)
            except PermissionError:
                raise PermissionError(
                    f"You do not have permissions to create a directory in folder {dirname}"
                    )
    return dirname
    

def get_engine():
    "Returns an instance of the database engine."
    db_url = get_db_url()
    return create_engine(db_url)


def load_session():
    """Returns an instance of the session."""
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    return session


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
