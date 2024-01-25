__all__= [
    "filter_table", 
    "get_db_url", 
    "get_id_from_name",
    "get_item_by_id",
    "get_objects_by_filter", 
    "get_operator_filter_list",
    "get_objects_by_operator_filter",
    "load_table"
]

import configparser
import operator
import os

from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import NoResultFound
from vernay.pvoutput.pvoutput.settings import DATABASE_URL

# write query for retrieving item from db


def get_db_url():
    cfg =  configparser.ConfigParser()
    cfg.read("config.cfg")
    username = cfg.get("DB", "user", fallback=None)
    password = cfg.get("DB", "pwd", fallback=None)
    dbname = cfg.get("DB", "dbname", fallback=None)
    db_url = f"postgresql://{username}:{password}@localhost/{dbname}"
    return db_url


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


def filter_table(table_name, column_name, **kwargs):
    """
    Filters a table for according to values given in kwargs dict

    :param table_name: string name of table to be filtered
    :param column_name: string name of table column on which filter 
        will be applied
    :param **kwargs: dictionary of filter arguments and values
    """
    table = load_table(table_name)
    try:
        query = table.select().where(getattr(table.columns, column_name) == kwargs["value"])
        session = load_session()
        return session.execute(query).fetchall()
    except Exception:
        return None


def get_operator_filter_param(column, operation, value):
    """
    Returns an operation using python's operator module based on the 
    provided parameters.

    :param column: The column on which the operation is to be executed.
    :param operation: The operation/method in python's operator module 
        as a string.
    :param value: The value to be used in the operation.

    :return: An operation using python's operator module based on the 
        provided parameters.
    """
    try:
        operation = getattr(operator, operation)
        return operation(column, value)
    except AttributeError:
        return


def get_operator_filter_list(column, **kwargs):
    """
    Generates a list of filter conditions using python's operator module.

    :param column: The column on which the operations are to be executed.
    :param kwargs: Dictionary of filter conditions. Keys are methods in 
        python's operator module as strings.

    :return: A list of filter conditions using python's operator module.
    """
    filter_list = list()
    for operation, value in kwargs.items():
        filter_list.append(get_operator_filter_param(column, operation, value))
    return filter_list


def get_objects_by_filter(session, model, **kwargs):
    """
    Returns model filtered by items in kwargs.

    :param session: session instance.
    :param model: model to be filtered.
    :param kwargs: dictionary of filter conditions. Keys are column names
        of the specified model which the should be filtered.

    :return objects: filtered query of the model.
    """
    objects = session.query(model).filter_by(**kwargs).all()
    return objects


def get_objects_by_operator_filter(session, model, column, **kwargs):
    """
    Gets model items filtered by the conditions in kwargs.
    .. note::

        It is suited for handling filtering of queries based on 
        operations specified in the python's operator module.

    :param session: session instance.
    :param model: model to be filtered.
    :param column: column on which the operation is to be executed.
    :param kwargs: dictionary of filter conditions. Specified keys
        are methods in python's operator module as `str`

    :return objects: filtered query of the model.
    """
    filter_list = get_operator_filter_list(column, **kwargs)
    objects = session.query(model).filter(*filter_list).all()
    return objects


def get_item_by_id(session, model, id):
    """
    Retrieves a single item from the database by its unique identifier.

    :param session: Session instance.
    :param model: Model representing the database table.
    :param id: Unique identifier of the item to retrieve.

    :return: The item corresponding to the provided ID, or None if not found.
    """
    return session.query(model).get(id)


def get_id_from_name(session, model, name):
    query_filter = {"name": name}
    query_object = get_objects_by_filter(session, model, **query_filter)
    if len(query_object) > 1:
        raise ValueError
    query_object = query_object[0]
    if query_object.sid:
        return query_object.sid
    elif query_object.id:
        return query_object.id


def get_class_by_tablename(fullname):
    """
    Returns class reference mapped to table.

    :param fullname: string with full name of table.
    :return: Class reference or None
    """
    for c in Base.registry._class_registry.data.values():
        if hasattr(c, "__table__") and c.__table__.fullname == fullname:
            return c