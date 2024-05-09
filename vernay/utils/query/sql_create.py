__all__ = ["create_item_with_sid", "get_or_create_item", "save_item"]

from vernay.utils.db import load_session


def create_item_with_sid(model, **kwargs):
    """
    Creates model instance using the kwargs provided.

    :param kwargs: dict
        keyword arguments representing field names and values.

    :return: model instance

    :raises KeyError: If sid is not in kwargs.
    """
    sid = kwargs.get("sid")
    if not sid:
        raise KeyError("'sid' not in kwargs.")
    return model(**kwargs)


def get_or_create_item(model, defaults=None, **kwargs):
    """
    Retrieves an item from the db by filtering the using the kwargs.
    Creates a new item if the item does not exist.

    :param defaults: dict, optional
        default values for creating new instances
    :param kwargs: dict
        filter conditions for querying the db.
    
    :return tuple:
        model instance and boolean. True if a new instance was created
        and False if it was retrieved from the db.
    """
    with load_session() as session:
        instance = session.query(model).filter_by(**kwargs).one_or_none()
        if instance:
            return instance, False
        else:
            kwargs |= defaults or {}
            instance = model(**kwargs)
            try:
                session.add(instance)
                session.commit()
            except Exception:
                session.rollback()
                instance = session.query(model).filter_by(**kwargs).one()
                return instance, False
            else:
                return instance, True


def save_item(item):
    """
    Saves the session item to the db.
    """
    with load_session() as session:
        model_item, _ = get_or_create_item(**item)
        for attr, val in item.items():
            setattr(model_item, attr, val)
        session.add(model_item)
        session.commit()
