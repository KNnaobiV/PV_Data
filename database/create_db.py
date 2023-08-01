import configparser

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

cfg = configparser.ConfigParser()
cfg.read_dict("config.cfg")

user = cfg.get("CREDENTIALS", "user", fallback=None)
pwd = cfg.get("CREDENTIALS", "password", fallback=None)
