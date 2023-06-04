"""
DO NOT MOVE THIS FILE
This is used to set the configuration file
"""
import os
import configparser

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

CONFIG_PATH = os.path.join(ROOT_DIR, "config.cfg")

if not os.path.join(ROOT_DIR, "outputs"):
    os.mkdir(os.path.join(ROOT_DIR, "outputs"))



cfg = configparser.ConfigParser()

cfg["PROJECT"] = {
    "root_dir": ROOT_DIR,
    "output_dir": os.path.join(ROOT_DIR, "outputs")
}

with open(os.path.join(ROOT_DIR, "config.cfg"), "w") as cfgfile:
    cfg.write(cfgfile)
