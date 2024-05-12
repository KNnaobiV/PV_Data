# Vernay Project Overview

## Introduction
The Vernay project consists of two main components: a data collector and a prediction model. The data collector is a prerequisite for running the prediction model, and it is responsible for gathering data related to photovoltaic (PV) system power generation. This data can be obtained either as CSV files or stored directly in an SQL database.

## Data Collection Process
### CSV Files
- Before collecting data, run `definitions.py` to set up project definitions, configurations, and the output directory.
- To collect data in CSV format, run the command `vernay.scrape_all()`.

### SQL Database
- Define the database connection parameters in `config.cfg`.
- To collect data directly into an SQL database, follow the configuration steps and then run `vernay.scrape_all()`.


## Data Prediction Model