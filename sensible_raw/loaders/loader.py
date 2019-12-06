import json
import os

from monary import Monary
import pandas


def load_config(config_path=None):
    if not config_path:
        config_path = os.path.expanduser("~") + "/.sensible_raw/loader_config.json"

    return json.loads(open(config_path, "r").read())


def get_index(index_name, raw_value):
    config = load_config()
    columns, data = load_from_db("indices", index_name, ["index"], ["int32"], config["db_host"],
                                 query_spec={"raw_value": raw_value})
    if len(data) == 0 or len(data[0]) == 0:
        return -1

    return data[0][0]


def docs(data_type):
    pandas.set_option('display.max_colwidth', -1)
    documentation_folder = os.path.abspath(__file__ + "/../../")
    return pandas.read_csv(os.path.join(documentation_folder, "documentation_files", data_type + ".csv"))


def get_raw_value(index_name, indexed_value):
    config = load_config()
    columns, data = load_from_db("indices", index_name, ["raw_value"], ["string:100"], config["db_host"],
                                 query_spec={"index": indexed_value})
    if len(data) == 0 or len(data[0]) == 0:
        return -1

    return data[0][0]


def load_data(data_type, month, config=None, as_dataframe=True):
    """Main function for loading data.
    
    Input
    -----
        data_type : str
            Shuold be one of the following:
                    'bluetooth',
                    'calllog',
                    'cell',
                    'context',
                    'facebook_friends',
                    'grades',
                    'indices',
                    'local',
                    'location',
                    'location_raw',
                    'questionnaires',
                    'screen',
                    'sms',
                    'stop_locations',
                    'transport_minute',
                    'transport_segment',
                    'user_metadata',
                    'vectors',
                    'weather'
        
        month : str
            Month of year. Example value: "january_2014". Typically, you want to query months between
            September 2013 and January 2016. Refer to https://ulfaslak.com/files/sensible_dtu_data.png
            for overview of data volume for each month.
        
        config : bool/None-type
            Whether to provide a config file or not.
            
        as_dataframe : bool
            Whether the return the data as a `pandas.DataFrame` object.
    """
    if not config:
        config = load_config()
    columns, data = load_from_db(data_type,
                                 month,
                                 config["data_types"][data_type]["field_names"],
                                 config["data_types"][data_type]["field_types"],
                                 config["db_host"])

    if not as_dataframe:
        return columns, data
    else:
        dict = {}
        for column, array in zip(columns, data):
            dict[column] = array
        return pandas.DataFrame(dict)


def load_from_db(db, collection, field_names, field_types, db_host, query_spec={}):
    with Monary(host=db_host["hostname"], username=db_host["username"], password=db_host["password"],
                database="admin") as monary:
        arrays = monary.query(
            db,  # database name
            collection,  # collection name
            query_spec,  # query spec
            field_names,  # field names (in Mongo record)
            field_types  # Monary field types (see below)
        )

    return field_names, arrays
