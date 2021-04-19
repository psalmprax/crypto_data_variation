import datetime
import json

import numpy as np
import pandas as pd
import requests
from sqlalchemy import create_engine
from utilsfile import set_logger

LOGGER = set_logger("crypto_class_logger")


class ApiCrypto:

    def get_json_api(self, page):
        """
        getting the data from the url
        :param page:
        :type page:
        :return:
        :rtype:
        """
        get_request = requests.get(page)
        assert get_request.status_code == 200, "Request not successful"
        return get_request.json(), get_request.status_code

    @staticmethod
    def store_api_data(data=None, file_location=None):
        """
        storing the data to a json file on the local repository
        :param data:
        :type data:
        :param file_location:
        :type file_location:
        """
        try:
            with open(file_location, 'w') as outfile:
                json.dump(data, outfile)
        except Exception as e:
            LOGGER.info(f"Exception: {e}")

    @staticmethod
    def save_json(filename=None, table_name=None, conn=None):
        """
        loading and saving the dataset to the database(postgres)
        :param filename:
        :type filename:
        :param table_name:
        :type table_name:
        :param conn:
        :type conn:
        :return:
        :rtype:
        """
        LOGGER = set_logger("save_json_logger")

        try:
            destination_query = f""" insert into {table_name} select * from json_populate_recordset(NULL::{table_name}, %s) """
            destination = create_engine(conn)
            destination_conn = destination.connect()
            records = destination_conn.execute(destination_query, (json.dumps(json.load(open(filename, ))),))
            return destination
        except Exception as e:
            LOGGER.info(f"Exception: {e}")

    @staticmethod
    def load_save_db_api_data_variation(file_location=None, tablename=None, engine=None):
        """
        loading , calculating the price volatility and storing the result into the database(postgres)
        :param file_location:
        :type file_location:
        :param tablename:
        :type tablename:
        :param engine:
        :type engine:
        """
        try:
            data = pd.json_normalize(json.load(open(file_location)))
            data.time_period_start = data.time_period_start.apply(
                lambda x: datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.%f0000Z'))
            data.index = pd.to_datetime(data.time_period_start, unit='s')
            d_df = data.resample('D', closed='left', label='left').mean().copy()
            d_df['log_price'] = np.log(d_df.price_close)
            d_df['log_return'] = d_df['log_price'] - d_df['log_price'].shift(1)
            d_df['squared_log_return'] = np.power(d_df['log_return'], 2)

            six_min_df = pd.DataFrame(data.loc[:, ['price_close', 'volume_traded']], index=data.index) \
                .resample('6T', closed='left', label='left').mean().copy()
            six_min_df['log_price'] = np.log(six_min_df.price_close)
            six_min_df['log_return'] = six_min_df['log_price'] - six_min_df['log_price'].shift(1)
            six_min_df['squared_log_return'] = np.power(six_min_df['log_return'], 2)

            d_df['realized_variance_6min'] = pd.Series(six_min_df.loc[:, 'squared_log_return'], index=data.index)\
                .resample('D', closed='left', label='left').sum().copy()
            d_df['realized_volatility_6min'] = np.sqrt(d_df['realized_variance_6min'])
            d_df.to_sql(tablename, engine, if_exists='replace', index=False)

        except Exception as e:
            LOGGER.info(f"Exception: {e}")
