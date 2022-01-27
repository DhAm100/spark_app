#!/usr/bin/env python3

import pandas as pd
from pymongo import MongoClient
import json
import os


class MongoDatabase(object):
    """
    MongoDB object for managing insert into database
    """
    def __init__(self, db_url, db_name, coll_name):
        # connect to database
        self.client = MongoClient(db_url)
        # get database object
        self.db = self.client[db_name]
        # get collection object
        self.coll = self.db[coll_name]

    def read_file(self, csv_path):
        # read excel file into panda
        data = pd.read_excel(csv_path)
        # jsonify data
        payload = json.loads(data.to_json(orient='records'))

        return payload

    def insert_data(self, csv_path):
        # get payload
        payload = self.read_file(csv_path)
        # remove previous collection
        self.coll.remove()
        # insert new data
        return self.coll.insert(payload)
        


if __name__=='__main__':
    # this code is for test
    csv_path = os.path.realpath(os.path.join(os.path.dirname(__file__), '..', 'data'))
    print(csv_path)
