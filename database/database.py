#!/usr/bin/env python3

import pandas as pd
from pymongo import MongoClient
import json
import os


def mongoimport(csv_path, db_name, coll_name, db_url):
    """ 
    Imports a csv file at path csv_name to a mongo colection
    returns: count of the documants in the new collection
    """
    # connect to database
    client = MongoClient(db_url)
    # get database object
    db = client[db_name]
    # get collection object
    coll = db[coll_name]
    # read excel file into panda
    data = pd.read_excel(csv_path)
    # jsonify data
    payload = json.loads(data.to_json(orient='records'))
    # remove previous collection
    coll.remove()
    # insert new data
    coll.insert(payload)
    # return number of documents inserted
    return coll.count()


if __name__=='__main__':
    # this code is for test
    csv_path = os.path.realpath(os.path.join(os.path.dirname(__file__), '..', 'data'))
    print(csv_path)
