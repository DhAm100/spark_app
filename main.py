import uvicorn
import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routes import templates

import config
from spark import data
from database.database import MongoDatabase


config.parse_args()
app = FastAPI(
    title="Spark MongoDB API",
    description="Simple API for csv data manipulation using spark and MongoDB",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8080/", "http://localhost:8080"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(templates.router)


@app.get("/")
async def root():
    return {
        "docs": "api documentation at /docs or /redoc",
    }


if __name__ == "__main__":
    # csv path
    csv_path = os.path.realpath(os.path.join(os.path.dirname(__file__), 'data'))
    # get csv file
    csv_file = csv_path + '/' + config.CONFIG.file
    # insert csv data into mongodb
    #print('Data insertion in MongoDB database from {}... This might take some time'.format(csv_file))
    #mongo_object = MongoDatabase(config.CONFIG.database_connection, config.CONFIG.db, config.CONFIG.coll)
    #mongo_object.insert_data(csv_file)
    # instantiate spark object
    templates.router.sp = data.SparkConnector(config.CONFIG.collection_input)
    # run web server
    uvicorn.run("main:app", host=config.CONFIG.host, port=int(config.CONFIG.port))