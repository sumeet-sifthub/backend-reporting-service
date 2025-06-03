from typing import TypeVar

import motor.motor_asyncio

R = TypeVar("R")


class MongoDBClient(object):
    def __init__(self, connection: str):
        self.mongo_client = motor.motor_asyncio.AsyncIOMotorClient(connection)

    def connect(self, database_name: str):
        return self.mongo_client.get_database(database_name)

    def disconnect(self):
        self.mongo_client.close() 