import asyncio
from typing import TypeVar

import motor.motor_asyncio

R = TypeVar("R")


class MongoClient(object):
    def __init__(self, connection: str):
        self.mongo_client = motor.motor_asyncio.AsyncIOMotorClient(connection)

    def connect(self, database_name: str):
        db = self.mongo_client.get_database(database_name)
        try:
            loop = asyncio.get_event_loop()
            # If there's a running loop, schedule the warm-up asynchronously
            loop.create_task(self._warm_up_connection(db))
        except RuntimeError:
            # No running loop, create a new one and run synchronously
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self._warm_up_connection(db))
        return db

    async def _warm_up_connection(self, db):
        """Asynchronous warm-up function to ensure the connection is ready."""
        await db.list_collection_names()  # Access metadata to warm up connection

    def disconnect(self):
        self.mongo_client.close() 