import os

#Mongo Configs
MONGO_DATASOURCE_URL = os.environ.get("MONGO_DATASOURCE_URL",
                                  "mongodb://dev_admin:sifthub1@192.168.0.110:27017/")

AUDIT_LOG_MONGO_DATABASE = os.environ.get("AUDIT_LOG_MONGO_DATABASE", "auditlogs")

# Reporting Service MongoDB Configs
MONGODB_CONNECTION_STRING = os.environ.get("MONGODB_CONNECTION_STRING", MONGO_DATASOURCE_URL)
MONGODB_DATABASE_NAME = os.environ.get("MONGODB_DATABASE_NAME", AUDIT_LOG_MONGO_DATABASE)