import os

#Mongo Configs
MONGO_DATASOURCE_URL = os.environ.get("MONGO_DATASOURCE_URL",
                                  "mongodb://dev_admin:sifthub1@192.168.0.110:27017/")

AUDIT_LOG_MONGO_DATABASE = os.environ.get("AUDIT_LOG_MONGO_DATABASE", "auditlogs")