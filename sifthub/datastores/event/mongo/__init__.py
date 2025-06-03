# MongoDB Package 
from sifthub.configs import mongo_configs
from sifthub.configs.mongo_configs import AUDIT_LOG_MONGO_DATABASE
from sifthub.datastores.event.mongo.client.mongoClient import MongoDBClient
from sifthub.datastores.event.mongo.report_audit_log_datastore import ReportAuditLogDataStore

mongo_datasource_url = mongo_configs.MONGO_DATASOURCE_URL.split("#")

__mongo_client = MongoDBClient(",".join(mongo_datasource_url))

__mongo_audit_log_db_client = __mongo_client.connect(AUDIT_LOG_MONGO_DATABASE)

audit_log_store = ReportAuditLogDataStore(__mongo_audit_log_db_client)