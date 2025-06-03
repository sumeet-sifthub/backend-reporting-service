from bson import ObjectId
from fastapi.encoders import jsonable_encoder
from motor.motor_asyncio import AsyncIOMotorDatabase
from typing import Optional
from datetime import datetime

from sifthub.reporting.models.export_models import ReportAuditLog, ExportStatus
from sifthub.utils.logger import setup_logger

logger = setup_logger()


class ReportAuditLogDataStore:
    def __init__(self, client: AsyncIOMotorDatabase):
        self.collection = client.get_collection("report_audit_log")

    async def update_status(self, event_id: str, client_id: str, status: ExportStatus, 
                           total_time: Optional[int] = None, s3_bucket: Optional[str] = None, 
                           download_url: Optional[str] = None) -> bool:
        try:
            update_data = {
                "status": status.value,
                "updated_at": datetime.utcnow()
            }
            if total_time is not None:
                update_data["total_time"] = total_time
            if s3_bucket:
                update_data["s3_bucket"] = s3_bucket
            if download_url:
                update_data["download_url"] = download_url
            logger.info(f"Updating Report Audit Log status to {status.value} for event_id: {event_id}")
            result = await self.collection.update_one(
                {"event_id": event_id, "client_id": client_id}, 
                {"$set": update_data}
            )
            return result.modified_count > 0
        except Exception as ex:
            logger.error(f"Error while updating report audit log: {ex}", exc_info=True)
            return False

    async def find_by_event_id(self, event_id: str, client_id: str) -> Optional[ReportAuditLog]:
        try:
            log_dict = await self.collection.find_one({"event_id": event_id, "client_id": client_id})
            if log_dict:
                # Remove MongoDB's _id field for Pydantic model
                log_dict.pop("_id", None)
                return ReportAuditLog(**log_dict)
            return None
        except Exception as ex:
            logger.error(f"Error while fetching report audit log: {ex}", exc_info=True)
            return None

    async def find_by_event_id_with_object_id(self, event_id: str, client_id: str) -> tuple[Optional[ReportAuditLog], Optional[ObjectId]]:
        try:
            log_dict = await self.collection.find_one({"event_id": event_id, "client_id": client_id})
            if log_dict:
                object_id = log_dict.pop("_id", None)
                return ReportAuditLog(**log_dict), object_id
            return None, None
        except Exception as ex:
            logger.error(f"Error while fetching report audit log with object_id: {ex}", exc_info=True)
            return None, None

    async def update_by_object_id(self, object_id: ObjectId, log: ReportAuditLog) -> bool:
        try:
            logger.info(f"Updating Report Audit Log by ObjectId: {object_id}")
            log_dict = jsonable_encoder(log)
            log_dict.pop("event_id", None)  # Don't update the event_id
            log_dict["updated_at"] = datetime.utcnow()
            
            result = await self.collection.update_one(
                {"_id": object_id}, 
                {"$set": log_dict}
            )
            return result.modified_count > 0
        except Exception as ex:
            logger.error(f"Error while updating report audit log by ObjectId: {ex}", exc_info=True)
            return False 