import time
from typing import Dict, Any, Optional
from datetime import datetime

from sifthub.reporting.models.export_models import SQSExportMessage, ExportStatus, ReportAuditLog
from sifthub.reporting.factories.module_factory import ModuleProcessorFactory
from sifthub.reporting.factories.delivery_factory import DeliveryProcessorFactory
from sifthub.datastores.event.mongo_client import MongoClient
from sifthub.datastores.product.firebase import Firebase
from sifthub.configs import mongo_configs, aws_configs
from sifthub.utils.logger import setup_logger

logger = setup_logger(__name__)


async def handle_event(event_context: Dict[str, Any], event_type: str) -> bool:
    try:
        export_message = SQSExportMessage(**event_context)
        logger.info(f"Processing export request for event: {export_message.eventId}")
        return await process_export_request(export_message)
    except Exception as e:
        logger.error(f"Error handling export event: {e}", exc_info=True)
        return False


async def process_export_request(message: SQSExportMessage) -> bool:
    start_time = time.time()
    try:
        # Update status to processing
        await update_audit_log_status(
            message.eventId, 
            message.clientId, 
            ExportStatus.PROCESSING
        )
        # Create module processor using factory
        module_processor = ModuleProcessorFactory.create_processor(message.module)
        if not module_processor:
            logger.error(f"No processor found for module: {message.module}")
            await update_audit_log_status(
                message.eventId, 
                message.clientId, 
                ExportStatus.FAILED
            )
            return False
        # Process the export
        export_file = await module_processor.process_export(message)
        if not export_file:
            logger.error(f"Export processing failed for event: {message.eventId}")
            await update_audit_log_status(
                message.eventId, 
                message.clientId, 
                ExportStatus.FAILED
            )
            return False
        # Create delivery processor using factory
        delivery_processor = DeliveryProcessorFactory.create_processor(message.mode)
        if not delivery_processor:
            logger.error(f"No delivery processor found for mode: {message.mode}")
            await update_audit_log_status(
                message.eventId, 
                message.clientId, 
                ExportStatus.FAILED
            )
            return False
        # Get filename from module processor
        filename = module_processor.get_export_filename(message)
        # Deliver the export
        delivery_result = await delivery_processor.deliver_export(
            export_file, message, filename
        )
        if delivery_result.get("success"):
            # Calculate total time in seconds
            total_time = int(time.time() - start_time)
            
            # Update status to success
            await update_audit_log_status(
                message.eventId,
                message.clientId,
                ExportStatus.SUCCESS,
                total_time=total_time,
                s3_bucket=delivery_result.get("s3_bucket"),
                download_url=delivery_result.get("download_url")
            )
            # Send Firebase notification for download mode
            if message.mode.value == "download":
                await send_firebase_notification(
                    message, 
                    delivery_result.get("download_url", ""),
                    "SUCCESS"
                )
            logger.info(f"Export completed successfully for event: {message.eventId}")
            return True
        else:
            await update_audit_log_status(
                message.eventId, 
                message.clientId, 
                ExportStatus.FAILED
            )
            # Send failure notification
            await send_firebase_notification(message, "", "FAILED")
            return False
            
    except Exception as e:
        logger.error(f"Error processing export request: {e}", exc_info=True)
        await update_audit_log_status(
            message.eventId, 
            message.clientId, 
            ExportStatus.FAILED
        )
        # Send failure notification
        await send_firebase_notification(message, "", "FAILED")
        return False


async def update_audit_log_status(event_id: str, client_id: str, 
                                 status: ExportStatus, total_time: Optional[int] = None,
                                 s3_bucket: Optional[str] = None, 
                                 download_url: Optional[str] = None):
    """Update audit log status in MongoDB"""
    try:
        # Initialize MongoDB connection
        mongo_client = MongoClient(mongo_configs.MONGODB_CONNECTION_STRING)
        db = mongo_client.connect(mongo_configs.MONGODB_DATABASE_NAME)
        audit_log_collection = db.get_collection("report_audit_log")
        
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
        
        result = await audit_log_collection.update_one(
            {"event_id": event_id, "client_id": client_id},
            {"$set": update_data}
        )
        if result.modified_count > 0:
            logger.info(f"Updated audit log status to {status.value} for event: {event_id}")
        else:
            logger.warning(f"No audit log found to update for event: {event_id}")
    except Exception as e:
        logger.error(f"Error updating audit log status: {e}", exc_info=True)


async def send_firebase_notification(message: SQSExportMessage, download_url: str, status: str):
    """Send Firebase notification for export completion using dedicated export notification method"""
    try:
        # Get Firebase publisher
        firebase_publisher = Firebase.get_publisher()
        
        # Use the dedicated export notification method
        success = await firebase_publisher.publish_export_notification(
            event_id=message.eventId,
            download_url=download_url,
            status=status,
            user_id=message.user_id,  # Convert to int as required by publisher
            client_id=message.clientId,  # Convert to int as required by publisher
            product_id=message.productId  # Convert to int as required by publisher
        )
        if success:
            logger.info(f"Firebase export notification sent successfully for event: {message.eventId}")
        else:
            logger.warning(f"Failed to send Firebase export notification for event: {message.eventId}, client: {message.clientId}, user: {message.user_id}")
    except Exception as e:
        logger.error(f"Error sending Firebase notification: {e}", exc_info=True) 