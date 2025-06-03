import time
from typing import Optional
from sifthub.reporting.models.export_models import SQSExportMessage, ExportStatus
from sifthub.reporting.services.export_factory import ExportProcessorFactory, DeliveryProcessorFactory
from sifthub.datastores.event.mongo import audit_log_store
from sifthub.datastores.product.firebase.firebaseConfiguration import firebase_config
from sifthub.datastores.product.firebase.firebase_publisher import FirebasePublisher
from sifthub.utils.logger import setup_logger

logger = setup_logger(__name__)


class ExportService:
    """Main service for handling export requests"""

    def __init__(self):
        self.audit_log_datastore = None
        self.firebase_publisher = None

    async def initialize(self):
        """Initialize database connections"""
        try:
            # Initialize MongoDB using the global store
            self.audit_log_datastore = audit_log_store
            
            # Initialize Firebase
            firebase_app, firebase_client = await firebase_config.initialize_firebase_app()
            self.firebase_publisher = FirebasePublisher(firebase_client)
            
            logger.info("Export service initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize export service: {e}", exc_info=True)
            raise

    async def process_export_request(self, message: SQSExportMessage) -> bool:
        """Process an export request"""
        start_time = time.time()
        
        try:
            logger.info(f"Processing export request for event: {message.eventId}")
            
            # Update status to processing
            await self._update_audit_log_status(
                message.eventId, 
                message.clientId, 
                ExportStatus.PROCESSING
            )
            
            # Create export processor
            export_processor = ExportProcessorFactory.create_processor(message)
            if not export_processor:
                logger.error(f"No processor found for message: {message}")
                await self._update_audit_log_status(
                    message.eventId, 
                    message.clientId, 
                    ExportStatus.FAILED
                )
                return False
            
            # Process the export
            export_file = await export_processor.process_export(message)
            if not export_file:
                logger.error(f"Export processing failed for event: {message.eventId}")
                await self._update_audit_log_status(
                    message.eventId, 
                    message.clientId, 
                    ExportStatus.FAILED
                )
                return False
            
            # Create delivery processor
            delivery_processor = DeliveryProcessorFactory.create_processor(message.mode)
            if not delivery_processor:
                logger.error(f"No delivery processor found for mode: {message.mode}")
                await self._update_audit_log_status(
                    message.eventId, 
                    message.clientId, 
                    ExportStatus.FAILED
                )
                return False
            
            # Get filename
            filename = export_processor.get_export_filename(message)
            
            # Deliver the export
            delivery_result = await delivery_processor.deliver_export(
                export_file, filename, message
            )
            
            if delivery_result.get("success"):
                # Calculate total time
                total_time = int((time.time() - start_time) * 1000)  # milliseconds
                
                # Update status to completed
                await self._update_audit_log_status(
                    message.eventId,
                    message.clientId,
                    ExportStatus.COMPLETED,
                    total_time=total_time,
                    s3_bucket=delivery_result.get("s3_bucket"),
                    download_url=delivery_result.get("download_url")
                )
                
                # Send Firebase notification
                await self._send_firebase_notification(
                    message, 
                    delivery_result.get("download_url", ""), 
                    "SUCCESS"
                )
                
                logger.info(f"Export completed successfully for event: {message.eventId}")
                return True
            else:
                await self._update_audit_log_status(
                    message.eventId, 
                    message.clientId, 
                    ExportStatus.FAILED
                )
                return False
                
        except Exception as e:
            logger.error(f"Error processing export request: {e}", exc_info=True)
            await self._update_audit_log_status(
                message.eventId, 
                message.clientId, 
                ExportStatus.FAILED
            )
            return False

    async def _update_audit_log_status(self, event_id: str, client_id: str, status: ExportStatus,
                                     total_time: Optional[int] = None, s3_bucket: Optional[str] = None,
                                     download_url: Optional[str] = None):
        """Update audit log status"""
        try:
            if not self.audit_log_datastore:
                logger.error("Audit log datastore not initialized")
                return
            
            success = await self.audit_log_datastore.update_status(
                event_id, client_id, status, total_time, s3_bucket, download_url
            )
            
            if success:
                logger.info(f"Updated audit log status to {status.value} for event: {event_id}")
            else:
                logger.error(f"Failed to update audit log status for event: {event_id}")
                
        except Exception as e:
            logger.error(f"Error updating audit log status: {e}", exc_info=True)

    async def _send_firebase_notification(self, message: SQSExportMessage, download_url: str, status: str):
        """Send Firebase notification"""
        try:
            if not self.firebase_publisher:
                logger.error("Firebase publisher not initialized")
                return
            
            success = await self.firebase_publisher.publish_export_notification(
                message.eventId,
                download_url,
                status,
                message.user_id,
                message.clientId,
                message.productId
            )
            
            if success:
                logger.info(f"Sent Firebase notification for event: {message.eventId}")
            else:
                logger.error(f"Failed to send Firebase notification for event: {message.eventId}")
                
        except Exception as e:
            logger.error(f"Error sending Firebase notification: {e}", exc_info=True)

    async def cleanup(self):
        """Cleanup resources"""
        try:
            # No need to disconnect mongo client as it's managed globally
            logger.info("Export service cleanup completed")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}", exc_info=True)


# Global export service instance
export_service = ExportService() 