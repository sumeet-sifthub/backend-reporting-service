from typing import Dict, Any
from io import BytesIO

from sifthub.reporting.delivery_processors.base_delivery_processor import DeliveryProcessor
from sifthub.reporting.models.export_models import SQSExportMessage
from sifthub.utils.logger import setup_logger

logger = setup_logger()


class DownloadDeliveryProcessor(DeliveryProcessor):
    """Processor for download delivery mode"""
    
    async def deliver_export(self, file_stream: BytesIO, message: SQSExportMessage, 
                           filename: str) -> Dict[str, Any]:
        """Upload to S3, generate download URL, and send Firebase notification"""
        try:
            from sifthub.datastores.document.s3.s3_client import S3Client
            
            s3_client = S3Client()
            
            # Generate S3 key
            s3_key = s3_client.generate_s3_key(
                message.eventId, 
                message.clientId, 
                message.module.value, 
                message.type, 
                message.subType
            )
            
            # Upload to S3
            upload_success = await s3_client.upload_file_stream(file_stream, s3_key)
            if not upload_success:
                raise Exception("Failed to upload file to S3")
            
            # Generate presigned URL
            download_url = await s3_client.generate_presigned_url(s3_key)
            if not download_url:
                raise Exception("Failed to generate presigned URL")
            
            logger.info(f"Successfully uploaded file to S3 and generated presigned URL for event: {message.eventId}")
            
            # Send Firebase notification for successful download
            await self._send_firebase_notification(message, download_url, "SUCCESS")
            
            return {
                "success": True,
                "s3_bucket": "sifthub-exports",  # From aws_configs
                "s3_key": s3_key,
                "download_url": download_url
            }
            
        except Exception as e:
            logger.error(f"Error in download delivery: {e}", exc_info=True)
            # Send failure notification
            await self._send_firebase_notification(message, "", "FAILED")
            return {"success": False, "error": str(e)}
    
    async def _send_firebase_notification(self, message: SQSExportMessage, download_url: str, status: str):
        """Send Firebase notification for download completion"""
        try:
            from sifthub.datastores.product.firebase import Firebase
            
            # Get Firebase publisher
            firebase_publisher = Firebase.get_publisher()
            
            # Use the dedicated export notification method
            success = await firebase_publisher.publish_export_notification(
                event_id=message.eventId,
                download_url=download_url,
                status=status,
                user_id=message.user_id,
                client_id=message.clientId,
                product_id=message.productId
            )
            if success:
                logger.info(f"Firebase export notification sent successfully for event: {message.eventId}")
            else:
                logger.warning(f"Failed to send Firebase export notification for event: {message.eventId}, client: {message.clientId}, user: {message.user_id}")
        except Exception as e:
            logger.error(f"Error sending Firebase notification: {e}", exc_info=True) 