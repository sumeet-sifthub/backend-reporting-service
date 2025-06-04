from typing import Dict, Any, Union
from io import BytesIO

from sifthub.reporting.delivery_processors.base_delivery_processor import DeliveryProcessor
from sifthub.reporting.models.export_models import SQSExportRequest
from sifthub.utils.logger import setup_logger

logger = setup_logger()


class DownloadDeliveryProcessor(DeliveryProcessor):
    """Processor for download delivery mode - supports both legacy and streaming approaches"""
    
    async def deliver_export(self, export_result: Union[BytesIO, Dict[str, str]], message: SQSExportRequest,
                             filename: str) -> Dict[str, Any]:
        """Handle export delivery - supports both BytesIO (legacy) and dict (streaming) inputs"""
        try:
            # Check if this is the new streaming result (dict) or legacy BytesIO
            if isinstance(export_result, dict):
                return await self._handle_streaming_result(export_result, message)
            else:
                return await self._handle_legacy_result(export_result, message, filename)
                
        except Exception as e:
            logger.error(f"Error in download delivery: {e}", exc_info=True)
            # Send failure notification
            await self._send_firebase_notification(message, "", "FAILED")
            return {"success": False, "error": str(e)}
    
    async def _handle_streaming_result(self, streaming_result: Dict[str, str], message: SQSExportRequest) -> Dict[str, Any]:
        """Handle streaming result where file is already on S3"""
        try:
            s3_key = streaming_result.get("s3_key")
            download_url = streaming_result.get("download_url") 
            s3_bucket = streaming_result.get("s3_bucket")
            
            if not all([s3_key, download_url, s3_bucket]):
                raise Exception("Invalid streaming result - missing required fields")
            
            logger.info(f"Processing streaming result for event: {message.eventId}, S3 key: {s3_key}")
            
            # Send Firebase notification for successful download
            await self._send_firebase_notification(message, download_url, "SUCCESS")
            
            return {
                "success": True,
                "s3_bucket": s3_bucket,
                "s3_key": s3_key,
                "download_url": download_url
            }
            
        except Exception as e:
            logger.error(f"Error handling streaming result: {e}", exc_info=True)
            raise e
    
    async def _handle_legacy_result(self, file_stream: BytesIO, message: SQSExportRequest, filename: str) -> Dict[str, Any]:
        """Handle legacy BytesIO result - upload to S3 and generate URL"""
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
            
            logger.info(f"Successfully uploaded legacy file to S3 and generated presigned URL for event: {message.eventId}")
            
            # Send Firebase notification for successful download
            await self._send_firebase_notification(message, download_url, "SUCCESS")
            
            return {
                "success": True,
                "s3_bucket": "sifthub-exports",  # From aws_configs
                "s3_key": s3_key,
                "download_url": download_url
            }
            
        except Exception as e:
            logger.error(f"Error handling legacy result: {e}", exc_info=True)
            raise e
    
    async def _send_firebase_notification(self, message: SQSExportRequest, download_url: str, status: str):
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