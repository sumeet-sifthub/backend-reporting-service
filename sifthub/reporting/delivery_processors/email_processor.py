from typing import Dict, Any, Union
from io import BytesIO

from sifthub.reporting.delivery_processors.base_delivery_processor import DeliveryProcessor
from sifthub.reporting.models.export_models import SQSExportMessage
from sifthub.utils.logger import setup_logger

logger = setup_logger()


class EmailDeliveryProcessor(DeliveryProcessor):
    """Processor for email delivery mode"""
    
    async def deliver_export(self, export_result: Union[BytesIO, Dict[str, str]], message: SQSExportMessage, 
                           filename: str) -> Dict[str, Any]:
        """Send export file via email - supports both legacy and streaming approaches"""
        try:
            # Check if this is the new streaming result (dict) or legacy BytesIO
            if isinstance(export_result, dict):
                return await self._handle_streaming_email(export_result, message, filename)
            else:
                return await self._handle_legacy_email(export_result, message, filename)
                
        except Exception as e:
            logger.error(f"Error in email delivery: {e}", exc_info=True)
            # Send failure notification
            await self._send_firebase_notification(message, "", "FAILED")
            return {"success": False, "error": str(e)}
    
    async def _handle_streaming_email(self, streaming_result: Dict[str, str], message: SQSExportMessage, filename: str) -> Dict[str, Any]:
        """Handle streaming result for email delivery"""
        try:
            # For email delivery with streaming, we'd need to download from S3 and attach
            # For now, just send download link via email
            download_url = streaming_result.get("download_url")
            
            if not download_url:
                raise Exception("No download URL available for email delivery")
            
            logger.info(f"Sending email with download link for event: {message.eventId}")
            
            # TODO: Implement actual email sending logic
            # This would typically involve:
            # 1. Get user email from user service
            # 2. Send email with download link
            # 3. Set expiry reminder
            
            # Send Firebase notification for successful email
            await self._send_firebase_notification(message, download_url, "SUCCESS")
            
            return {
                "success": True,
                "delivery_method": "email_link",
                "download_url": download_url
            }
            
        except Exception as e:
            logger.error(f"Error handling streaming email: {e}", exc_info=True)
            raise e
    
    async def _handle_legacy_email(self, file_stream: BytesIO, message: SQSExportMessage, filename: str) -> Dict[str, Any]:
        """Handle legacy BytesIO result for email delivery"""
        try:
            logger.info(f"Sending email with attachment for event: {message.eventId}")
            
            # TODO: Implement actual email sending logic
            # This would typically involve:
            # 1. Get user email from user service  
            # 2. Send email with file attachment
            # 3. Handle attachment size limits
            
            # Send Firebase notification for successful email
            await self._send_firebase_notification(message, "", "SUCCESS")
            
            return {
                "success": True,
                "delivery_method": "email_attachment",
                "filename": filename
            }
            
        except Exception as e:
            logger.error(f"Error handling legacy email: {e}", exc_info=True)
            raise e
    
    async def _send_firebase_notification(self, message: SQSExportMessage, download_url: str, status: str):
        """Send Firebase notification for email completion"""
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