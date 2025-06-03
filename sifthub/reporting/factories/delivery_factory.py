from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from io import BytesIO

from sifthub.reporting.models.export_models import SQSExportMessage, ExportMode
from sifthub.utils.logger import setup_logger

logger = setup_logger(__name__)


class DeliveryProcessor(ABC):
    """Abstract base class for delivery processors"""
    
    @abstractmethod
    async def deliver_export(self, file_stream: BytesIO, message: SQSExportMessage, 
                           filename: str) -> Dict[str, Any]:
        """Deliver export file"""
        pass


class DownloadDeliveryProcessor(DeliveryProcessor):
    """Processor for download delivery mode"""
    
    async def deliver_export(self, file_stream: BytesIO, message: SQSExportMessage, 
                           filename: str) -> Dict[str, Any]:
        """Upload to S3 and return download URL"""
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
            
            return {
                "success": True,
                "s3_bucket": "sifthub-exports",  # From aws_configs
                "s3_key": s3_key,
                "download_url": download_url
            }
            
        except Exception as e:
            logger.error(f"Error in download delivery: {e}", exc_info=True)
            return {"success": False, "error": str(e)}


class EmailDeliveryProcessor(DeliveryProcessor):
    """Processor for email delivery mode (placeholder for future implementation)"""
    
    async def deliver_export(self, file_stream: BytesIO, message: SQSExportMessage, 
                           filename: str) -> Dict[str, Any]:
        """Send export via email - placeholder implementation"""
        try:
            logger.info(f"Email delivery not yet implemented for event: {message.eventId}")
            
            # Placeholder - upload to S3 for now and return success
            from sifthub.datastores.document.s3.s3_client import S3Client
            
            s3_client = S3Client()
            s3_key = s3_client.generate_s3_key(
                message.eventId, 
                message.clientId, 
                message.module.value, 
                message.type, 
                message.subType
            )
            
            upload_success = await s3_client.upload_file_stream(file_stream, s3_key)
            
            return {
                "success": upload_success,
                "delivery_method": "email",
                "message": "Email delivery functionality will be implemented in future release",
                "s3_key": s3_key if upload_success else None
            }
            
        except Exception as e:
            logger.error(f"Error in email delivery: {e}", exc_info=True)
            return {"success": False, "error": str(e)}


class DeliveryProcessorFactory:
    """Factory for creating delivery processors"""
    
    _processors = {
        ExportMode.DOWNLOAD: DownloadDeliveryProcessor,
        ExportMode.EMAIL: EmailDeliveryProcessor,
    }
    
    @classmethod
    def create_processor(cls, mode: ExportMode) -> Optional[DeliveryProcessor]:
        """Create delivery processor based on delivery mode"""
        try:
            processor_class = cls._processors.get(mode)
            if processor_class:
                return processor_class()
            
            logger.error(f"No processor found for delivery mode: {mode}")
            return None
            
        except Exception as e:
            logger.error(f"Error creating delivery processor for {mode}: {e}", exc_info=True)
            return None
    
    @classmethod
    def register_processor(cls, mode: ExportMode, processor_class: type):
        """Register new delivery processor"""
        cls._processors[mode] = processor_class 