from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from io import BytesIO
from datetime import datetime
from sifthub.reporting.models.export_models import (
    SQSExportMessage, ExportModule, ExportMode, InsightsType, UsageLogsType
)
from sifthub.utils.logger import setup_logger

logger = setup_logger(__name__)


class ExportProcessor(ABC):
    """Abstract base class for export processors"""
    
    @abstractmethod
    async def process_export(self, message: SQSExportMessage) -> Optional[BytesIO]:
        """Process export and return Excel file stream"""
        pass

    @abstractmethod
    def get_export_filename(self, message: SQSExportMessage) -> str:
        """Generate filename for the export"""
        pass


class InsightsFrequentAskedQuestionsProcessor(ExportProcessor):
    """Processor for insights frequent asked questions export"""
    
    async def process_export(self, message: SQSExportMessage) -> Optional[BytesIO]:
        """Process insights frequent asked questions export"""
        try:
            from sifthub.reporting.services.excel_generators.insights_faq_generator import InsightsFAQExcelGenerator
            
            generator = InsightsFAQExcelGenerator()
            return await generator.generate_excel(message)
            
        except Exception as e:
            logger.error(f"Error processing insights FAQ export: {e}", exc_info=True)
            return None

    def get_export_filename(self, message: SQSExportMessage) -> str:
        """Generate filename for insights FAQ export"""
        from datetime import datetime
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        return f"Frequently_Asked_Questions_Report_{timestamp}.xlsx"


class ProjectCollaborationProcessor(ExportProcessor):
    """Processor for project collaboration insights export"""
    
    async def process_export(self, message: SQSExportMessage) -> Optional[BytesIO]:
        """Process project collaboration insights export"""
        try:
            from sifthub.reporting.services.excel_generators.project_collaboration_generator import ProjectCollaborationExcelGenerator
            
            logger.info(f"Processing project collaboration export for event: {message.eventId}")
            generator = ProjectCollaborationExcelGenerator()
            return await generator.generate_excel(message)
            
        except Exception as e:
            logger.error(f"Error processing project collaboration export: {e}", exc_info=True)
            return None

    def get_export_filename(self, message: SQSExportMessage) -> str:
        """Generate filename for project collaboration export"""
        from datetime import datetime
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        return f"Project_Collaboration_Report_{timestamp}.xlsx"


class AITeammateProcessor(ExportProcessor):
    """Processor for AI teammate insights export"""
    
    async def process_export(self, message: SQSExportMessage) -> Optional[BytesIO]:
        """Process AI teammate insights export"""
        try:
            from sifthub.reporting.services.excel_generators.ai_teammate_generator import AITeammateExcelGenerator
            
            logger.info(f"Processing AI teammate export for event: {message.eventId}")
            generator = AITeammateExcelGenerator()
            return await generator.generate_excel(message)
            
        except Exception as e:
            logger.error(f"Error processing AI teammate export: {e}", exc_info=True)
            return None

    def get_export_filename(self, message: SQSExportMessage) -> str:
        """Generate filename for AI teammate export"""
        from datetime import datetime
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        return f"AI_Teammate_Report_{timestamp}.xlsx"


class UsageLogsProcessor(ExportProcessor):
    """Processor for usage logs export"""
    
    async def process_export(self, message: SQSExportMessage) -> Optional[BytesIO]:
        """Process usage logs export"""
        try:
            from sifthub.reporting.services.excel_generators.usage_logs_generator import UsageLogsExcelGenerator
            
            logger.info(f"Processing usage logs export for type: {message.type}, subType: {message.subType}")
            generator = UsageLogsExcelGenerator()
            return await generator.generate_excel(message)
            
        except Exception as e:
            logger.error(f"Error processing usage logs export: {e}", exc_info=True)
            return None

    def get_export_filename(self, message: SQSExportMessage) -> str:
        """Generate filename for usage logs export"""
        from datetime import datetime
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        
        # Create specific filename based on usage log type
        if message.type == UsageLogsType.ANSWER:
            return f"Answer_Usage_Logs_{timestamp}.xlsx"
        elif message.type == UsageLogsType.AUTOFILL:
            return f"Autofill_Usage_Logs_{timestamp}.xlsx"
        elif message.type == UsageLogsType.AI_TEAMMATE:
            return f"AI_Teammate_Usage_Logs_{timestamp}.xlsx"
        else:
            return f"Usage_Logs_Report_{timestamp}.xlsx"


class ExportProcessorFactory:
    """Factory class to create appropriate export processors"""
    
    @staticmethod
    def create_processor(message: SQSExportMessage) -> Optional[ExportProcessor]:
        """Create appropriate processor based on message module and type"""
        try:
            if message.module == ExportModule.INSIGHTS:
                if message.type == InsightsType.RESPONSE_GENERATION:
                    if message.subType == "frequentAskedQuestions":
                        return InsightsFrequentAskedQuestionsProcessor()
                    else:
                        logger.warning(f"Unsupported insights subType: {message.subType}")
                        return None
                elif message.type == InsightsType.PROJECT_COLLABORATION:
                    return ProjectCollaborationProcessor()
                elif message.type == InsightsType.AI_TEAMMATE:
                    return AITeammateProcessor()
                else:
                    logger.warning(f"Unsupported insights type: {message.type}")
                    return None
                    
            elif message.module == ExportModule.USAGE_LOGS:
                if message.type in [UsageLogsType.ANSWER, UsageLogsType.AUTOFILL, UsageLogsType.AI_TEAMMATE]:
                    return UsageLogsProcessor()
                else:
                    logger.warning(f"Unsupported usage logs type: {message.type}")
                    return None
            else:
                logger.warning(f"Unsupported export module: {message.module}")
                return None
                
        except Exception as e:
            logger.error(f"Error creating export processor: {e}", exc_info=True)
            return None


class DeliveryProcessor(ABC):
    """Abstract base class for delivery processors"""
    
    @abstractmethod
    async def deliver_export(self, file_stream: BytesIO, message: SQSExportMessage, 
                           filename: str) -> Dict[str, Any]:
        """Deliver export file and return delivery details"""
        pass


class DownloadDeliveryProcessor(DeliveryProcessor):
    """Processor for download delivery mode"""
    
    async def deliver_export(self, file_stream: BytesIO, message: SQSExportMessage, 
                           filename: str) -> Dict[str, Any]:
        """Upload to S3 and return download URL"""
        try:
            from sifthub.datastores.document.s3.s3_client import s3_client
            
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
            
            return {
                "success": True,
                "s3_bucket": s3_client.session._client_config.region_name,
                "s3_key": s3_key,
                "download_url": download_url
            }
            
        except Exception as e:
            logger.error(f"Error in download delivery: {e}", exc_info=True)
            return {"success": False, "error": str(e)}


class EmailDeliveryProcessor(DeliveryProcessor):
    """Processor for email delivery mode"""
    
    async def deliver_export(self, file_stream: BytesIO, message: SQSExportMessage, 
                           filename: str) -> Dict[str, Any]:
        """Send export via email"""
        try:
            import smtplib
            from email.mime.multipart import MIMEMultipart
            from email.mime.base import MIMEBase
            from email.mime.text import MIMEText
            from email import encoders
            import os
            
            logger.info(f"Sending email delivery for event: {message.eventId}")
            
            # Email configuration from environment variables
            smtp_server = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
            smtp_port = int(os.getenv('SMTP_PORT', '587'))
            smtp_username = os.getenv('SMTP_USERNAME')
            smtp_password = os.getenv('SMTP_PASSWORD')
            from_email = os.getenv('FROM_EMAIL', smtp_username)
            
            if not all([smtp_username, smtp_password, from_email]):
                raise Exception("Email configuration missing. Please set SMTP_USERNAME, SMTP_PASSWORD, and FROM_EMAIL environment variables.")
            
            # Get recipient email from message or use default
            to_email = getattr(message, 'recipientEmail', None)
            if not to_email:
                raise Exception("Recipient email not provided in export message")
            
            # Create message
            msg = MIMEMultipart()
            msg['From'] = from_email
            msg['To'] = to_email
            msg['Subject'] = f"Export Report - {filename}"
            
            # Email body
            body = f"""
            Dear User,
            
            Your requested export report is ready and attached to this email.
            
            Export Details:
            - Event ID: {message.eventId}
            - Module: {message.module.value}
            - Type: {message.type}
            - Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC
            
            Please find the attached Excel file with your requested data.
            
            Best regards,
            Reporting Service Team
            """
            
            msg.attach(MIMEText(body, 'plain'))
            
            # Attach the Excel file
            file_stream.seek(0)  # Reset stream position
            attachment = MIMEBase('application', 'octet-stream')
            attachment.set_payload(file_stream.read())
            encoders.encode_base64(attachment)
            attachment.add_header(
                'Content-Disposition',
                f'attachment; filename= {filename}'
            )
            msg.attach(attachment)
            
            # Send email
            server = smtplib.SMTP(smtp_server, smtp_port)
            server.starttls()  # Enable encryption
            server.login(smtp_username, smtp_password)
            text = msg.as_string()
            server.sendmail(from_email, to_email, text)
            server.quit()
            
            logger.info(f"Email sent successfully to {to_email} for event {message.eventId}")
            
            return {
                "success": True,
                "delivery_method": "email",
                "recipient": to_email,
                "filename": filename,
                "sent_at": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error in email delivery: {e}", exc_info=True)
            return {"success": False, "error": str(e)}


class DeliveryProcessorFactory:
    """Factory class to create appropriate delivery processors"""
    
    @staticmethod
    def create_processor(mode: ExportMode) -> Optional[DeliveryProcessor]:
        """Create appropriate delivery processor based on mode"""
        try:
            if mode == ExportMode.DOWNLOAD:
                return DownloadDeliveryProcessor()
            elif mode == ExportMode.EMAIL:
                return EmailDeliveryProcessor()
            else:
                logger.warning(f"Unsupported delivery mode: {mode}")
                return None
                
        except Exception as e:
            logger.error(f"Error creating delivery processor: {e}", exc_info=True)
            return None 