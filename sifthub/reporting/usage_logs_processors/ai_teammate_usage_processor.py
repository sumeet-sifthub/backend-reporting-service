from typing import Optional, Union, Dict
from io import BytesIO

from sifthub.reporting.usage_logs_processors.base_usage_logs_processor import UsageLogsTypeProcessor
from sifthub.reporting.models.export_models import SQSExportRequest
from sifthub.reporting.excel_generators.usage_logs_excel_generator import UsageLogsExcelGenerator
from sifthub.utils.logger import setup_logger

logger = setup_logger()


class AITeammateUsageLogsProcessor(UsageLogsTypeProcessor):
    # Processor for AITeammate usage logs with streaming batch processing
    
    async def process_export(self, message: SQSExportRequest) -> Optional[Union[BytesIO, Dict[str, str]]]:
        try:
            logger.info(f"Processing AITeammate usage logs export with streaming batches for event: {message.eventId}")
            
            generator = UsageLogsExcelGenerator()
            
            # Use streaming batch processing following the exact flow:
            # Fetch Batch 1 → Write to Excel → Stream to S3
            # Fetch Batch 2 → Append to Excel → Update S3
            # Continue... → Until Done → Generate URL
            # Firebase Notification
            result = await generator.generate_excel_streaming(message)
            
            if result:
                logger.info(f"Successfully generated streaming AITeammate usage logs export for event: {message.eventId}")
                logger.info(f"S3 Key: {result['s3_key']}, Download URL ready for Firebase notification")
                return result
            else:
                logger.error(f"Failed to generate streaming AITeammate usage logs export for event: {message.eventId}")
                return None
                
        except Exception as e:
            logger.error(f"Error processing streaming AITeammate usage logs export: {e}", exc_info=True)
            return None 