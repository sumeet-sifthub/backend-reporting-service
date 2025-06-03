from typing import Optional, Union, Dict
from io import BytesIO

from sifthub.reporting.insights_processors.base_insights_processor import InsightsTypeProcessor
from sifthub.reporting.models.export_models import SQSExportMessage
from sifthub.reporting.excel_generators.insights_faq_excel_generator import InsightsFAQExcelGenerator
from sifthub.utils.logger import setup_logger

logger = setup_logger()


class ResponseGenerationProcessor(InsightsTypeProcessor):
    # Processor for response generation insights with streaming batch processing
    
    async def process_export(self, message: SQSExportMessage) -> Optional[Union[BytesIO, Dict[str, str]]]:
        try:
            if message.subType == "frequentAskedQuestions":
                return await self._process_faq_export(message)
            else:
                logger.error(f"Unsupported subType for response generation: {message.subType}")
                return None
        except Exception as e:
            logger.error(f"Error in response generation processor: {e}", exc_info=True)
            return None
    
    async def _process_faq_export(self, message: SQSExportMessage) -> Optional[Dict[str, str]]:
        # Process FAQ export using streaming batch processing: Fetch Batch → Write to Excel → Stream to S3
        try:
            logger.info(f"Processing FAQ export with streaming batches for event: {message.eventId}")
            
            generator = InsightsFAQExcelGenerator()
            
            # Use streaming batch processing following the exact flow:
            # Fetch Batch 1 → Write to Excel → Stream to S3
            # Fetch Batch 2 → Append to Excel → Update S3
            # Continue... → Until Done → Generate URL
            # Firebase Notification
            result = await generator.generate_excel_streaming(message)
            
            if result:
                logger.info(f"Successfully generated streaming FAQ export for event: {message.eventId}")
                logger.info(f"S3 Key: {result['s3_key']}, Download URL ready for Firebase notification")
                return result
            else:
                logger.error(f"Failed to generate streaming FAQ export for event: {message.eventId}")
                return None
                
        except Exception as e:
            logger.error(f"Error processing streaming FAQ export: {e}", exc_info=True)
            return None 