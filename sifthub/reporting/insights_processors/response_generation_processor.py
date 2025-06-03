from typing import Optional
from io import BytesIO

from sifthub.reporting.excel_generators.insights_faq_excel_generator import InsightsFAQExcelGenerator
from sifthub.reporting.insights_processors.base_insights_processor import InsightsTypeProcessor
from sifthub.reporting.models.export_models import SQSExportMessage, InsightsSubType
from sifthub.utils.logger import setup_logger

logger = setup_logger(__name__)


class ResponseGenerationProcessor(InsightsTypeProcessor):
    """Processor for responseGeneration insights type"""
    
    async def process_export(self, message: SQSExportMessage) -> Optional[BytesIO]:
        """Process responseGeneration export"""
        try:
            if message.subType == InsightsSubType.FREQUENT_ASKED_QUESTIONS.value:
                generator = InsightsFAQExcelGenerator()
                return await generator.generate_excel(message)
            else:
                logger.error(f"Unsupported subType for responseGeneration: {message.subType}")
                return None
                
        except Exception as e:
            logger.error(f"Error processing responseGeneration export: {e}", exc_info=True)
            return None 