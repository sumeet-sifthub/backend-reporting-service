from typing import Optional
from io import BytesIO
from datetime import datetime

from sifthub.reporting.factories.insights_type_factory import get_insights_type_processor
from sifthub.reporting.processors.base_processor import ModuleProcessor
from sifthub.reporting.models.export_models import SQSExportMessage
from sifthub.utils.logger import setup_logger

logger = setup_logger()


class InsightsProcessor(ModuleProcessor):
    # Processor for INSIGHTS module.
    
    async def process_export(self, message: SQSExportMessage) -> Optional[BytesIO]:
        try:
            # Create processor based on type and subtype
            type_processor = await get_insights_type_processor(message.type, message.subType)
            if not type_processor:
                logger.error(f"No processor found for insights type: {message.type}, subType: {message.subType}")
                return None
            return await type_processor.process_export(message)
        except Exception as e:
            logger.error(f"Error processing insights export: {e}", exc_info=True)
            return None
    
    def get_export_filename(self, message: SQSExportMessage) -> str:
        # Determine sheet name based on filter
        sheet_suffix = self._get_sheet_suffix(message)
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        
        if message.subType == "frequentAskedQuestions":
            return f"Frequently_Asked_Questions_Report_{sheet_suffix}_{timestamp}.xlsx"
        else:
            return f"Insights_{message.type}_{message.subType}_{timestamp}.xlsx"
    
    def _get_sheet_suffix(self, message: SQSExportMessage) -> str:
        """Get sheet suffix based on filter data"""
        try:
            if message.filter and message.filter.conditions:
                status_condition = message.filter.conditions.get("status")
                if status_condition and status_condition.data:
                    data = status_condition.data
                    if "ANSWERED#@#NO_INFORMATION#@#PARTIAL" in data:
                        return "All"
                    elif "ANSWERED#@#PARTIAL" in data:
                        return "Answered"
                    elif "NO_INFORMATION" in data:
                        return "Unanswered"
            return "All"
        except Exception:
            return "All" 