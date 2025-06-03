from typing import Optional
from io import BytesIO
from datetime import datetime

from sifthub.reporting.processors.base_processor import ModuleProcessor
from sifthub.reporting.models.export_models import SQSExportMessage
from sifthub.utils.logger import setup_logger

logger = setup_logger(__name__)


class UsageLogsProcessor(ModuleProcessor):
    """Processor for USAGE_LOGS module"""
    
    async def process_export(self, message: SQSExportMessage) -> Optional[BytesIO]:
        """Process usage logs export"""
        try:
            from sifthub.reporting.factories.usage_logs_type_factory import get_usage_logs_type_processor
            
            # Create processor based on type and subtype
            type_processor = await get_usage_logs_type_processor(message.type, message.subType)
            if not type_processor:
                logger.error(f"No processor found for usage logs type: {message.type}, subType: {message.subType}")
                return None
            
            return await type_processor.process_export(message)
            
        except Exception as e:
            logger.error(f"Error processing usage logs export: {e}", exc_info=True)
            return None
    
    def get_export_filename(self, message: SQSExportMessage) -> str:
        """Generate filename for usage logs export"""
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        return f"Usage_Logs_{message.type}_{message.subType}_{timestamp}.xlsx" 