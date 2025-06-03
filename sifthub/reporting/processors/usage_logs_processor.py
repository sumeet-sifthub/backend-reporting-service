from typing import Optional, Union, Dict
from io import BytesIO
from datetime import datetime

from sifthub.reporting.processors.base_processor import ModuleProcessor
from sifthub.reporting.models.export_models import SQSExportMessage
from sifthub.utils.logger import setup_logger

logger = setup_logger(__name__)


class UsageLogsProcessor(ModuleProcessor):
    """Processor for USAGE_LOGS module with streaming batch processing support"""
    
    async def process_export(self, message: SQSExportMessage) -> Optional[Union[BytesIO, Dict[str, str]]]:
        """Process usage logs export with streaming support"""
        try:
            from sifthub.reporting.factories.usage_logs_type_factory import get_usage_logs_type_processor
            
            # Create processor based on type and subtype
            type_processor = await get_usage_logs_type_processor(message.type, message.subType)
            if not type_processor:
                logger.error(f"No processor found for usage logs type: {message.type}, subType: {message.subType}")
                return None
            
            logger.info(f"Processing usage logs export for type: {message.type}, subType: {message.subType}")
            return await type_processor.process_export(message)
            
        except Exception as e:
            logger.error(f"Error processing usage logs export: {e}", exc_info=True)
            return None
    
    def get_export_filename(self, message: SQSExportMessage) -> str:
        """Generate filename for usage logs export based on type and date range"""
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        
        # Extract date range for filename
        date_range = self._get_date_range_string(message.pageFilter)
        
        # Format type name for filename
        type_name = message.type.title()
        
        return f"{type_name}_Usage_logs_{date_range}_{timestamp}.xlsx"
    
    def _get_date_range_string(self, page_filter) -> str:
        """Extract date range from page filter for filename"""
        try:
            if page_filter and page_filter.conditions:
                meta_created = page_filter.conditions.get("meta.created")
                if meta_created and meta_created.data:
                    timestamps = meta_created.data.split("#@#")
                    if len(timestamps) == 2:
                        start_ts = int(timestamps[0]) / 1000
                        end_ts = int(timestamps[1]) / 1000
                        
                        start_date = datetime.fromtimestamp(start_ts).strftime("%b_%d_%Y")
                        end_date = datetime.fromtimestamp(end_ts).strftime("%b_%d_%Y")
                        
                        return f"{start_date}_to_{end_date}"
            
            return "Jan_03_2025_to_Feb_03_2024"  # Default fallback
            
        except Exception as e:
            logger.error(f"Error parsing date range: {e}")
            return "Jan_03_2025_to_Feb_03_2024" 