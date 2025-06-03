from abc import ABC, abstractmethod
from typing import Optional
from io import BytesIO

from sifthub.reporting.models.export_models import SQSExportMessage, ExportModule
from sifthub.utils.logger import setup_logger

logger = setup_logger(__name__)


class ModuleProcessor(ABC):
    """Abstract base class for module processors"""
    
    @abstractmethod
    async def process_export(self, message: SQSExportMessage) -> Optional[BytesIO]:
        """Process export for the specific module"""
        pass
    
    @abstractmethod
    def get_export_filename(self, message: SQSExportMessage) -> str:
        """Generate filename for the export"""
        pass


class InsightsProcessor(ModuleProcessor):
    """Processor for INSIGHTS module"""
    
    async def process_export(self, message: SQSExportMessage) -> Optional[BytesIO]:
        """Process insights export"""
        try:
            from sifthub.reporting.factories.insights_type_factory import InsightsTypeFactory
            
            # Create processor based on type and subtype
            type_processor = InsightsTypeFactory.create_processor(message.type, message.subType)
            if not type_processor:
                logger.error(f"No processor found for insights type: {message.type}, subType: {message.subType}")
                return None
            
            return await type_processor.process_export(message)
            
        except Exception as e:
            logger.error(f"Error processing insights export: {e}", exc_info=True)
            return None
    
    def get_export_filename(self, message: SQSExportMessage) -> str:
        """Generate filename for insights export"""
        from datetime import datetime
        
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


class UsageLogsProcessor(ModuleProcessor):
    """Processor for USAGE_LOGS module"""
    
    async def process_export(self, message: SQSExportMessage) -> Optional[BytesIO]:
        """Process usage logs export"""
        try:
            from sifthub.reporting.factories.usage_logs_type_factory import UsageLogsTypeFactory
            
            # Create processor based on type and subtype
            type_processor = UsageLogsTypeFactory.create_processor(message.type, message.subType)
            if not type_processor:
                logger.error(f"No processor found for usage logs type: {message.type}, subType: {message.subType}")
                return None
            
            return await type_processor.process_export(message)
            
        except Exception as e:
            logger.error(f"Error processing usage logs export: {e}", exc_info=True)
            return None
    
    def get_export_filename(self, message: SQSExportMessage) -> str:
        """Generate filename for usage logs export"""
        from datetime import datetime
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        return f"Usage_Logs_{message.type}_{message.subType}_{timestamp}.xlsx"


class ModuleProcessorFactory:
    """Factory for creating module processors"""
    
    _processors = {
        ExportModule.INSIGHTS: InsightsProcessor,
        ExportModule.USAGE_LOGS: UsageLogsProcessor,
    }
    
    @classmethod
    def create_processor(cls, module: ExportModule) -> Optional[ModuleProcessor]:
        """Create module processor based on module type"""
        try:
            processor_class = cls._processors.get(module)
            if processor_class:
                return processor_class()
            
            logger.error(f"No processor found for module: {module}")
            return None
            
        except Exception as e:
            logger.error(f"Error creating module processor for {module}: {e}", exc_info=True)
            return None
    
    @classmethod
    def register_processor(cls, module: ExportModule, processor_class: type):
        """Register new module processor"""
        cls._processors[module] = processor_class 