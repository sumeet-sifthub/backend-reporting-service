from abc import ABC, abstractmethod
from typing import Optional
from io import BytesIO

from sifthub.reporting.models.export_models import SQSExportMessage, UsageLogsType, UsageLogsSubType
from sifthub.utils.logger import setup_logger

logger = setup_logger(__name__)


class UsageLogsTypeProcessor(ABC):
    """Abstract base class for usage logs type processors"""
    
    @abstractmethod
    async def process_export(self, message: SQSExportMessage) -> Optional[BytesIO]:
        """Process export for the specific usage logs type"""
        pass


class AnswerProcessor(UsageLogsTypeProcessor):
    """Processor for answer usage logs type"""
    
    async def process_export(self, message: SQSExportMessage) -> Optional[BytesIO]:
        """Process answer usage logs export"""
        try:
            logger.info("Answer usage logs export processor - placeholder implementation")
            # TODO: Implement answer usage logs specific export logic
            return None
                
        except Exception as e:
            logger.error(f"Error processing answer usage logs export: {e}", exc_info=True)
            return None


class AutofillProcessor(UsageLogsTypeProcessor):
    """Processor for autofill usage logs type"""
    
    async def process_export(self, message: SQSExportMessage) -> Optional[BytesIO]:
        """Process autofill usage logs export"""
        try:
            logger.info("Autofill usage logs export processor - placeholder implementation")
            # TODO: Implement autofill usage logs specific export logic
            return None
                
        except Exception as e:
            logger.error(f"Error processing autofill usage logs export: {e}", exc_info=True)
            return None


class AITeammateUsageProcessor(UsageLogsTypeProcessor):
    """Processor for AITeammate usage logs type"""
    
    async def process_export(self, message: SQSExportMessage) -> Optional[BytesIO]:
        """Process AITeammate usage logs export"""
        try:
            logger.info("AITeammate usage logs export processor - placeholder implementation")
            # TODO: Implement AI teammate usage logs specific export logic
            return None
                
        except Exception as e:
            logger.error(f"Error processing AITeammate usage logs export: {e}", exc_info=True)
            return None


class UsageLogsTypeFactory:
    """Factory for creating usage logs type processors"""
    
    _processors = {
        UsageLogsType.ANSWER.value: AnswerProcessor,
        UsageLogsType.AUTOFILL.value: AutofillProcessor,
        UsageLogsType.AI_TEAMMATE.value: AITeammateUsageProcessor,
    }
    
    @classmethod
    def create_processor(cls, type_name: str, sub_type: str) -> Optional[UsageLogsTypeProcessor]:
        """Create usage logs type processor based on type and subtype"""
        try:
            processor_class = cls._processors.get(type_name)
            if processor_class:
                return processor_class()
            
            logger.error(f"No processor found for usage logs type: {type_name}")
            return None
            
        except Exception as e:
            logger.error(f"Error creating usage logs type processor for {type_name}: {e}", exc_info=True)
            return None
    
    @classmethod
    def register_processor(cls, type_name: str, processor_class: type):
        """Register new usage logs type processor"""
        cls._processors[type_name] = processor_class 