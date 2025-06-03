from abc import ABC, abstractmethod
from typing import Optional
from io import BytesIO

from sifthub.reporting.models.export_models import SQSExportMessage, InsightsType, InsightsSubType
from sifthub.utils.logger import setup_logger

logger = setup_logger(__name__)


class InsightsTypeProcessor(ABC):
    """Abstract base class for insights type processors"""
    
    @abstractmethod
    async def process_export(self, message: SQSExportMessage) -> Optional[BytesIO]:
        """Process export for the specific insights type"""
        pass


class ResponseGenerationProcessor(InsightsTypeProcessor):
    """Processor for responseGeneration insights type"""
    
    async def process_export(self, message: SQSExportMessage) -> Optional[BytesIO]:
        """Process responseGeneration export"""
        try:
            if message.subType == InsightsSubType.FREQUENT_ASKED_QUESTIONS.value:
                from sifthub.reporting.excel_generators.insights_faq_excel_generator import InsightsFAQExcelGenerator
                
                generator = InsightsFAQExcelGenerator()
                return await generator.generate_excel(message)
            else:
                logger.error(f"Unsupported subType for responseGeneration: {message.subType}")
                return None
                
        except Exception as e:
            logger.error(f"Error processing responseGeneration export: {e}", exc_info=True)
            return None


class ProjectCollaborationProcessor(InsightsTypeProcessor):
    """Processor for projectCollaboration insights type"""
    
    async def process_export(self, message: SQSExportMessage) -> Optional[BytesIO]:
        """Process projectCollaboration export"""
        try:
            logger.info("ProjectCollaboration export processor - placeholder implementation")
            # TODO: Implement project collaboration specific export logic
            return None
                
        except Exception as e:
            logger.error(f"Error processing projectCollaboration export: {e}", exc_info=True)
            return None


class AITeammateProcessor(InsightsTypeProcessor):
    """Processor for AITeammate insights type"""
    
    async def process_export(self, message: SQSExportMessage) -> Optional[BytesIO]:
        """Process AITeammate export"""
        try:
            logger.info("AITeammate export processor - placeholder implementation")
            # TODO: Implement AI teammate specific export logic
            return None
                
        except Exception as e:
            logger.error(f"Error processing AITeammate export: {e}", exc_info=True)
            return None


class InsightsTypeFactory:
    """Factory for creating insights type processors"""
    
    _processors = {
        InsightsType.RESPONSE_GENERATION.value: ResponseGenerationProcessor,
        InsightsType.PROJECT_COLLABORATION.value: ProjectCollaborationProcessor,
        InsightsType.AI_TEAMMATE.value: AITeammateProcessor,
    }
    
    @classmethod
    def create_processor(cls, type_name: str, sub_type: str) -> Optional[InsightsTypeProcessor]:
        """Create insights type processor based on type and subtype"""
        try:
            processor_class = cls._processors.get(type_name)
            if processor_class:
                return processor_class()
            
            logger.error(f"No processor found for insights type: {type_name}")
            return None
            
        except Exception as e:
            logger.error(f"Error creating insights type processor for {type_name}: {e}", exc_info=True)
            return None
    
    @classmethod
    def register_processor(cls, type_name: str, processor_class: type):
        """Register new insights type processor"""
        cls._processors[type_name] = processor_class 