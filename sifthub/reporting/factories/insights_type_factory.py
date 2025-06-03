from typing import Optional

from sifthub.reporting.insights_processors.base_insights_processor import InsightsTypeProcessor
from sifthub.reporting.models.export_models import InsightsType
from sifthub.reporting.insights_processors import response_generation_processor
from sifthub.utils.logger import setup_logger

logger = setup_logger()

# Insights type processor mapping
_processors = {
    InsightsType.RESPONSE_GENERATION.value: response_generation_processor,
}


async def get_insights_type_processor(type_name: str, sub_type: str) -> Optional[InsightsTypeProcessor]:
    """Get insights type processor based on type and subtype"""
    try:
        processor = _processors.get(type_name)
        if processor:
            return processor
        
        logger.error(f"No processor found for insights type: {type_name}")
        return None
        
    except Exception as e:
        logger.error(f"Error getting insights type processor for {type_name}: {e}", exc_info=True)
        return None 