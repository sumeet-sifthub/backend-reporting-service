from typing import Optional

from sifthub.reporting.usage_logs_processors.base_usage_logs_processor import UsageLogsTypeProcessor
from sifthub.reporting.models.export_models import UsageLogsType
from sifthub.reporting.usage_logs_processors import (
    answer_processor,
    autofill_processor,
    ai_teammate_usage_processor
)
from sifthub.utils.logger import setup_logger

logger = setup_logger(__name__)

# Usage logs type processor mapping
_processors = {
    UsageLogsType.ANSWER.value: answer_processor,
    UsageLogsType.AUTOFILL.value: autofill_processor,
    UsageLogsType.AI_TEAMMATE.value: ai_teammate_usage_processor,
}


async def get_usage_logs_type_processor(type_name: str, sub_type: str) -> Optional[UsageLogsTypeProcessor]:
    """Get usage logs type processor based on type and subtype"""
    try:
        processor = _processors.get(type_name)
        if processor:
            return processor
        
        logger.error(f"No processor found for usage logs type: {type_name}")
        return None
        
    except Exception as e:
        logger.error(f"Error getting usage logs type processor for {type_name}: {e}", exc_info=True)
        return None 