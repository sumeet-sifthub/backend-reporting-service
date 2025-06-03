from typing import Optional

from sifthub.reporting.models.export_models import ExportModule
from sifthub.reporting.processors.base_processor import ModuleProcessor
from sifthub.reporting.processors import insights_processor, usage_logs_processor
from sifthub.utils.logger import setup_logger

logger = setup_logger()

# Module processor mapping
_processors = {
    ExportModule.INSIGHTS: insights_processor,
    ExportModule.USAGE_LOGS: usage_logs_processor,
}


async def get_module_processor(module: ExportModule) -> Optional[ModuleProcessor]:
    try:
        processor = _processors.get(module)
        if processor:
            return processor
        logger.error(f"No processor found for module: {module}")
        return None
    except Exception as e:
        logger.error(f"Error getting module processor for {module}: {e}", exc_info=True)
        return None 