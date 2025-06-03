from typing import Optional

from sifthub.reporting.delivery_processors.base_delivery_processor import DeliveryProcessor
from sifthub.reporting.models.export_models import ExportMode
from sifthub.reporting.delivery_processors import (
    download_delivery_processor
)
from sifthub.utils.logger import setup_logger

logger = setup_logger()

# Delivery processor mapping - email flow removed for now
_processors = {
    ExportMode.DOWNLOAD: download_delivery_processor,
}


async def get_delivery_processor(mode: ExportMode) -> Optional[DeliveryProcessor]:
    """Get delivery processor based on delivery mode"""
    try:
        processor = _processors.get(mode)
        if processor:
            return processor
        
        logger.error(f"No processor found for delivery mode: {mode}")
        return None
        
    except Exception as e:
        logger.error(f"Error getting delivery processor for {mode}: {e}", exc_info=True)
        return None 