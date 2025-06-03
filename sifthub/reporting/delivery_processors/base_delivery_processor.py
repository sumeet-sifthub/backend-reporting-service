from abc import ABC, abstractmethod
from typing import Dict, Any
from io import BytesIO

from sifthub.reporting.models.export_models import SQSExportMessage


class DeliveryProcessor(ABC):
    """Abstract base class for delivery processors"""
    
    @abstractmethod
    async def deliver_export(self, file_stream: BytesIO, message: SQSExportMessage, 
                           filename: str) -> Dict[str, Any]:
        """Deliver export file"""
        raise NotImplementedError 