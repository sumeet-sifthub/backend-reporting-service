from abc import ABC, abstractmethod
from typing import Dict, Any, Union
from io import BytesIO

from sifthub.reporting.models.export_models import SQSExportRequest


class DeliveryProcessor(ABC):
    """Abstract base class for delivery processors"""
    
    @abstractmethod
    async def deliver_export(self, export_result: Union[BytesIO, Dict[str, str]], message: SQSExportRequest,
                             filename: str) -> Dict[str, Any]:
        """Deliver export file"""
        raise NotImplementedError 