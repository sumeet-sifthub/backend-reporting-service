from abc import ABC, abstractmethod
from typing import Optional
from io import BytesIO

from sifthub.reporting.models.export_models import SQSExportRequest


class InsightsTypeProcessor(ABC):
    """Abstract base class for insights type processors"""
    
    @abstractmethod
    async def process_export(self, message: SQSExportRequest) -> Optional[BytesIO]:
        """Process export for the specific insights type"""
        raise NotImplementedError 