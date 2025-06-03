from abc import ABC, abstractmethod
from typing import Optional
from io import BytesIO

from sifthub.reporting.models.export_models import SQSExportMessage


class UsageLogsTypeProcessor(ABC):
    """Abstract base class for usage logs type processors"""
    
    @abstractmethod
    async def process_export(self, message: SQSExportMessage) -> Optional[BytesIO]:
        """Process export for the specific usage logs type"""
        raise NotImplementedError 