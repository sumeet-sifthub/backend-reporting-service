from abc import ABC, abstractmethod
from typing import Optional, Union, Dict
from io import BytesIO

from sifthub.reporting.models.export_models import SQSExportRequest


class UsageLogsTypeProcessor(ABC):
    """Base class for usage logs type processors with streaming support"""
    
    @abstractmethod
    async def process_export(self, message: SQSExportRequest) -> Optional[Union[BytesIO, Dict[str, str]]]:
        """Process export for usage logs type - returns either BytesIO (legacy) or Dict (streaming)"""
        pass 