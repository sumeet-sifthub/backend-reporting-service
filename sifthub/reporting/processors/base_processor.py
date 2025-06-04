from abc import ABC, abstractmethod
from typing import Optional
from io import BytesIO

from sifthub.reporting.models.export_models import SQSExportRequest


class ModuleProcessor(ABC):

    @abstractmethod
    async def process_export(self, message: SQSExportRequest) -> Optional[BytesIO]:
        # Process export for the specific module.
        raise NotImplementedError
    
    @abstractmethod
    def get_export_filename(self, message: SQSExportRequest) -> str:
        # Generate filename for the export
        raise NotImplementedError 