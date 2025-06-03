from abc import ABC, abstractmethod
from typing import Optional
from io import BytesIO

from sifthub.reporting.models.export_models import SQSExportMessage


class ModuleProcessor(ABC):

    @abstractmethod
    async def process_export(self, message: SQSExportMessage) -> Optional[BytesIO]:
        # Process export for the specific module.
        raise NotImplementedError
    
    @abstractmethod
    def get_export_filename(self, message: SQSExportMessage) -> str:
        # Generate filename for the export
        raise NotImplementedError 