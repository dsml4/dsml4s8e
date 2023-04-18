from abc import ABC, abstractmethod
from typing import Dict
from dsml4s8e.data_catalog import DataCatalogPaths


class StorageCatalogABC(ABC):

    @abstractmethod
    def __init__(self, runid):
        self.runid = runid

    @abstractmethod
    def is_valid(self) -> bool:
        ...

    @abstractmethod
    def get_outs_data_paths(
        self,
        catalog: DataCatalogPaths
    ) -> Dict[str, str]:
        """
        A map of catalog paths to storage paths
        """
        ...
