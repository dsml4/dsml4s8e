from abc import ABC, abstractmethod
from typing import List


class StorageCatalogABC(ABC):
    @property
    @abstractmethod
    def url_prefix(self):
        ...

    @abstractmethod
    def is_valid(self) -> bool:
        ...

    @abstractmethod
    def get_urls(self, prefix: str) -> List[str]:
        """
        urls which have the prefix
        """
        ...
