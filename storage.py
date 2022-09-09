from abc import ABC, abstractmethod


class StorageABC(ABC):
    @property
    @abstractmethod
    def url_prefix(self):
        ...

    @abstractmethod
    def catalog_validation(self) -> bool:
        ...

    @abstractmethod
    def sava_artefact(self, path2atr: str, storage_url: str) -> bool:
        """
        Save a artefact from local storge with path2atr to shared storage
        return status
        """
        ...
