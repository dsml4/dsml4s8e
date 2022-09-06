from abc import ABC, abstractmethod


class StorageABC(ABC):
    @property
    @abstractmethod
    def url_prefix(self):
        ...

    @abstractmethod
    def catalog_validation(self) -> bool:
        ...


class LoacalStorage(StorageABC):

    def __init__(self, prefix):
        self._prefix = prefix

    @property
    def url_prefix(self):
        return self._prefix

    def catalog_validation(self) -> bool:
        return True
