from abc import ABC, abstractmethod
from typing import List, Dict
from .nb_data_keys import DataKeys


def _get_in_urls(
        local_vars: Dict[str, str],
        op_parameters_ins: Dict[str, str]
        ) -> Dict[str, str]:
    return {
        k: local_vars.get(k_alias, '')
        for k, k_alias in op_parameters_ins.items()
    }


class StorageCatalogABC(ABC):
    @property
    @abstractmethod
    def url_prefix(self):
        ...

    @abstractmethod
    def __init__(self, runid):
        self.runid = runid

    @abstractmethod
    def is_valid(self) -> bool:
        ...

    @abstractmethod
    def get_out_urls(self,
                     data_kyes: DataKeys
                     ) -> Dict[str, str]:
        """
        urls which have the prefix
        """
        ...

    def get_in_urls(self,
                    local_vars: Dict[str, str],
                    op_parameters_ins: Dict[str, str]
                    ) -> Dict[str, str]:
        """
        op_parameters_ins = op_parameters['ins']
        op_parameters is a dict from notebook cell with tag op_parameters
        local_vars = locals() # Local symbol Table
        'ins': {'key': 'nb_data1'} -> {key: local_vars['nb_data1']}
        """
        return _get_in_urls(local_vars, op_parameters_ins)
