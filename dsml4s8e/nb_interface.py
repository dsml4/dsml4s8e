from dataclasses import dataclass


from dsml4s8e.nb_data_keys import NotebookDataKeys, data_key2url_name
from dsml4s8e.storage_catalog import StorageCatalogABC
from dsml4s8e import dotted_urls_names
from dsml4s8e.dag_params import NbOpParams

from typing import Dict


@dataclass(frozen=True)
class NbDataUrl:
    ins: object
    outs: object


class NBInterface:
    def __init__(self,
                 locals_: Dict[str, dict],
                 nb_op_params: NbOpParams,
                 storage_catalog: StorageCatalogABC) -> None:
        nb_op_params.set_locals(locals_)
        op_params = nb_op_params.op_params
        self.nb_data_keys = NotebookDataKeys(
            ins_data_key_dag_name=op_params.get('ins', {}),
            outs=op_params.get('outs', []),
            op_id=nb_op_params.id
        )
        self._ins_dict = {}
        self._outs_dict = {}
        if 'ins' in op_params:
            self._ins_dict = nb_op_params.get_ins_data_urls(locals_)
        if 'outs' in op_params:
            self._outs_dict = storage_catalog.get_outs_data_urls(
                data_kyes=self.nb_data_keys.outs,
            )

    def get_data_urls(
            self
            ) -> NbDataUrl:
        return NbDataUrl(
            ins=dotted_urls_names.do_dotted_urls_names(self._ins_dict),
            outs=dotted_urls_names.do_dotted_urls_names(self._outs_dict)
            )

    def validate(self):
        return True

    def send_outs_data_urls_to_next_step(self, yield_result):
        print('outs:')
        for data_obj_key, url in self._outs_dict.items():
            url_name = data_key2url_name(data_obj_key)
            print(f'{url_name} = "{url}"')
            yield_result(
                url,
                output_name=url_name
             )
