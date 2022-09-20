from dagster import job, op, get_dagster_logger, Field, Out, In
import os
from pathlib import Path

from dsml4s8e.notebook_interface import NotebookInterface, extract_from_nb


def uniq_name(entity_id: str):
    name = entity_id.split('.')[-1]
    return name


class NbOpParams:
    def __init__(self, nb_id: str):
        wd = os.getcwd()
        nb_path = nb_id.replace('.', '/')
        self.nb_path = f'{wd}/{nb_path}.ipynb'
        self.compwd = str(Path(self.nb_path).parent)
        self.nbi, self.parameters = extract_from_nb(self.nb_path)
        self.out_ = {}
        self.ins_ = {}
        self.description = nb_id
        self.run_id = ''

    @property
    def out(self):
        if self.out_:
            return self.out_
        for a in self.nbi.entityIDs.artefacts:
            self.out_[uniq_name(a)] = Out(str)
        return self.out_

    @property
    def ins(self):
        if not self.ins_:
            return self.ins_
        for r in self.nbi.entityIDs.resources:
            self.ins_[uniq_name(r)] = In(str)
        return self.ins_


def get_op_params(nb_id: str):
    dag_params = {}
    dag_params['description'] = nb_id
    wd = os.getcwd()
    nb_path = nb_id.replace('.', '/')
    nb_path = f'{wd}/{nb_path}.ipynb'
    nb_path = nb_path
    nbi, parameters = extract_from_nb(nb_path)
    out = {}
    for a in nbi.entityIDs.artefacts:
        out[uniq_name(a)] = Out(str)
    if out:
        dag_params['out'] = out
    return dag_params
