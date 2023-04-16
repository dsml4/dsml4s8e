
from dsml4s8e.op_params_from_nb import dagstermill_op_params_from_nb
from dsml4s8e.data_catalog import make_data_key
from pathlib import Path
from typing import Dict, List


class Node:
    def __init__(self,
                 name: str,
                 ins: List[str],
                 outs: List[str]
                 ):
        self.name: str = name
        self.ins: List[str] = ins
        self.outs: List[str] = outs
        self.childs: List[Node] = []


def find_nbs(root_p: str):
    nb_files = [nb for nb in Path(root_p).rglob('*.ipynb') if
                '.ipynb_checkpoints' not in str(nb)
                ]
    ops_params = []
    for f in nb_files:
        params = dagstermill_op_params_from_nb(str(f))
        print(f)
        if params:
            ops_params.append(params)
    return ops_params


def build_dag(root_path: str):
    import dsml4s8e.nb_op as op
    ops_params = find_nbs(root_path)
    top_lev_nb_params = {
        k: p for (k, p) in
        op.NbOp.ops.items() if 'ins' not in p
    }
    for op_param in ops_params:
        print(op_param.get('outs', []))
    ins_set = set()
    for nb_id, nb_params in top_lev_nb_params.items():
        print(nb_id)
        for o_var_name in nb_params.get('outs', []):
            ins_set.update([make_data_key(nb_id, o_var_name)])
    print(ins_set)
    # op1.outs [0: var1, 1: var2, 2: var3], op2.outs: [0: var4, 1: var5, 2: var6]
    #
    # op2.ins [0: var6, 1: var3, 2: var1]
    # op2.outs[2], op1.outs[2], op1.outs[0]


if __name__ == '__main__':
    build_dag('/home/jovyan/work/dev/dsml4s8e/examples/simple_pipeline')
