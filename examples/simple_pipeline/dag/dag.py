from dsml4s8e.op_params_from_nb import dagstermill_op_params_from_nb
from dagstermill import define_dagstermill_op, local_output_notebook_io_manager
from dagster import (
    job,
    op,
    In
)
from pathlib import Path


def full_path(nb_name: str) -> str:
    p = Path(__file__).parent.parent
    return str(p.joinpath(p, nb_name))

'''
op_0_params = dagstermill_op_params_from_nb(full_path("data_load/nb_0.ipynb"))
op0 = define_dagstermill_op(**op_0_params,
                            save_notebook_on_failure=True)

op_1_params = dagstermill_op_params_from_nb(full_path("data_load/nb_1.ipynb"))
op1 = define_dagstermill_op(**op_1_params,
                            save_notebook_on_failure=True)

op_2_params = dagstermill_op_params_from_nb(full_path("data_load/nb_2.ipynb"))
op2 = define_dagstermill_op(**op_2_params,
                            save_notebook_on_failure=True)

'''


def define_nb_op(nb_path: str):
    params = dagstermill_op_params_from_nb(full_path(nb_path))
    return define_dagstermill_op(**params,
                                 save_notebook_on_failure=True)



@job(
    name='simple_pipeline',
    tags={"cdlc_stage": "dev"},
    resource_defs={
        "output_notebook_io_manager": local_output_notebook_io_manager,
    }
)
def dagstermill_pipeline():
    nbs = [
        "data_load/nb_0.ipynb",
        "data_load/nb_1.ipynb",
        "data_load/nb_2.ipynb"
    ]
    ops = [define_nb_op(nb) for nb in nbs]
    out_urls_op0 = ops[0]()
    out_urls_op1 = ops[1]()

    # ins={'simple_pipeline.data_load.nb_1.data1': 'url_nb_1_data1',
    #      'simple_pipeline.data_load.nb_1.data3': 'url_nb_1_data3'
    print(list(ops[0].outs.keys()))
    print(ops[2].positional_inputs)
    f_params = [out_urls_op1[0],
                out_urls_op1[2],
                out_urls_op0[0]
                ]
    res = ops[2](*f_params)
    print(res)
    #final_op(*res[:-1])
    # start()
