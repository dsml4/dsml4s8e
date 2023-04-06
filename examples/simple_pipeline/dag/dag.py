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


op_0_params = dagstermill_op_params_from_nb(full_path("data_load/nb_0.ipynb"))
op0 = define_dagstermill_op(**op_0_params,
                            save_notebook_on_failure=True)

op_1_params = dagstermill_op_params_from_nb(full_path("data_load/nb_1.ipynb"))
op1 = define_dagstermill_op(**op_1_params,
                            save_notebook_on_failure=True)

op_2_params = dagstermill_op_params_from_nb(full_path("data_load/nb_2.ipynb"))
op2 = define_dagstermill_op(**op_2_params,
                            save_notebook_on_failure=True)


@op(
    description="""end op""",
    ins={
        "data2": In(str),
    },
    out={},
)
def final_op(context, data2: str):
    context.log.info(data2)
    # context.log.info(output_notebook_name)


@op(
    description="""start op""",
    ins={
    },
    out={},
)
def start(context):
    import time
    time.sleep(3)
    context.log.info('log')
    # context.log.info(output_notebook_name)


@job(
    name='simple_pipeline',
    tags={"cdlc_stage": "dev"},
    resource_defs={
        "output_notebook_io_manager": local_output_notebook_io_manager,
    }
)
def dagstermill_pipeline():
    out_urls_op0 = op0()
    out_urls_op1 = op1()

    # ins={'simple_pipeline.data_load.nb_1.data1': 'url_nb_1_data1',
    #      'simple_pipeline.data_load.nb_1.data3': 'url_nb_1_data3'

    f_params = [out_urls_op1[0],
                out_urls_op1[2],
                out_urls_op0[0]
                ]
    res = op2(*f_params)
    #final_op(*res[:-1])
    # start()
