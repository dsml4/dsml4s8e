from dsml4s8e.op_params_from_nb import get_dagstermill_op_params
from dagstermill import define_dagstermill_op, local_output_notebook_io_manager
from dagster import (
    job,
    op,
    In
)
from pathlib import Path


def full_path(path: str) -> str:
    return str(Path(path).resolve())


op_1_params = get_dagstermill_op_params(full_path("../data_load/nb_1.ipynb"))
op1 = define_dagstermill_op(**op_1_params,
                            save_notebook_on_failure=True)

op_2_params = get_dagstermill_op_params(full_path("../data_load/nb_2.ipynb"))
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
    res_urls = op1()
    res = op2(*res_urls[:-1])
    final_op(*res[:-1])
    # start()


if __name__ == "__main__":
    result = dagstermill_pipeline.execute_in_process()
