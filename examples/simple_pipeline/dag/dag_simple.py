from dagster import (
    job,
    op,
    In,
    Out
)


@op(
    description="""start op""",
    ins={
    },
    out={
        "data1": Out(str),
    },
)
def start(context):
    import time
    time.sleep(3)
    context.log.info('log')
    # context.log.info(output_notebook_name)
    data1 = '1'
    return data1


@op(
    description="""end op""",
    ins={
        "data1": In(str),
    },
    out={},
)
def end(context, data1: str):
    context.log.info(data1)


@job(
    name='statr_end',
    tags={"cdlc_stage": "dev"}
)
def start_end():
    end(start())
