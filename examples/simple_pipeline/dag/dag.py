from dsml4s8e.define_job import define_job
from dagstermill import local_output_notebook_io_manager
from dagster import job

from pathlib import Path


@job(
    name='simple_pipeline',
    tags={"cdlc_stage": "dev"},
    resource_defs={
        "output_notebook_io_manager": local_output_notebook_io_manager,
    }
)
def dagstermill_pipeline():
    module_path = Path(__file__)
    define_job(
        root_path=module_path.parent.parent,
        nbs_sequence=[
            "data_load/nb_0.ipynb",
            "data_load/nb_1.ipynb",
            "data_load/nb_2.ipynb"
        ]
    )
