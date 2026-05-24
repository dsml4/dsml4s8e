from dsml4s8e.job_composition import NbsJobComposition
from dagstermill import local_output_notebook_io_manager
from dagster import (
    job,
    config_mapping,
    Config,
    RunConfig,
    Definitions,
    reconstructable,
    execute_job,
    DagsterInstance,
    fs_io_manager,
    IOManager,
)

from pathlib import Path

from dagstermill.manager import MANAGER_FOR_NOTEBOOK_INSTANCE

_root_path = Path(__file__).parent.parent


class PathIO(IOManager):
    def handle_output(self, context, obj):
        context.log.info(obj)

    def load_input(self, context):
        return "facke"


class SimplifiedConfig(Config):
    a: int
    b: int


nbs_job_composition = NbsJobComposition(
    root_path=_root_path,
    nbs_sequence=[
        "data_load/nb_0.ipynb",
        "data_load/nb_1.ipynb",
        "data_load/nb_2.ipynb",
    ],
)


@config_mapping
def simplified_config(val: SimplifiedConfig) -> RunConfig:
    # return RunConfig(
    #     ops={
    #         "nb_0": nbs_job_composition.ops_configs["nb_0"](a=val.a),
    #         "nb_1": nbs_job_composition.ops_configs["nb_1"](a=val.a),
    #         "nb_2": nbs_job_composition.ops_configs["nb_2"](b=val.b),
    #     }
    # )
    return RunConfig(
        ops={
            **nbs_job_composition.make_config(nb_name="nb_0", a=val.a),
            **nbs_job_composition.make_config(nb_name="nb_1", a=val.a),
            **nbs_job_composition.make_config(nb_name="nb_2", b=val.b),
        }
    )


my_custom_path_fs_io_manager = fs_io_manager.configured(
    {"base_dir": "/home/jovyan/work/daghome/storage/test"}
)


@job(
    name="dagstermill_pipeline",
    tags={
        "cdlc_stage": "dev",
    },
    resource_defs={
        "output_notebook_io_manager": local_output_notebook_io_manager,
        "io_manager": PathIO(),
    },
    metadata=nbs_job_composition.metadata,
    config=simplified_config,
)
def dagstermill_pipeline():
    nbs_job_composition.do_compositioin()


defs = Definitions(
    jobs=[dagstermill_pipeline],
    resources={
        "output_notebook_io_manager": local_output_notebook_io_manager,
        # "io_manager": my_custom_path_fs_io_manager,
    },
)


if __name__ == "__main__":
    context = MANAGER_FOR_NOTEBOOK_INSTANCE.context
    job = reconstructable(dagstermill_pipeline)
    res = execute_job(
        job=job, instance=DagsterInstance.get(), run_config={"a": 110, "b": 22}
    )
    print(res.run_id)
