from pathlib import Path

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
    success_hook,
    HookContext,
)
from dagstermill.manager import MANAGER_FOR_NOTEBOOK_INSTANCE

from dsml4s8e.job_composition import NbsJobComposition
from dsml4s8e.nb_op import NbOp


_root_path = Path(__file__).parent.parent


class PathExistIO(IOManager):
    def handle_output(self, context, obj):
        """
        warning if not exist path
        """
        context.log.info(obj)

    def load_input(self, context):
        # context.asset_key
        return "context.asset_key.to_string()"


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
    return RunConfig(
        ops={
            "nb_0": {"config": {"a": 1}},
            "nb_1": {"config": {"a": 1}},
            "nb_2": {"config": {"b": 2}},
        }
        # import dg_cfg
        # ops= {
        #         "nb_0": dg_cfg.NB0Cfg(a=1),
        #         "nb_1": dg_cfg.NB1Cfg(a=1),
        #         "nb_2": dg_cfg.NB2Cfg(b=2),
        #     }
        # )
    )


my_custom_path_fs_io_manager = fs_io_manager.configured(
    {"base_dir": "/home/jovyan/work/daghome/storage/test"}
)


@job(
    name="nb_pipeline",
    tags={
        "cdlc_stage": "dev",
    },
    resource_defs={
        "output_notebook_io_manager": local_output_notebook_io_manager,
        "io_manager": PathExistIO(),
    },
    metadata=nbs_job_composition.metadata,
    config=simplified_config,
)
def nb_pipeline():
    nbs_job_composition.do_compositioin()


defs = Definitions(
    jobs=[nb_pipeline],
    resources={
        "output_notebook_io_manager": local_output_notebook_io_manager,
        # "io_manager": my_custom_path_fs_io_manager,
    },
)


if __name__ == "__main__":
    context = MANAGER_FOR_NOTEBOOK_INSTANCE.context
    j = reconstructable(nb_pipeline)
    res = execute_job(
        job=j, instance=DagsterInstance.get(), run_config={"a": 110, "b": 22}
    )
    print(res.run_id)
