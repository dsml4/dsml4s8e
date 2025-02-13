from dsml4s8e.job_composition import NbsJobComposition
from dagster._core.definitions.decorators.job_decorator import _Job
from .paths import SIMPLE_PIPELINE_PATH


def test_job_composition():
    job_comp = NbsJobComposition(
        root_path=SIMPLE_PIPELINE_PATH,
        nbs_sequence=[
            "component/nb_without_ins_outs.ipynb",
        ],
    )
    _Job(name="test_job_composition")(job_comp)
