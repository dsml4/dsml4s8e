import pytest

from pathlib import Path
import dagster as dg

from dsml4s8e import define_dagstermill_op_kvargs_from_nb, MissingTagsException
from dsml4s8e.op_params_from_nb import create_nb_op_from_cell_source


@pytest.fixture
def tmp_src_nbs_path(tmp_path: Path) -> Path:
    d = tmp_path / "nb_src"
    d.mkdir()
    return d


@pytest.fixture
def nbs_path() -> Path:
    test_dir = Path(__file__).parent
    return test_dir / "notebooks"


def test_exec_cell_source():

    source_parameters = """
from pydantic import Field
from dsml4s8e.nb_op import standalone_context
import dagster as dg
from dsml4s8e.nb_op import NbOp
class Nb0Cfg(dg.Config):
    a: int = Field(description="this is the paramentr description", default=10)
context=standalone_context(Nb0Cfg(a=2))
"""

    source_op_parameters = """
class MyNbOp(NbOp):
    def create_catalog(self, names: list[str], run_id: str):
        super().create_catalog(names, run_id)
op = MyNbOp(
    context=context,
    ins=['data0'],
    outs=['data1']
)
    """
    varname, nb_op = create_nb_op_from_cell_source(
        source_parameters=source_parameters, source_op_parameters=source_op_parameters
    )
    define_dagstermill_op_kvargs = nb_op.op_params
    assert varname == "op"
    assert define_dagstermill_op_kvargs["ins"] == ["data0"]
    assert define_dagstermill_op_kvargs["outs"] == ["data1"]
    cfg = define_dagstermill_op_kvargs["config_schema"](a=2)
    assert isinstance(cfg, dg.Config)


def test_define_dagstermill_op_kvargs_from_nb(tmp_src_nbs_path: Path, nbs_path: Path):
    nb_path = nbs_path / "nb_with_params.ipynb"
    define_dagstermill_op_kvargs = define_dagstermill_op_kvargs_from_nb(
        nb_path=nb_path, tmp_src_nbs_path=tmp_src_nbs_path
    )
    define_dagstermill_op_kvargs["notebook_path"]
    assert True


def test_missing_nb_tags(nbs_path: Path, tmp_src_nbs_path):
    nb_path = nbs_path / "empty.ipynb"
    with pytest.raises(
        MissingTagsException,
        match=r"op_parameters",
    ):
        _ = define_dagstermill_op_kvargs_from_nb(
            nb_path=nb_path, tmp_src_nbs_path=tmp_src_nbs_path
        )
