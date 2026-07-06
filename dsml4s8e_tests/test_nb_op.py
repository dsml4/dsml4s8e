from pydantic import Field
import dagster as dg
from dsml4s8e.nb_op import NbOp, standalone_context


def test_catalog():
    nb_op = NbOp(
        context=standalone_context(),
        ins=["data1"],
        outs=["data2"],
    )
    nb_op.ins.data1
    nb_op.outs.data2
    assert True


class NbCfg(dg.Config):
    a: int = Field(description="this is the paramentr description", default=10)


def test_op_config():
    nb_op = NbOp(context=standalone_context(op_config=NbCfg(a=1)))
    assert nb_op.cfg.a == 1
    nb_op_0 = NbOp(context=standalone_context())
    assert nb_op_0.cfg is None
    assert True
