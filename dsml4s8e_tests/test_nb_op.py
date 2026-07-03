import dagster as dg
from dsml4s8e.nb_op import NbOp, standalone_context


def test_catalog():
    nb_op = NbOp(
        context=standalone_context(),
        ins=["data1"],
        outs=["data2"],
    )
    nb_op.catalog.ins.data1
    nb_op.catalog.outs.data2
    assert True
