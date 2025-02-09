from collections import namedtuple


def get_fake_context(run_id: str, tags: str = []):
    return namedtuple("DagsterContextMock", ["run", "op_def"])(
        namedtuple("Run", ["run_id"])(run_id), namedtuple("OpDef", ["tags"])(tags)
    )
