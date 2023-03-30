from collections import namedtuple


def get_context_mock(run_id: str,
                     tags: str = []):
    return namedtuple(
        'DagsterContextMock',
        ['run', 'op_def'])(
            namedtuple('Run', ['run_id'])(run_id),
            namedtuple('OpDef', ['tags'])(tags)
    )
