from dataclasses import dataclass
from pathlib import Path
# TODO: project_stucture_abc


@dataclass(frozen=True)
class Project:
    pipeline: str
    component: str
    notebook: str


def get_project_structure(full_name_nb: str = '') -> Project:
    if not full_name_nb:
        import ipynbname
        path = ipynbname.path()
    else:
        path = Path(full_name_nb)
    parts = path.parts
    return Project(
        pipeline=parts[-3],
        component=parts[-2],
        notebook=path.stem
    )

