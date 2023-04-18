from collections import defaultdict
from typing import DefaultDict
from dataclasses import make_dataclass


def _reduce_leafs(
        path_leaf_dict: dict[str: object]
        ) -> DefaultDict[str, list]:
    """
    path_leaf_dict:
    {
     'pip1.comp1.nb1.entity1': 'path_entity1',
     'pip1.comp1.nb2.entity2': 'path_entity2',
     'pip2.comp2.nb2.entity2': 'path_entity2',
     'pip2.comp2.nb2.entity3': 'path_entity3'
     }
     output dict:
     defaultdict(list,
            {'pip1.comp1.nb1': [('entity1', 'path_entity1')],
             'pip1.comp1.nb2': [('entity2', 'path_entity2')],
             'pip2.comp2.nb2': [('entity2', 'path_entity2'),
                                ('entity3', 'path_entity3')]
            })
    """
    d = defaultdict(list)
    for dot_sep_path in path_leaf_dict:
        path = dot_sep_path.split('.')
        k = '.'.join(path[:-1])
        d[k].append((path[-1], path_leaf_dict[dot_sep_path]))
    return d


def _camelize(name: str) -> str:
    return name[0].capitalize() + name[1:]


class_data_path = 'DataPath'


def _pack_leafs_list_to_obj(
        d_in: DefaultDict[str, list]
        ) -> dict[str, object]:
    """
    In input dict the leaves at the end of the branch stores in a list.
    in output dict the leaves are packed to object.
    d_in:  {'pip1.comp1.nb1': [('entity1', 'path_entity1')]}
    d_out: {'pip1.comp1.nb1': Pip1Comp1Nb1(entity1='path_entity1')}
    """
    d_out = {}
    for k in d_in:
        if k == '':
            cname = class_data_path
        else:
            path = k.split('.')
            cname = ''.join([_camelize(p) for p in path])
        fields = d_in[k]
        C = make_dataclass(
            cls_name=cname,
            fields=[(f[0], type(f[1])) for f in fields],
            frozen=True
        )
        obj = C(*[f[1] for f in fields])
        if k == '':
            return obj
        d_out[k] = C(*[f[1] for f in fields])
    return d_out


def do_dotted_paths(path_leaf_dict: dict[str: str]) -> object:
    if not path_leaf_dict:
        return None
    path_leaf_obj = path_leaf_dict
    while type(path_leaf_obj).__name__ != class_data_path:
        path_leaf_obj = _pack_leafs_list_to_obj(
            _reduce_leafs(path_leaf_obj))
    return path_leaf_obj
