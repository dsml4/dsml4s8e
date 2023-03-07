from collections import defaultdict
from typing import DefaultDict
from dataclasses import make_dataclass
from os.path import exists
import json

_storage_cfg_json = '/home/jovyan/work/storage.json'
if exists(_storage_cfg_json):
    with open(_storage_cfg_json) as f:
        storage_cfg = json.load(f)
        _storage_url_prefix = storage_cfg['prefix']
else:
    _storage_url_prefix = '/home/jovyan/data'


def _reduce_leafs(
        path_leaf_dict: dict[str: object]
        ) -> DefaultDict[str, list]:
    """
    path_leaf_dict:
    {
     'pip1.comp1.nb1.entity1': 'url_entity1',
     'pip1.comp1.nb2.entity2': 'url_entity2',
     'pip2.comp2.nb2.entity2': 'url_entity2',
     'pip2.comp2.nb2.entity3': 'url_entity3'
     }
     output dict:
     defaultdict(list,
            {'pip1.comp1.nb1': [('entity1', 'url_entity1')],
             'pip1.comp1.nb2': [('entity2', 'url_entity2')],
             'pip2.comp2.nb2': [('entity2', 'url_entity2'),
                                ('entity3', 'url_entity3')]
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


class_urls_name = 'ResourceUrls'


def _pack_leafs_list_to_obj(
        d_in: DefaultDict[str, list]
        ) -> dict[str, object]:
    """
    In input dict the leaves at the end of the branch stores in a list.
    in output dict the leaves are packed to object.
    d_in:  {'pip1.comp1.nb1': [('entity1', 'url_entity1')]}
    d_out: {'pip1.comp1.nb1': Pip1Comp1Nb1(entity1='url_entity1')}
    """
    d_out = {}
    for k in d_in:
        if k == '':
            cname = class_urls_name
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


def make_data_obj_urls_from_dict(path_leaf_dict: dict[str: str]) -> object:
    if not path_leaf_dict:
        return None
    path_leaf_obj = path_leaf_dict
    while type(path_leaf_obj).__name__ != class_urls_name:
        path_leaf_obj = _pack_leafs_list_to_obj(
            _reduce_leafs(path_leaf_obj))
    return path_leaf_obj


def data_key2url(
        key: str,
        cdlc_stage: str,
        run_id: str,
        prefix: str
        ) -> str:
    """
    format of data_key: <pipeline>.<component>.<notebook>.<name_data_opj>
    cdlc_stage is a stage of component development life cycle: dev, test, ops
    url: <prefix>/<pipeline>/<component>/<notebook>/<name_data_opj>
    """
    p, c, nb, e = key.split('.')
    return f'{prefix}/{cdlc_stage}/{p}/{c}/{run_id}/{nb}/{e}'


def url2data_key(url: str) -> str:
    p = url.split('/')
    return '.'.join((p[-5], p[-4], p[-2], p[-1]))


def get_urls_from_local(local_vars: dict,
                        res_id: list[str]) -> dict:
    short_names_set = set([name.split('.')[-1] for name in res_id])
    res = {url2data_key(local_vars[var_name]): url
           for var_name, url in local_vars.items()
           if var_name in short_names_set}
    if set(res.keys()) == set(res_id):
        return res
    return None
