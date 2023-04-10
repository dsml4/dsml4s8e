from dsml4s8e import dotted_catalog_path


def test_reduce_leafs():
    id_paths = {
        'pip1.comp1.nb1.entity1': 'path_entity1',
        'pip1.comp1.nb2.entity2': 'path_entity2',
        'pip2.comp2.nb2.entity2': 'path_entity2',
        'pip2.comp2.nb2.entity3': 'path_entity3'
    }
    res = dotted_catalog_path._reduce_leafs(id_paths)
    assert (res['pip1.comp1.nb1'] == [('entity1', 'path_entity1')])
    assert (res['pip1.comp1.nb2'] == [('entity2', 'path_entity2')])
    assert (res['pip2.comp2.nb2'] == [('entity2', 'path_entity2'),
                                      ('entity3', 'path_entity3')])


def test_pack_leafs_list_to_obj():
    id_paths = {
        'pip1.comp1.nb1.entity1': 'path_entity1',
        'pip1.comp1.nb2.entity2': 'path_entity2',
        'pip2.comp2.nb2.entity2': 'path_entity2',
        'pip2.comp2.nb2.entity3': 'path_entity3'
    }
    path_entitie_list = dotted_catalog_path._reduce_leafs(id_paths)
    path_entitie_obj = dotted_catalog_path._pack_leafs_list_to_obj(path_entitie_list)
    assert (path_entitie_obj['pip2.comp2.nb2'].entity2 == 'path_entity2')
    obj_class_name = type(path_entitie_obj['pip2.comp2.nb2']).__name__
    assert (obj_class_name == 'Pip2Comp2Nb2')


def test_do_dotted_paths():
    catalog_path_varas_map = {
        'pip1.comp1.nb1.entity1': 'path_entity1',
        'pip1.comp1.nb2.entity2': 'path_entity2',
        'pip2.comp2.nb2.entity2': 'path_entity2',
        'pip2.comp2.nb2.entity3': 'path_entity3'
    }
    comp_paths = dotted_catalog_path.do_dotted_paths(catalog_path_varas_map)
    assert (comp_paths.pip1.comp1.nb1.entity1 == 'path_entity1')
    assert (comp_paths.pip1.comp1.nb2.entity2 == 'path_entity2')
    assert (comp_paths.pip2.comp2.nb2.entity2 == 'path_entity2')
    assert (comp_paths.pip2.comp2.nb2.entity3 == 'path_entity3')

    assert (not dotted_catalog_path.do_dotted_paths([]))
