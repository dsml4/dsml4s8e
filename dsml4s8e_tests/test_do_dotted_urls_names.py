from dsml4s8e import dotted_urls_names


def test_reduce_leafs():
    id_urls = {
        'pip1.comp1.nb1.entity1': 'url_entity1',
        'pip1.comp1.nb2.entity2': 'url_entity2',
        'pip2.comp2.nb2.entity2': 'url_entity2',
        'pip2.comp2.nb2.entity3': 'url_entity3'
    }
    res = dotted_urls_names._reduce_leafs(id_urls)
    assert (res['pip1.comp1.nb1'] == [('entity1', 'url_entity1')])
    assert (res['pip1.comp1.nb2'] == [('entity2', 'url_entity2')])
    assert (res['pip2.comp2.nb2'] == [('entity2', 'url_entity2'),
                                      ('entity3', 'url_entity3')])


def test_pack_leafs_list_to_obj():
    id_urls = {
        'pip1.comp1.nb1.entity1': 'url_entity1',
        'pip1.comp1.nb2.entity2': 'url_entity2',
        'pip2.comp2.nb2.entity2': 'url_entity2',
        'pip2.comp2.nb2.entity3': 'url_entity3'
    }
    path_entitie_list = dotted_urls_names._reduce_leafs(id_urls)
    path_entitie_obj = dotted_urls_names._pack_leafs_list_to_obj(path_entitie_list)
    assert (path_entitie_obj['pip2.comp2.nb2'].entity2 == 'url_entity2')
    obj_class_name = type(path_entitie_obj['pip2.comp2.nb2']).__name__
    assert (obj_class_name == 'Pip2Comp2Nb2')


def test_do_dotted_urls_names():
    key_urls = {
        'pip1.comp1.nb1.entity1': 'url_entity1',
        'pip1.comp1.nb2.entity2': 'url_entity2',
        'pip2.comp2.nb2.entity2': 'url_entity2',
        'pip2.comp2.nb2.entity3': 'url_entity3'
    }
    comp_urls = dotted_urls_names.do_dotted_urls_names(key_urls)
    assert (comp_urls.pip1.comp1.nb1.entity1 == 'url_entity1')
    assert (comp_urls.pip1.comp1.nb2.entity2 == 'url_entity2')
    assert (comp_urls.pip2.comp2.nb2.entity2 == 'url_entity2')
    assert (comp_urls.pip2.comp2.nb2.entity3 == 'url_entity3')

    assert (not dotted_urls_names.do_dotted_urls_names([]))
