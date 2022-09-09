from .. import urls


def test_reduce_leafs():
    id_urls = {
        'pip1.comp1.nb1.entity1': 'url_entity1',
        'pip1.comp1.nb2.entity2': 'url_entity2',
        'pip2.comp2.nb2.entity2': 'url_entity2',
        'pip2.comp2.nb2.entity3': 'url_entity3'
    }
    res = urls._reduce_leafs(id_urls)
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
    path_entitie_list = urls._reduce_leafs(id_urls)
    path_entitie_obj = urls._pack_leafs_list_to_obj(path_entitie_list)
    assert (path_entitie_obj['pip2.comp2.nb2'].entity2 == 'url_entity2')
    obj_class_name = type(path_entitie_obj['pip2.comp2.nb2']).__name__
    assert (obj_class_name == 'Pip2Comp2Nb2')


def test_make_urls():
    id_urls = {
        'pip1.comp1.nb1.entity1': 'url_entity1',
        'pip1.comp1.nb2.entity2': 'url_entity2',
        'pip2.comp2.nb2.entity2': 'url_entity2',
        'pip2.comp2.nb2.entity3': 'url_entity3'
    }
    comp_urls = urls.make_urls(id_urls)
    assert (comp_urls.pip1.comp1.nb1.entity1 == 'url_entity1')
    assert (comp_urls.pip1.comp1.nb2.entity2 == 'url_entity2')
    assert (comp_urls.pip2.comp2.nb2.entity2 == 'url_entity2')
    assert (comp_urls.pip2.comp2.nb2.entity3 == 'url_entity3')

    assert (urls.make_urls([]) == None)


def test_id2url():
    id_ = 'pip1.comp1.nb1.entity1'
    url = urls.id2url(id_, 'dev', 'run_id', '/data')
    assert (url == '/data/dev/pip1/comp1/run_id/nb1/entity1')
