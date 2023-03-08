from dsml4s8e import storage_catalog


def test_get_in_urls():
    local_vars = {
        'comp1_nb1_data1': 'data1_url'
    }
    dag_op_ins = {
        'pip1.comp1.nb1.data1': 'comp1_nb1_data1'
    }
    d = storage_catalog._get_in_urls(
        local_vars=local_vars,
        op_parameters_ins=dag_op_ins
    )
    assert (d['pip1.comp1.nb1.data1'] == 'data1_url')
