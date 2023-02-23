def get_op_config(dag_op_params):
    return {
        k: v.default_value for k, v in
        dag_op_params['config_schema'].items()
    }

