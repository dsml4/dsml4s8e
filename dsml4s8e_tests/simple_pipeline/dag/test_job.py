from dagster import reconstructable
from dagster._core.execution.api import execute_pipeline
from dagster._cli.utils import get_instance_for_service

from pathlib import Path
from .dag import dagstermill_pipeline


def test_job():
    config = {
        'ops': {
            'nb_2': {
                'config': {
                    'b': 300},
                'inputs': {
                    'path_nb_1_data1': 'uri value'
                    }
                }
            }
        }
    import os
    p = Path(__file__)
    os.chdir(p.parent.parent)
    print("Current working directory: {0}".format(os.getcwd()))
    with get_instance_for_service("``dagster job execute``") as instance:
        pipeline = reconstructable(dagstermill_pipeline)
        results = execute_pipeline(
            pipeline=pipeline,
            run_config=config,
            instance=instance,
            solid_selection=['nb_2'],
        )

        assert (results.success)

        storage_dict = f'{instance.root_directory}/storage'
        nb_name = list(config['ops'].keys())[0]
        output_var_name = list(
            results.pipeline_def.graph.node_dict['nb_2'].output_dict.keys()
            )[0]

        local_alias = output_var_name[len('path_') + len(nb_name) + len('_'):]

        (res_output_values,
         output_values
         ) = (
            results.node_result_list[0].output_values[output_var_name],
            f'{storage_dict}/{results.run_id}/{nb_name}/{local_alias}'
        )
        assert (res_output_values ==
                output_values)
