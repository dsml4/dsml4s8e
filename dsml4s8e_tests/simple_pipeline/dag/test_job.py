from dagster import reconstructable
from dagster._core.execution.api import execute_job
from dagster import DagsterInstance, execute_job, job, reconstructable

from pathlib import Path
import os
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
    
    
    p = Path(__file__)
    os.chdir(p.parent.parent)
    print("Current working directory: {0}".format(os.getcwd()))
    instance = DagsterInstance.get()
    job = reconstructable(dagstermill_pipeline)

    with execute_job(
        job=job,
        run_config=config,
        instance=instance,
        op_selection=['nb_2'],
    ) as result:

        assert (result.success)

        storage_dict = f'{instance.root_directory}/storage'
        nb_name = list(config['ops'].keys())[0]
        output_var_name = next(
            iter(result.job_def.graph.node_dict['nb_2'].output_dict.keys())
            )

        local_alias = output_var_name[len('path_') + len(nb_name) + len('_'):]

        (res_output_values,
            output_values
            ) = (
            result.output_for_node("nb_2", output_var_name),
            f'{storage_dict}/{result.run_id}/{nb_name}/{local_alias}'
            )
        assert (res_output_values ==
                output_values)
