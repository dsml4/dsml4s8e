from dag import dagstermill_pipeline

from pathlib import Path
from dagster import reconstructable
from dagster._core.execution.api import execute_pipeline
from dagster._cli.utils import get_instance_for_service


if __name__ == "__main__":
    # do_execute_command(pipeline, instance, config, mode, tags, solid_selection, preset)
    config = {
        'ops': {
            'nb_2': {
                'config': {
                    'b': 300},
                'inputs': {
                    'url_nb_1_data1': '/home/jovyan/work/dagster_home/storage/f95df29a-95b9-4eb9-9b51-fb10c8cb5f7c/nb_1/data1'
                    }
                }
            }
        }

    with get_instance_for_service("``dagster job execute``") as instance:
        pipeline = reconstructable(dagstermill_pipeline)
        results = execute_pipeline(
            pipeline=pipeline,
            run_config=config,
            instance=instance,
            solid_selection=['nb_2'],
        )
        print(results)
        print(results.run_id)
        print(results.node_result_list[0].output_values['url_nb_2_data2'])
        print(results.success)
