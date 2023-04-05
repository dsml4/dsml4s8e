from dag import dagstermill_pipeline

from pathlib import Path
from dagster import reconstructable
from dagster._core.execution.api import execute_pipeline
from dagster._cli.utils import get_instance_for_service
import yaml
from yaml.loader import SafeLoader
import sys


if __name__ == "__main__":
    # do_execute_command(pipeline, instance, config, mode, tags, solid_selection, preset)
    sys.argv[1]
    print(sys.argv[1])
    with open('nb_2.yaml') as f:
        config = yaml.load(f, Loader=SafeLoader)

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

        storage_dict = f'{instance.root_directory}/storage'
        nb_name = list(config['ops'].keys())[0]
        output_var_name = list(
            results.pipeline_def.graph.node_dict['nb_2'].output_dict.keys()
            )[0]

        local_alias = output_var_name[len('url_') + len(nb_name) + len('_'):]

        (res_output_values,
         output_values
         ) = (
            results.node_result_list[0].output_values[output_var_name],
            f'{storage_dict}/{results.run_id}/{nb_name}/{local_alias}'
        )

        print(res_output_values == output_values)
        print(results.success)
