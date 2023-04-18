from dag import dagstermill_pipeline

from pathlib import Path
from dagster import reconstructable
from dagster._core.execution.api import execute_pipeline
from dagster._cli.utils import get_instance_for_service
import yaml
from yaml.loader import SafeLoader
import sys


if __name__ == "__main__":

    config = None
    # with open(sys.argv[1]) as f:
    #    config = yaml.load(f, Loader=SafeLoader)

    with get_instance_for_service("``dagster job execute``") as instance:
        pipeline = reconstructable(dagstermill_pipeline)
        results = execute_pipeline(
            pipeline=pipeline,
            run_config=config,
            instance=instance,
            # solid_selection=['nb_2'],
        )
        print(results)
        print(results.run_id)
        print(results.node_result_list[0].output_values['url_nb_1_data1'])
