from dag import dagstermill_pipeline

from pathlib import Path
from dagster import reconstructable
from dagster import DagsterInstance, execute_job, job, reconstructable
import yaml
from yaml.loader import SafeLoader
import sys
import os

if __name__ == "__main__":

    config = None
    # with open(sys.argv[1]) as f:
    #    config = yaml.load(f, Loader=SafeLoader)
    instance = DagsterInstance.get()
    job = reconstructable(dagstermill_pipeline)
    p = Path(__file__)
    print(p.parent)
    os.chdir(p.parent)
    print(os.getcwd())
    with execute_job(
        job=job,
        run_config=config,
        instance=instance,
        #op_selection=['nb_2'],
        ) as result:
        print(result)
        print(result.run_id)
        #result.output_for_node("nb_2", "url_nb_1_data1")
