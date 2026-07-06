
# Dsml4s8e

Dsml4s8e is a Python library that extends Dagster to help manage the DSML (Data Scinece and Machine Learning) workflow around Jupyter notebooks development.

**Dsml4s8e** addresses issues: 

 1. Building of pipelines from **standalone** notebooks
 2. Standardizing a structure of DS/ML pipeline projects to share and reproduce experiments easily
 3. Implementing CD principles in DS/ML applications development
 
Dsml4s8e designed to support the following workflow:
 1. Define a project structure and a structure of the pipeline data catalog for your pipeline by using class `Storage Catalog ABC`
 2. Develop **standalone** Jupyter notebooks corresponding specification requirements. All you need is add two cell in you notbooke to get the lib transform the set of notbookes into the Dagster Pipepline (SDML application).
 3. Difine a **pipeline** -- a sequence of notebooks and deloy the pipelien in vary environments(experimental/test/prod)
 4. Execute pipelines many times with different configurations in vary environments and on vary infrastructure


A job is a set of notebooks arranged into a DAG.
Dsml4s8e simplify jobs definition based on a set of Jupyter notebooks.
All you need to do is add two cells to your notebook to have the library transform the set of notebooks into the Dagster Pipeline (SDML application).

```python
# dag.py

from dsml4s8e.define_job import define_job
from dagstermill import local_output_notebook_io_manager
from dagster import job

from pathlib import Path


nbs_job_composition = NbsJobComposition(
    root_path=_root_path,
    nbs_sequence=[
        "data_load/nb_0.ipynb",
        "data_load/nb_1.ipynb",
        "data_load/nb_2.ipynb",
    ],
)

@job(
    name="nb_pipeline",
    tags={
        "cdlc_stage": "dev",
    },
    resource_defs={
        "output_notebook_io_manager": local_output_notebook_io_manager,
        "io_manager": PathExistIO(),
    },
    metadata=nbs_job_composition.metadata,
    config=simplified_config,
)
def nb_pipeline():
    nbs_job_composition.do_compositioin()


defs = Definitions(
    jobs=[nb_pipeline],
    resources={
        "output_notebook_io_manager": local_output_notebook_io_manager,
    },
)


```
In this code block, we use  `define_job` to automate a [dagstermil](https://docs.dagster.io/integrations/dagstermill/reference#notebooks-as-ops) job definition:
As a result, a dagster pipeline will be built from standalone notebooks:
  
![Simple Pipeline](https://user-images.githubusercontent.com/1010096/232598898-b536ec12-26da-4693-a4e9-ba15858164de.svg)

Dsml4s8e proper for build a cloud agnostic DSML platform.

You can play with a demo pipeline [skeleton project](https://github.com/dsml4/pipeline_skelet).

## Installation of local dev container
```bash
# Create a work directory to clone a git repository. The work directory will be mounted to the container
mkdir work
#Step into the work directory
cd work
# Clone repository
git clone https://github.com/dsml4/dsml4s8e.git
# Step into a derictory with a Dockerfile
cd dsml4s8e/images/dev
# Build a image
docker build -t dsml4s8e .
# Go back into the work directory to a correct using pwd command inside the next docker run instruction
cd ../../../
# Create and run a container staying in the work directory.
docker run --rm --name my_dag -p 3000:3000 -p 8888:8888 -v $(pwd):/home/jovyan/work -e DAGSTER_HOME=/home/jovyan/work/daghome dsml4s8e bash setup_pipeline.sh
```

Open JupyterLab in a browser: http://localhost:8888/lab

Open Dagster in a browser: http://localhost:3000/


<img width="1387" alt="simple_pipeline_Overview" src="https://user-images.githubusercontent.com/1010096/232596393-a7da68b5-9d17-4e78-bc85-123bf756976d.png">


## The standalone notebook specification

A standalone notebook is a main building block in our pipelines development flow.

To transform a standalone notebook to  Dagster Op(a pipeline step) we need to add **2** specific cells to the notebook. Next, we will discuss what concerns are addressed each of the cells and what library classes are responsible for each one.


<img width="945" alt="notebook_4_cell_specification" src="https://user-images.githubusercontent.com/1010096/232596564-99a1108d-cbcc-4ded-84fb-ba4068f1b24f.png">


### Cell 2: Op parameters defenition

In a cell with the tag `op_parameters` defines parameters which will be transformed and passed to [define_dagstermill_op](https://docs.dagster.io/integrations/dagstermill/reference#results-and-custom-materializations) as arguments on a job definition stage. 
On the Dagster job definition stage this cell will be called and parametrs needed to define Op will be passed from the cell 'op' variabel to the function `define_dagstermill_op` from the dagstermill library. Then, these parameters will be avalible in Dagster Launchpad to edit run configuration in the launge stage.

A definition of a Dagster op in a standalone notebook in JupyterLab:

<img width="982" alt="op_parameters" src="https://user-images.githubusercontent.com/1010096/232597083-9601c183-fbaf-4c98-ad8b-9e3d57e0b162.png">

Configure op in Dagster Launchpad:


<img width="808" alt="simple_pipeline" src="https://user-images.githubusercontent.com/1010096/232597525-36053177-5b58-4aa9-8668-6912c7c63036.png">


### Cell 1: Op context initialization

Define variabel with 'contex' name in a cell wiht the tag `parametrs`.
First we need create the standalone_context

```python
from dsml4s8e.nb_op import standalone_context
from my_cgf.dg_cfg import Nb0Cfg

context = standalone_context(Nb0Cfg(a=30))
print(context.job_name)
type(context.op_config)
```

Now, if a notebook is executed by Dagster as an Op then it is replaced the parameters cell with the injected-parameters cell. Thus, the variable context can be used in the notebook body when the notebook  is executed in standalone mode and when the notebook is executed as Dagster Op.

To be clear, let's look at one of the notebooks executed by Dagster:

<img width="999" alt="runs" src="https://user-images.githubusercontent.com/1010096/232601657-a8e3788a-96a8-4043-a14d-77306f7318f4.png">

<img width="1001" alt="open_path" src="https://user-images.githubusercontent.com/1010096/232603425-b6db4e71-893d-4633-8713-f1feaf96ecbb.png">

Notice that the injected-parameters cell in your output notebook defines a variable called context.

<img width="1180" alt="out_nb_2" src="https://user-images.githubusercontent.com/1010096/232603465-41f3f647-4898-439f-95de-f00af1ba8da8.png">




