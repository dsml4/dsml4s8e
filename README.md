
# Dsml4s8e
**D**ata **S**cience / **ML** flow(**4**) **s**tandalon(**8**)**e**
**Dsml4s8e** is a python library that aims to extend Dagster to: 

 1. Simplify building of pipelines from **standalon** notebooks
 2. Standardize a structure of ML/DS pipeline projects to easy share and continuous improve them
 3. Manage pipelines data in a process of pipelines continuous improvement

Dsml4s8e designed to support the following workflow:
 1. Define a project structure for your pipeline and a structure of the pipeline data catalog
 2. Develop **standalon** Jupyter notebooks with **clear interface**
 3. Build a **pipeline** and deloy in vary environments(experimental/test/prod) and on vary infrastructure
 4. Configure and run the certaine version of pipeline many times in vary environments and on vary infrastructure 

Dsml4s8e proper for build a cloud agnostic DSML platform.

You can play with the demo pipeline skelet project:

https://github.com/dsml4/pipeline_skelet

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
docker run --rm --name my_dag -p 3000:3000 -p 8888:8888 -v $(pwd):/home/jovyan/work -e DAGSTER_HOME=/home/jovyan/work/daghome dsml4s8e bash work/dsml4s8e/setup_pipeline.sh
```

Open JupyterLab in a browser: http://localhost:8888/lab

Open Dagster in a browser: http://localhost:3000/

<img width="1076" alt="dagstermill_pipeline" src="https://user-images.githubusercontent.com/1010096/228894074-704a1484-519b-43f4-ba5c-194b6a81bb8a.png">


## The standalone notebook specification

A standalone notebook is a main building block in our pipelines building flow.

To do a notebook capable to be loaded into dagster pipeline we need to add 3 specific cells to the notebook. Next, we will discuss what concerns are addressed each of the cells and what library classes are responsible for each one.

<img width="1108" alt="mandatory_cells_4" src="https://user-images.githubusercontent.com/1010096/228894923-e9711a62-9bcc-4b78-ba78-ad4224eae0b8.png">


### A cell with the op_parameters tag

A cell tagged **'op_parameters'** responses for an integration of a standalone notebook with Dagster. In the load stage dictionary of parameters from this cell is trasformed to dagstermill format and passed to a function define_dagstermill_op from dagstermill library to make parameters avalible in Dagster Launchpad to edit run configuration in the launge stage.

Definition of a dagster operation in a standalone notebook:

<img width="827" alt="op_parameters_cell" src="https://user-images.githubusercontent.com/1010096/228895556-c88742e3-59ed-419e-b0a1-d9811300d624.png">

Launchpad:

<img width="846" alt="notebook_op_in_launchpad" src="https://user-images.githubusercontent.com/1010096/228895622-989dc0cb-d7d5-458f-96ff-12b15310ddff.png">


### A cell with the parameters tag

A cell tagged **'parameters'** responses for setting a notebook run configuration.


The function
```python 
get_context
```
set default values from config_schema in cell 'op_parameters' to context
```python
context = op.get_context()
```

If a notebook is run by dagster then it replace 'parameter' cell with 'injected-parameters' cell.

<img width="1441" alt="Runs_results" src="https://user-images.githubusercontent.com/1010096/228903557-74ae28fe-e8e4-4b09-8734-e5e0491c4053.png">


<img width="1518" alt="injected" src="https://user-images.githubusercontent.com/1010096/228903342-c6d0681d-f2d1-44e6-92f3-75fc97da5c82.png">


### A cell with urls interface 

The pipeline data catalog

LocalStorage Catalog is a class responsible for a structure of a data catalog, but a structure is fully customizable using StorageCatalogABC.

'/home/jovyan/data/dev/pipeline_example/data_load/9e4d0418-b711-4b84-83e3-28bcd521c260/nb_1/data1'

 * /home/jovyan/data/ - prefix
 * dev/ - stage in development life cycle
 * pipeline_example/ - pipeline name
 * data_load/ - component name 
 * 9e4d0418-b711-4b84-83e3-28bcd521c260/ - run_id
 * nb_1/ - notebook name
 * data1 - data object

### A cell responsible for outs passing to the next step

```python
op.pass_outs_to_next_step()
```

## A dagster pipleline code

```python
from dsml4s8e.op_params_from_nb import dagstermill_op_params_from_nb
from dagstermill import define_dagstermill_op, local_output_notebook_io_manager
from dagster import (
    job,
    op,
    In
)
from pathlib import Path


def full_path(path: str) -> str:
    return str(Path(path).resolve())


op_1_params = dagstermill_op_params_from_nb(full_path("../data_load/nb_1.ipynb"))
op1 = define_dagstermill_op(**op_1_params,
                            save_notebook_on_failure=True)

op_2_params = dagstermill_op_params_from_nb(full_path("../data_load/nb_2.ipynb"))
op2 = define_dagstermill_op(**op_2_params,
                            save_notebook_on_failure=True)


@op(
    description="""end op""",
    ins={
        "data2": In(str),
    },
    out={},
)
def final_op(context, data2: str):
    context.log.info(data2)
    # context.log.info(output_notebook_name)


@job(
    name='simple_pipeline',
    tags={"cdlc_stage": "dev"},
    resource_defs={
        "output_notebook_io_manager": local_output_notebook_io_manager,
    }
)
def dagstermill_pipeline():
    res_urls = op1()
    res = op2(*res_urls[:-1])
    final_op(*res[:-1])

```

As a result the followed dagster pipeline will be built from standalone notebooks:

![Job_simple_pipeline](https://user-images.githubusercontent.com/1010096/228895849-12a04f1b-a4c0-4bdf-ab88-779d8e84d3b7.svg)



  
