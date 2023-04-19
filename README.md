
# Dsml4s8e
**D**ata **S**cience / **ML** flow(**4**) **s**tandalon(**8**)**e**
**Dsml4s8e** is a python library that aims to extend Dagster to: 

 1. Simplify building of pipelines from **standalone** notebooks
 2. Standardize a structure of ML/DS pipeline projects to easy share and continuous improve them
 3. Manage pipelines data in a process of pipelines continuous improvement

Dsml4s8e designed to support the following workflow:
 1. Define a project structure for your pipeline and a structure of the pipeline data catalog, see about `StorageCatalogABC`
 2. Develop **standalone** Jupyter notebooks with **clear interface**, see the specification below
 3. Build a **pipeline** and deloy in vary environments(experimental/test/prod) and on vary infrastructure
 4. Configure and execute the certain pipeline version many times in vary environments and on vary infrastructure 

Dsml4s8e proper for build a cloud agnostic DSML platform.

You can play with a demo pipeline skelet project:

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


<img width="1387" alt="simple_pipeline_Overview" src="https://user-images.githubusercontent.com/1010096/232596393-a7da68b5-9d17-4e78-bc85-123bf756976d.png">


## The standalone notebook specification

A standalone notebook is a main building block in our pipelines development flow.

To transform a standalone notebook to  Dagster Op(a pipeline step) we need to add **4** specific cells to the notebook. Next, we will discuss what concerns are addressed each of the cells and what library classes are responsible for each one.


<img width="945" alt="notebook_4_cell_specification" src="https://user-images.githubusercontent.com/1010096/232596564-99a1108d-cbcc-4ded-84fb-ba4068f1b24f.png">


### Cell 1: Op parameters defenition

In a cell with the tag `op_parameters` defines parameters which will be transformed and passed to [define_dagstermill_op](https://docs.dagster.io/integrations/dagstermill/reference#results-and-custom-materializations) as arguments on a job definition stage. 
On the Dagster job definition stage this cell will be called and parametrs needed to define Op will be passed from the cell 'op' variabel to the function `define_dagstermill_op` from the dagstermill library. Then, these parameters will be avalible in Dagster Launchpad to edit run configuration in the launge stage.

A definition of a Dagster op in a standalone notebook in JupyterLab:

<img width="982" alt="op_parameters" src="https://user-images.githubusercontent.com/1010096/232597083-9601c183-fbaf-4c98-ad8b-9e3d57e0b162.png">

Configure op in Dagster Launchpad:


<img width="808" alt="simple_pipeline" src="https://user-images.githubusercontent.com/1010096/232597525-36053177-5b58-4aa9-8668-6912c7c63036.png">


### Cell 2: Op context initialization

Define variabel with 'contex' name in a cell wiht the tag `parametrs`.

A method `get_context` passes default values from config_schema in cell `op_parameters` to `context` variable.
```python
context = op.get_context()
```

Now, if a notebook is executed by Dagster as an Op then it is replaced the parameters cell with the injected-parameters cell. Thus, the variable context can be used in the notebook body when the notebook  is executed in standalone mode and when the notebook is executed as Dagster Op.

To be clear, let's look at one of the notebooks executed by Dagster:

<img width="999" alt="runs" src="https://user-images.githubusercontent.com/1010096/232601657-a8e3788a-96a8-4043-a14d-77306f7318f4.png">

<img width="1001" alt="open_path" src="https://user-images.githubusercontent.com/1010096/232603425-b6db4e71-893d-4633-8713-f1feaf96ecbb.png">

You can notice that the injected-parameters cell in your output notebook defines a variable called context.

<img width="1180" alt="out_nb_2" src="https://user-images.githubusercontent.com/1010096/232603465-41f3f647-4898-439f-95de-f00af1ba8da8.png">


### Cell 3: data catalog initialization

A data catalog is a set of paths. A catalog path is a unique name of a data entity in pipelines namespaces. Catalog paths are used for linking ops into pipeline.
```python
op = NbOp(
    config_schema={
        "b": Field(
            int,
            description='this is the paramentr description',
            default_value=20
            )
    },
    ins={
         'simple_pipeline.data_load.nb_0.data0':'path_nb_0_data0',
         'simple_pipeline.data_load.nb_1.data1':'path_nb_1_data1',
         'simple_pipeline.data_load.nb_1.data3':'path_nb_1_data3',
         },
    outs=['data2']
)
```

In this code block, we use the `ins` argumet to link outputs produced by nb_0 and nb_1 with inputs of nb_2. 
The keys if the `ins` dict are catalog paths. And the values are names of paths variables which are used in a notebook.
Catalog paths and paths variables names are generated automatically by the `outs` list, the class `NbOp` is responsible for generating.

A name of a path variable must be unique for each notebook namespace where the variable is used, thus a name of path variable could be shorter than the corresponding catalog path.

The method `get_catalog()` use 'DagsterStorageCatalog' derived from `StorageCatalogABC` to create paths in a file system (storage) catalog. Creating of file system paths for each the data entity name in `outs` list must be implemented in the method `get_outs_data_paths`.

```python
class StorageCatalogABC(ABC):

    @abstractmethod
    def __init__(self, runid):
        self.runid = runid

    @abstractmethod
    def is_valid(self) -> bool:
        ...

    @abstractmethod
    def get_outs_data_paths(
        self,
        catalog: DataCatalogPaths
    ) -> Dict[str, str]:
        """
        A map of catalog paths to storage paths
        """
        ...
```

A catalog structure in file system is fully customizable using `StorageCatalogABC`.


<img width="951" alt="Initialize_catalog_outs" src="https://user-images.githubusercontent.com/1010096/232607728-c01acde4-bbbd-4bab-9cb6-25ab111fe172.png">


### Cell 4: passing notebook outputs to next pipeline steps

In the last cell we inform the next steps of the pipeline where data produced by the current notebook are stored. Now we can link this notebook(step) with the next notebooks representing pipeline steps. To do this we need to declare this notebook outputs as inputs for the next netbooks. 
```python
op.pass_outs_to_next_step()

```
Format of output message returning by `pass_outs_to_next_step()` allow as to copy strings from the cell output and paste them to `NbOp` declarations in the cells `op_parametrs` of the pipeline next steps notebooks.
Look at the illustration below:

<img width="1758" alt="copy_passte_ins_variables" src="https://user-images.githubusercontent.com/1010096/232604788-42a54dba-f098-47f5-ab5d-a8fbf90e0197.png">


## Dagster job definition step

A job is a set of notebooks arranged into a DAG.
A function `define_job` automate a Dagster job definition.

```python
# dag.py

from dsml4s8e.define_job import define_job
from dagstermill import local_output_notebook_io_manager
from dagster import job

from pathlib import Path


@job(
    name='simple_pipeline',
    tags={"cdlc_stage": "dev"},
    resource_defs={
        "output_notebook_io_manager": local_output_notebook_io_manager,
    }
)
def dagstermill_pipeline():
    module_path = Path(__file__)
    define_job(
        root_path=module_path.parent.parent,
        nbs_sequence=[
            "data_load/nb_0.ipynb",
            "data_load/nb_1.ipynb",
            "data_load/nb_2.ipynb"
        ]
    )


```

As a result, a dagster pipeline will be built from standalone notebooks:
  
![Simple Pipeline](https://user-images.githubusercontent.com/1010096/232598898-b536ec12-26da-4693-a4e9-ba15858164de.svg)
