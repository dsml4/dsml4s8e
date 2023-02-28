
# dsml4s8e
**D**ata **S**cience / **ML** flow(**4**) **s**tandalon(**8**)**e**


dsml4s8e is a python library that aims to:
 1. Simplify building of Dagster pipelines from **standalon** notebooks
 2. Standardize a structure of ML/DS pipeline projects to easy sharing
 3. Manage experiments data and artefacts in a continual improvement process.

It makes possible following workflow:
 1. Define a project structure for your pipeline and a structure of the pipeline data catalog
 2. Develop **standalon** Jupyter notebooks with **clear interface**
 3. Loade the notebooks into dagit (build a **pipeline**) and deloy in vary environments(experimental/test/prod) and on vary infrastructure
 4. Configure and run the certaine version of pipeline many times in vary environments and on vary infrastructure 

## A standalone notebook specification

To do a notebook capable to be loaded into dagster pipeline we need to add 3 specific cells to the notebook. Next, we will discuss what concerns are addressed each of the cells and what library classes are responsible for each one.

### op_parameters cell

A cell tagged **'op_parameters'** responses for an integration of a standalone notebook with Dagster. In the load stage dictionary of parameters from this cell is trasformed to dagstermill format and passed to a function define_dagstermill_op from dagstermill library to make parameters avalible in Dagster Launchpad to edit run configuration in the launge stage.

Standalone notebook:
![op_parameters_cell](https://user-images.githubusercontent.com/1010096/221004539-a13f6dac-056c-4633-94bf-ef995c857da8.png)

Launchpad:
![op_parameters_cell](https://user-images.githubusercontent.com/1010096/221832042-f1129a96-45eb-4678-a541-cab8c9e72c89.png)

### parameters cell

A cell tagged **'parameters'** responses for setting a notebook run configuration.

Variables declared in this cell are used in class NBInterface.
The function
```python 
get_context
```
set default values from config_schema in cell 'op_parameters' to context
```python
# pass context.op_config
context = dagstermill.get_context(op_config=
                                  dag_params.get_op_config(op_parameters)
                                 )
```

![nb_1](https://user-images.githubusercontent.com/1010096/221655435-3b01fb49-7ff9-4e53-82b8-ad0922fc2136.png)


A cell tagged **'parameters'** response for setup a notebook run configuration.

Variables declared in this cell are used in class NBInterface.
The function
```python 
get_context
```
set default values from config_schema in cell 'op_parameters' to context
```python
# pass context.op_config
context = dagstermill.get_context(op_config=
                                  dag_params.get_op_config(op_parameters)
                                 )
```
If the notebook is run by Dagster then it replace 'parameter' cell with 'injected-parameters' cell.


![injected](https://user-images.githubusercontent.com/1010096/221841608-8f0b51a9-c3ff-4152-a8f5-33feac4eb9aa.png)

### A pipeline data catalog

LocalStorage Catalog is a class responsible for a structure of a data catalog it is fully customizable.


## The pipleline code

```python
# in dag.py pipeline code

from dagstermill import define_dagstermill_op
from dsml4s8e.extract_dag_params_from_nb import get_dagstermill_op_params


op_1_params = get_dagstermill_op_params("/home/jovyan/work/dev/pipeline_example/data_load/nb_1.ipynb")
op1 = define_dagstermill_op(**op_1_params,
                            save_notebook_on_failure=True)


op_2_params = get_dagstermill_op_params("/home/jovyan/work/dev/pipeline_example/data_load/nb_2.ipynb")
op2 = define_dagstermill_op(**op_2_params,
                            save_notebook_on_failure=True)

```

Dagster pipeline built from standalone notebooks

![Job_dagstermill_pipeline](https://user-images.githubusercontent.com/1010096/221005076-7ba56646-8a9e-4d75-bbaf-e68ba04d036d.svg)


  
