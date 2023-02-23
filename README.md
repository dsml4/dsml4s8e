
# dsml4s8e
**D**ata **S**cience **ML** flow(**4**) **s**tandalon(**8**)**e**


dsml4s8e is a python library that aims to:
 1. simplify building of dagster pipelines from notebooks (loading notebooks to dagit)

It makes possible following workflow:
 1. Develop notebooks
 2. Loade the notebooks into dagit (build a pipeline)
 3. Configure and run the pipeline many times in vary environments and on vary infrastructure 

![op_parameters_cell](https://user-images.githubusercontent.com/1010096/221004539-a13f6dac-056c-4633-94bf-ef995c857da8.png)


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

![Job_dagstermill_pipeline](https://user-images.githubusercontent.com/1010096/221005076-7ba56646-8a9e-4d75-bbaf-e68ba04d036d.svg)


## Pipeline and workflow organization
Will be described the main entities behind pipeline development life cycle (entities of workflow)

### Data object key 
pipeline.component.nb.data_obj_name

### Mapping data object key to project structure
pipeliile.component.nb.data_obj_name ->
work/<dlc_stage>/pipeliile/component/nb
nb - notebook generate data obj
### Mapping data object key storage url


Test/OPs stage:
  1. gets from git run_params

 ### Development life cycle stage (dlc_satge)
  dlc_satge: dev/test/ops
  dlc_satge sets in job.<dlc_satge>.py - autorun script
  
 ## Mechanisms of passing entities of workflow
  The dlc_satge passes to the notebook with papermill params. Implemented in runner.py.
  
