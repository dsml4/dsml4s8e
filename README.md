
# dsml4s8e
**D**ata **S**cience / **ML** flow(**4**) **s**tandalon(**8**)**e**


dsml4s8e is a python library that aims to:
 1. simplify building of Dagster pipelines from **standalon** notebooks
 2. standardize a structure of ML/DS a pipeline project to easy sharing
 3. manage experiments data and artefacts

It makes possible following workflow:
 1. define a project structure for your pipeline and a structure of the pipeline catalog
 2. develop **standalon** Jupyter notebooks with clear interface
 3. loade the notebooks into dagit (build a pipeline)
 4. configure and run the pipeline many times in vary environments and on vary infrastructure 

![op_parameters_cell](https://user-images.githubusercontent.com/1010096/221004539-a13f6dac-056c-4633-94bf-ef995c857da8.png)


![nb_1](https://user-images.githubusercontent.com/1010096/221655435-3b01fb49-7ff9-4e53-82b8-ad0922fc2136.png)


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


  
