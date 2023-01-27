
# dsml4s8e
data science ml flow standalone

dsml4s8e is a python library that aims to
 1. simplify building of dagster pipelines from notebooks (loading notebooks to dagit)

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
  
