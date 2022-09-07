
# dsml4s8e
data science ml flow standalone

## Entity IDs 
pipeliile.component.nb.entity

## Mapping entity IDs to project structure
pipeliile.component.nb.entity ->
work/<stage>/pipeliile/component/nb/entity

## Mapping entity IDs to storage url

## IDs lineage
Development stage:
  1. from procect catalog (pipeline/component) to id (pipeline.component)
  2. the running component updates run_config in directory run_params
  3. push run_config to git

Test/OPs stage:
  1. gets from git run_params

## Papermill params lineage
Development stage:
  1. a user define in notebook cell with tag params
  2. the running notebook updates run_config
  3. push run_config to git

Test/OPs stage:
  1. gets from git run_params

 ## Development life cycle stage (dlc_satge)
  dlc_satge: dev/test/ops
  dlc_satge sets in job.<dlc_satge>.py - autorun script
  
  
