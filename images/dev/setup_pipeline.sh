cd /home/jovyan/work/dsml4s8e
poetry build
pip install --editable .
cd /home/jovyan/work/dsml4s8e/examples/simple_pipeline/dag
bash run.sh&
cd
start.sh jupyter lab --LabApp.token=''
