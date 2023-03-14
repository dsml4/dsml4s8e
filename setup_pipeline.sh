cd /home/jovyan/work/dsml4s8e
python setup.py sdist
pip install -U .
cd /home/jovyan/work/dsml4s8e/examples/simple_pipeline/dag
bash run.sh&
cd
start.sh jupyter lab --LabApp.token=''
