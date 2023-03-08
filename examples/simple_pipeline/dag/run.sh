daghome=$(pwd)/daghome
mkdir -p $daghome
DAGSTER_HOME=$daghome dagit -h 0.0.0.0 -f dag.py
