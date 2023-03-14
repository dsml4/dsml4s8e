mkdir -p $DAGSTER_HOME
echo $DAGSTER_HOME
dagit -h 0.0.0.0 -f $(pwd)/dag.py
