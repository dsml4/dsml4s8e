thome=$HOME/work/dag_home_tests
mkdir -p $thome
echo $thome
DAGSTER_HOME=$thome dagit -h 0.0.0.0 -f $(pwd)/dag_simple.py
