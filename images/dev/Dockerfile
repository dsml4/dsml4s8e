FROM  jupyter/scipy-notebook:latest
LABEL maintainer="Motuzov <motuzov@gmail.com>"

USER root

RUN apt-get update --yes && \
    apt-get install --yes --no-install-recommends \
    vim tree && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

USER ${NB_UID}

# Install dsml4s8e libs
RUN  mamba install --name base pycodestyle gitpython twine -y && \
     mamba clean --all -f -y && \
     fix-permissions "${CONDA_DIR}" && \
     fix-permissions "/home/${NB_USER}"

RUN pip install -U dagster dagstermill dagit && \
    pip install -U dsml4s8e && \
    pip install -U pytest 

USER ${NB_UID}
ENV DAGSTER_HOME=/home/jovyan/work/dagster_home

WORKDIR "${HOME}"
