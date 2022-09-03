import papermill


def run_notebook(
        temp_nb_name,
        out_nb_name,
        parameters
        ):

    nn = papermill.execute.execute_notebook(
        temp_nb_name,
        out_nb_name,
        parameters=parameters)
