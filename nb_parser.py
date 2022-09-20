import nbformat
import json


def get_results(nb_path):
    nb = nbformat.read(nb_path, as_version=4)
    for cell in nb.cells:
        tags = []
        if cell.cell_type == 'code':
            tags = cell.metadata.get('tags', [])

        print(tags)
        if 'results' in tags:
            return json.loads(cell.outputs[0]['text'])

    return {}
