# Functions for internal use, not exposed to users/documented/"exported" in ../__init__.py
# Prefixing them with '_' causes Pycharm and flake8 warnings.

def summarize_splits_dict(splits):
    """
    :param splits: dict returned by `get_splits`
    :return: a dict of the form
        { 'split_count': N,
          'columns': { split_col_name_0: set_of_values_for_this_col,
                        ...
                       split_col_name_i: set_of_values_for_this_col,
                       ...
                        'section_pk': {section_pk, ...}
                       ...
                     }
        }
    """
    results = splits['results']

    columns = defaultdict(set)
    for result in results:
        split_combo = result['split_combination']
        for key in split_combo:
            columns[key].add(split_combo[key])
    return {
        'split_count': len(results),
        'columns': dict(columns)
    }
