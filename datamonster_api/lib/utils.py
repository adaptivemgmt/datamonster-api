__doc__ = """\
Functions for internal use -- not exposed to users, not documented, not "exported"
in ../__init__.py.
Prefixing any of these with '_' causes Pycharm and flake8 warnings.
"""
from collections import defaultdict
from .errors import DataMonsterError


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
    # Detect failures of `splits` to be a "splits dict" and raise DataMonsterError if so
    try:
        results = splits['results']
    except KeyError:
        raise DataMonsterError(
            "argument `splits` to `summarize_splits_dict` is not a \"splits dict\": "
            "it lacks a 'results' key"
        )

    columns = defaultdict(set)
    for result in results:
        try:
            split_combo = result['split_combination']
        except KeyError:
            raise DataMonsterError(
                "argument `splits` to `summarize_splits_dict` is not a \"splits dict\": "
                "some item in `splits['results'] lacks a 'split_combination' key"
            )

        for key in split_combo:
            columns[key].add(split_combo[key])

    return {
        'split_count': len(results),
        'columns': dict(columns)
    }
