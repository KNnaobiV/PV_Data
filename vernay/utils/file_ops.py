__all__= [
    "get_or_create_dir"
]

import os


def get_or_create_dir(base, *args):
    """
    Creates nested directories in the base directory depending on 
    additional arguments.

    :param base: str or path object
        the base directory of the new directory to be created.
    :param *args:
        optional arguments for nested directories to be created.

    :return dirname: str or path object
    """
    dirname = os.path.join(base)
    if not os.path.exists(dirname):
        raise NotADirectoryError(f"{base} is not a directory.")
    if args:
        for arg in args:
            try:
                dirname = os.path.join(dirname, arg.lower())
                if not os.path.exists(dirname):
                    os.mkdir(dirname)
            except PermissionError:
                raise PermissionError(
                    f"You do not have permissions to create a directory in folder {dirname}"
                    )
    return dirname
    