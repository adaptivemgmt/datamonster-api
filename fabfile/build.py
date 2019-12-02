import hashlib
import os
import shutil

from fabric.api import lcd, local, task


BASE_DIR = os.path.dirname(os.path.realpath(__file__)) + "/.."


@task
def python_environment(
    base_dir=os.path.expanduser("~/.virtualenvs"), overwrite=False, dir_name=None
):
    """Build Python virtualenv for the application"""

    path_to_requirements = os.path.join(BASE_DIR, "requirements.txt")
    if dir_name is None:

        # This is memory inefficient. Don't put in a 5G requirements.txt file
        req_hash = hashlib.md5(open(path_to_requirements, "rb").read()).hexdigest()
        dir_name = os.path.join(base_dir, req_hash)

    path_to_virtual_env = os.path.join(base_dir, dir_name)
    bin_dir = os.path.join(path_to_virtual_env, "bin")

    # If we are overwritting the directory, clear it out now
    if overwrite:
        if os.path.exists(path_to_virtual_env):
            shutil.rmtree(path_to_virtual_env)

    # If the path already exists, we're done. Return now
    if os.path.exists(path_to_virtual_env):
        print("path_to_virtual_env: {}".format(path_to_virtual_env))
        return

    # Make the base dir if it doesn't exist
    if not os.path.exists(base_dir):
        os.makedirs(base_dir)

    with lcd(base_dir):
        local("python3 -m venv {}".format(dir_name))
        local("{}/pip3 install --upgrade pip".format(bin_dir))
        local("{}/pip3 install -r {}".format(bin_dir, path_to_requirements))
        print("path_to_virtual_env: {}".format(path_to_virtual_env))
