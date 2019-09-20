These steps update the pypi package:

1. Update the version number in setup.py, and commit it.
1. Tag a new release in github, with release notes.
1. With the release commit pulled, run the following commands locally:
   1. `python setup.py sdist`
   1. `twine upload dist/*`
