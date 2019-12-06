import os
import setuptools

requires = ["fastavro", "more-itertools", "numpy", "pandas", "requests", "six"]

here = os.path.abspath(os.path.dirname(__file__))

about = {}
with open(
    os.path.join(here, "datamonster_api", "__version__.py"), mode="r", encoding="utf-8"
) as f:
    exec(f.read(), about)

with open("README.rst", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name=about["__title__"],
    version=about["__version__"],
    author=about["__author__"],
    author_email=about["__author_email__"],
    description=about["__description__"],
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/adaptivemgmt/datamonster-api",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=requires,
    python_requires="!=3.5.*, !=3.6.*, !=3.7.*",
    project_urls={
        "Documentation": "https://datamonster-api.readthedocs.io",
        "Source": "https://github.com/adaptivemgmt/datamonster-api",
    },
)
