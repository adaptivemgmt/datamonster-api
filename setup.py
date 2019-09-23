import setuptools

with open("README.rst", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="datamonster_api",
    version="0.4.1",
    author="Kevin Thompson",
    author_email="kevin@adaptivemgmt.com",
    description="Library for accessing the Datamonster REST API",
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
    install_requires=[
        "fastavro",
        "memoized-property",
        "more-itertools",
        "numpy",
        "pandas",
        "requests",
        "six",
    ],
)
