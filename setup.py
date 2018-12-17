import setuptools

with open("README.rst", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="datamonster_api",
    version="0.1.0",
    author="Kevin Thompson",
    author_email="kevin@adaptivemgmt.com",
    description="Library for accessing the datamonster rest API",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/adaptivemgmt/datamonster-api",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
