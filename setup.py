from setuptools import setup, find_packages, Extension
import os.path

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="hepnos-wizard",
    version="0.0.1",
    author="Matthieu Dorier",
    author_email="mdorier@anl.gov",
    description="Python scripts to setup HEPnOS configurations",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/hepnos/HEPnOS-Wizard",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        "bedrock"
    ],
    python_requires='>=3.6',
    entry_points={
        'console_scripts': [
            'hepnos-gen-config = hepnos.cmd:cmd_gen_config',
        ],
    },
)
