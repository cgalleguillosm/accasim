from setuptools import setup, find_packages
from accasim import __version__
from codecs import open
from os import path
import re, mmap
import sys

here = path.abspath(path.dirname(__file__))
package_name = 'accasim'
with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()
    
setup(
    name=package_name,
    version=__version__,
    description='An HPC Workload Management Simulator',
    long_description=long_description,
    url='http://accasim.readthedocs.io/en/latest/',
    author='Cristian Galleguillos',
    author_email='cristian.galleguillos.m@mail.pucv.cl',
    license='MIT',

    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Science/Research',
        'Topic :: Scientific/Engineering',
        'License :: OSI Approved :: MIT License',
        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],

    keywords='research, hpc, dispathing',
    python_requires='>=3.4',

    packages=find_packages(exclude=['contrib', 'docs', 'tests']),

    install_requires=['matplotlib', 'psutil'],
)
