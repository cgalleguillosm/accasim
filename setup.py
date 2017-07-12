from setuptools import setup, find_packages
from codecs import open
from os import path
import re, mmap
import sys

here = path.abspath(path.dirname(__file__))
package_name = 'accasim'
with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()
    
with open(path.join(here, 'accasim', 'utils', 'version.py')) as f:
    data = f.readlines()[-1]
    mo = re.search('version = \'([\d.]+)\'', data)
    if mo:
        package_version = mo.group(1)
    else:
        raise Exception('Missing version')
setup(
    name=package_name,
    version=package_version,
    description='An HPC Workload Management Simulator',
    long_description=long_description,
    url='https://sites.google.com/view/accasim',
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
