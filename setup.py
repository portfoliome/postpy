#!/usr/bin/env python

import os
from setuptools import find_packages, setup

ENCODING = 'utf-8'
PACKAGE_NAME = 'postpy'

local_directory = os.path.abspath(os.path.dirname(__file__))
version_path = os.path.join(local_directory, PACKAGE_NAME, '_version.py')

version_ns = {}
with open(version_path, 'r', encoding=ENCODING) as f:
    exec(f.read(), {}, version_ns)


def get_requirements(requirement_file):
    requirements = list(
        open(requirement_file, 'r',
             encoding=ENCODING).read().strip().split('\r\n'))
    return requirements


setup(name=PACKAGE_NAME,
      packages=find_packages(exclude=('tests',)),
      include_package_data=True,
      version=version_ns['__version__'],
      license='MIT',
      description='Postgresql utilities for ETL and data processing.',
      url='https://github.com/portfoliome/postpy',
      author='Philip Martin',
      author_email='philip.martin@censible.co',
      classifiers=[
          'Development Status :: 5 - Production/Stable',
          'Intended Audience :: Developers',
          'Natural Language :: English',
          'Programming Language :: Python :: 3.5',
          'Programming Language :: Python :: 3.6',
          'Topic :: Utilities',
      ],
      keywords='ETL data postgres',
      install_requires=get_requirements('requirements.txt'),
      extras_require={
          'develop': get_requirements('requirements-dev.txt'),
          'test': get_requirements('requirements-test.txt')
      },
      zip_safe=False)
