import os
from setuptools import setup

pjoin = os.path.join

packages = []
for d, _, _ in os.walk('sensible_raw'):
    if os.path.exists(pjoin(d, '__init__.py')):
        packages.append(d.replace(os.path.sep, '.'))

setup(name='sensible_raw',
      version='0.3',
      packages = packages,
      description='Raw data management for sensibledtu project',
      author='Radu Gatej',
      author_email='radu.gatej@gmail.com',
      license='MIT',
      package_data={'': ['*.csv']},
      include_package_data=True,
      install_requires=[
          'monary',
      ],
      zip_safe=False)