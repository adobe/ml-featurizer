from __future__ import print_function
import sys
from setuptools import setup

if sys.version_info < (2, 7):
    print("Python versions prior to 2.7 are not supported for pip installed PySpark.",
          file=sys.stderr)
    sys.exit(-1)

long_description = '''
ML Featurizer is a library to enable users to create additional features from raw data with ease
'''

with open('requirements.txt') as f:
      required = f.read().splitlines()

setup(name='mlfeaturizer',
      version='0.0.1',
      description=long_description,
      url='https://github.com/adobe/ml-featurizer',
      author='Wei Shung Chung',
      author_email='wchung@adobe.com',
      license='http://www.apache.org/licenses/LICENSE-2.0',
      install_requires=required,
      packages=['mlfeaturizer','mlfeaturizer.core'])
