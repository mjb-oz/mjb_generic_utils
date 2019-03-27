#===============================================================================
#    Copyright 2017 Geoscience Australia
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#===============================================================================

"""
Created on 27/3/2019
@author: Mike Barnes
"""
from distutils.core import setup

setup(name='mjb_generic_utils',
      version='0.1',
      description='A collection of general helper scripts that don\'t '
                  'live anywhere else',
      url='https://github.com/mjb-oz/mjb_generic_utils',
      author='Mike Barnes',
      author_email='michael.barnes@ga.gov.au',
      requires = [
            'matplotlib',
            'netcdf4',
            'numpy',
            'pandas',
            'shapely',
            'scipy',
            'setuptools',
            'sqlalchemy'
              ],
      packages=['mjb_generic_utils'],
      license='Apache License Version 2.0')