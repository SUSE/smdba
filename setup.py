__author__="bo"
__date__ ="$May 8, 2012 2:54:34 PM$"

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup (
  name = 'SUSE Manager Database Control',
  version = '1.0',
  package_dir = {'': 'src'},
  package_data={'smdba': ['scenarios/*.scn']},
  packages = [
      'smdba',
  ],
  data_files=[('/usr/bin/', ['src/smdba/smdba-netswitch', 'src/smdba/smdba-pgarchive'])],

  author = 'bo',
  author_email = 'bo@suse.de',

#  summary = 'SUSE Manager Database Control',
  url = '',
  license = 'MIT',
  long_description= 'SUSE Manager database control to operate various database backends.',
)
