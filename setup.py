"""
This builds the setup files.
"""
import os
import io
import re

from setuptools import setup

here = os.path.abspath(os.path.dirname(__file__))

def get_version():
    version_file = 'xframes/version.py'
    try:
        with open(version_file) as f:
            version_line = f.read()
            version_re = r"^__version__ = ['\"]([^'\"]*)['\"]"
            match = re.search(version_re, version_line, re.M)
            if match:
                return match.group(1)
            else:
                raise RuntimeError("Could not find version string in '{}'",format(version_file))
    except:
        raise RuntimeError("Could not find version file: '{}'".format(version_file))

def read(*filenames, **kwargs):
    encoding = kwargs.get('encoding', 'utf-8')
    sep = kwargs.get('sep', '\n')
    buf = []
    for filename in filenames:
        with io.open(filename, encoding=encoding) as f:
            buf.append(f.read())
    return sep.join(buf)


setup(name='xframes',
      version=get_version(),
      url='https://github.com/Atigeo/xpatterns-xframe',
      license='Apache Software License 2.0',
      packages=['xframes', 'xframes.deps', 'xframes.toolkit'],
      package_data={'xframes': ['conf/*.properties', 'conf/*.template', 'default.ini']},
      data_files=[('', ['README.rst'])],
      install_requires=['prettytable', 'numpy'],
      provides=['xframes'],
      platforms='any',
      author='Charles Hayden',
      author_email='charles.hayden@atigeo.com',
      description='XFrame data manipulation for Spark.',
      classifiers=['Programming Language :: Python',
                   'Development Status :: 4 - Beta',
                   'Natural Language :: English',
                   'Environment :: Console',
                   'Intended Audience :: Developers',
                   'License :: OSI Approved :: Apache Software License',
                   'Operating System :: OS Independent',
                   'Topic :: Software Development :: Libraries :: Python Modules']
      )
