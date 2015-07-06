import os, sys, io
import codecs

from setuptools import setup, find_packages
import xpatterns

here = os.path.abspath(os.path.dirname(__file__))

def read(*filenames, **kwargs):
    encoding = kwargs.get('encoding', 'utf-8')
    sep = kwargs.get('sep', '\n')
    buf = []
    for filename in filenames:
        with io.open(filename, encoding=encoding) as f:
            buf.append(f.read())
    return sep.join(buf)

long_description = read('docs/README.rst')

setup(name='xframe',
      version=xpatterns.__version__,
      url='https://github.com/Atigeo/xframe',
      license='Apache Software License 2.0',
      packages=['xpatterns'],
      include_package_data=True,
      platforms='any',
      author='Charles Hayden',
      author_email='charles.hayden@atigeo.com',
      description='XFrame data manipulation for Spark.',
      classifiers=['Programming Language :: Python',
                   'Development Status :: 4 - Beta',
                   'Natural Language :: English',
                   'Environment :: ???',
                   'Intended Audience :: Developers',
                   'License :: OSI Approved :: Apache Software License 2.0',
                   'Operating System :: OS Independent',
                   'Topic :: Software Development :: Libraries :: Python Modules']
      )
