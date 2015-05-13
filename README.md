xFrame 0.1 Library (BETA)
==============================

The xFrame Library provides a consistent and scaleable data science library that is built on top of industry-standard open source technologies. 
xFrame provides the following advantages compared to other DataFrame implementations:

- A simple and well-tested Data Science library and Python based interface.
- Powerful abstraction over underlying scaleable big data and machine learning frameworks: Apache Spark, Spark DataFrames and ML libraries.
- Dockerized container that bundles IPython notebooks, scientific libraries, Apache Spark and other dependencies for painless setup.
- The library is extensible, allowing developers to add their own useful features and functionality. 


How xFrame Benefits You
-----------------------

If you're a data scientist, xFrame will isolate framework dependencies and their configuration within a single disposable, containerized environment, without compromising on any of the tools you're used to working with (notebooks, dataframes, machine learning and big data frameworks, etc.). Once you or someone else creates a single xFrame container, you just need to run the container and everything is installed and configured for you to work. Other members of your team create their development environments from the same configuration, so whether you're working on Linux, Mac OS X, or Windows, all your team members are running data experiments in the same environment, against the same dependencies, all configured the same way. Say goodbye to painful setup times and "works on my machine" bugs.


Minimum Requirements
--------------------
*Linux*:

- Ubuntu 12.04 and above
- Docker >= 1.5 installation

*Mac*:

- Docker >= 1.5 installation

*Windows*

- Run in VM

Download Library
----------------
```
git clone https://github.com/atigeo/xframe.git xframe
```

Build docker container
----------------------
Go to the docker directory and follow the build instructions in README.md.

Review introductory presentation
--------------------------------
After starting docker container, browse to http://localhost:7777/tree.
Then open info/Presentation.ipynb.

Documentation
-------------
You can view local documentation in localhost:8000

License
-------
This SDK is provided under the 3-clause BSD [license](LICENSE).


Install Library On Existing Spark Installation
----------------------------------------------

If you have an existing Spark installation that you already use with pySpark, then
you can simply install the library to work with that.

From the source distribution, you can either:

1.  Include the xpatterns directory in PYTHONPATH, or
2.  Build an archive (see below) and install it on a different machine.

Building the Library

In the source distribution, run 
    python setup.py sdist --formats=zip

This will create a file dist/xframe-<version>.tar.gz
This file can be copied to the server where you want to install xframes.

Install by:

    tar xfz xframe.<version>.tar.gz
    cd xframe.<version>
    python setup.py install

