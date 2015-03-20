xFrame 1.x SDK (BETA)
==============================

The xFrame SDK aims to provide an easy to configure, consistent and scaleable data science library that is 
built on top of industry-standard open source technologies. 
xFrame provides the following advantages compared to other DataFrame implementations:

- A simple and well-tested Data Science library and Python based interface.
- Powerful abstraction over underlying Scaleable Big Data and Machine learning frameworks like Apache Spark, 
Spark SQL and ML libraries.
- Dockerized Container that bundles IPython notebooks, Anaconda distribution, Apache Spark and other
dependencies for painless setup.
- Extensible API allowing developer to add their own useful features and functionality. 


How xFrame Benefits You
-----------------------

If you're a data scientist, xFrame will isolate framework dependencies and their configuration within a 
single disposable, containerized environment, without compromising on any of the tools you're used to 
working with (notebooks, dataframes, machine learning and big data frameworks, etc.). Once you or 
someone else creates a single xFrame container, you just need to run the xFrame Container and 
everything is installed and configured for you to work. Other members of your team create their 
development environments from the same configuration, so whether you're working on 
Linux, Mac OS X, or Windows, all your team members are running data experiments on the 
same environment, against the same dependencies, all configured the same way. Say goodbye to 
painful setup times and "works on my machine" bugs.


Minimum Requirements
--------------------
*Linux*:

- Ubuntu 12.04 and above
- Docker >= 1.5 installation

*Mac*:

- Docker >= 1.5 installation


Download Library
-------------
```
git clone https://github.com/atigeo/xframe.git xframe-sdk
```

Build Examples
--------------
```
cd xframe-sdk && build
```

Use the Example Extensions
--------------------------
```
xframe-sdk$ ipython
>>> import xpatterns.xframe 
>>> import sdk_example.example1 as example1
>>> example1.add(2, 5)
7
```

Documentation
-------------
[https://xpatterns.com/products/xframe/sdk/docs](https://xpatterns.com/products/xframe/sdk/docs/index.html)

Alternatively, you can type `make doc` to build local documentations (requires Doxygen).

License
-------
This SDK is provided under the 3-clause BSD [license](LICENSE).
