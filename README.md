xFrame 0.1 Library (BETA)
==============================

The xFrame Library provides a consistent and scaleable data science library that is built on top of industry-standard open source technologies. 
xFrame provides the following advantages compared to other DataFrame implementations:

- A simple and well-tested Data Science library and Python based interface.
- Powerful abstraction over underlying scaleable big data and machine learning frameworks: Apache Spark, Spark DataFrames and ML libraries.
- Dockerized container that bundles IPython notebooks, scientific libraries, Apache Spark and other dependencies for painless setup.
- The library is extensible, allowing developers to add their own useful features and functionality. 
- isolate framework dependencies and their configuration within a single disposable, containerized environment
- No need to learn intricacies of Apache Spark in order to leverage its power.
- Once the xFrames container is run, Other members of your team create their development environments from the same configuration
- Works whether you're working on Linux, Mac OS X, or Windows, all your team members are running data experiments in the same environment


Minimum Requirements
--------------------
*Linux*:

- Ubuntu 12.04 and above
- Docker >= 1.5 installation

*Mac*:

- Install Boot2Docker

*Windows*

- Install Boot2Docker 

## Quickstart

Assuming you have docker installed, you can run the following commands in order to get Started (no need for 'sudo' on non-Linux Environments):

```
sudo docker build -t xframes .
sudo docker run -d -p 8888:8888 -e "USE_HTTP=1" --name xframes xframes

```

You'll now be able to access your [xFrame Notebook](http://localhost:8888) to run the Examples

Run the xFrame Example Notebooks
----------------
- Navigate to the MachineLearningWithSpark folder
- Click on a Sample Notebook under it (Chapter-3.ipynb for e.g.)
- Click on Cell->Run All
- Notebook should run and succeed producing data and plots


Documentation
-------------
[https://xpatterns.com/products/xframe/lib/docs](https://xpatterns.com/products/xframe/lib/docs/index.html)

Alternatively, there is local documentation in xframe/docs/_build/html/index.html.

License
-------
This SDK is provided under the 3-clause BSD [license](LICENSE).
