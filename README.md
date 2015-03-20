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

## Quickstart

Assuming you have docker installed, run this to start up a notebook server on https://localhost.

```
docker run -d -p 443:8888 -e "PASSWORD=MakeAPassword" sparkserver
```

You'll now be able to access your notebook at https://localhost with password MakeAPassword (please change the environment variable above).

## Hacking on the Dockerfile

Clone this repository, make changes then build the container:

```
docker build -t sparkserver .
docker run -d -p 443:8888 -e "PASSWORD=MakeAPassword" sparkserver
```

## Use your own certificate
This image looks for `/key.pem`. If it doesn't exist a self signed certificate will be made. If you would like to use your own certificate, concatenate your private and public key along with possible intermediate certificates in a pem file. The order should be (top to bottom): key, certificate, intermediate certificate.

Example:
```
cat hostname.key hostname.pub.cert intermidiate.cert > hostname.pem
```

Then you would mount this file to the docker container:
```
docker run -v /path/to/hostname.pem:/key.pem -d -p 443:8888 -e "PASSWORD=pass" sparkserver
```

## Using HTTP
This docker image by default runs IPython notebook in HTTPS.  If you'd like to run this in HTTP,
you can use the `USE_HTTP` environment variable.  Setting it to a non-zero value enables HTTP.

Example:
```
docker run -d -p 80:8888 -e "PASSWORD=MakeAPassword" -e "USE_HTTP=1" sparkserver


Build Examples
--------------
=======
Download Library
----------------
```
git clone https://github.com/atigeo/xframe.git xframe
```

Use the Examples
----------------
```
xframe$ ipython
>>> import xpatterns.xframe 
>>> import examples.example1 as example1
>>> example1.add(2, 5)
7
```

Documentation
-------------
[https://xpatterns.com/products/xframe/lib/docs](https://xpatterns.com/products/xframe/lib/docs/index.html)

Alternatively, there is local documentation in xframe/docs/_build/html/index.html.

License
-------
This SDK is provided under the 3-clause BSD [license](LICENSE).
