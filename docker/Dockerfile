FROM ipython/scipyserver

MAINTAINER xFrames Project <xframes-dev@atigeo.com>

VOLUME /notebooks
WORKDIR /notebooks

EXPOSE 8888
EXPOSE 8080
EXPOSE 8081
EXPOSE 80

#xFrames Pre-reqs
RUN apt-get update --fix-missing
RUN apt-get install -y --fix-missing wget default-jdk emacs23 nginx

# This is for spark 1.6.1
ENV SPARK_VER spark-1.6.1
ENV PY4J_VER py4j-0.9-src.zip
ENV SPARK_HOME /opt/spark

RUN wget -q https://s3.amazonaws.com/xpatterns/dependencies/spark/${SPARK_VER}-bin-hadoop2.6.tgz -O /tmp/${SPARK_VER}.tgz && \
    tar -zxvf /tmp/${SPARK_VER}.tgz --directory /opt
RUN ln -s /opt/${SPARK_VER}-bin-hadoop2.6 ${SPARK_HOME}

# Setting Environment Variables
ENV PEM_FILE /key.pem
# $PASSWORD will get `unset` within server.sh, turned into an IPython style hash
ENV PASSWORD Dont make this your default
ENV USE_HTTP 0
ENV LIB=/notebooks
ENV SPARK_HOME=/usr/local/spark
ENV SPARK_CONF_DIR=/notebooks/conf
ENV PYTHONPATH="/usr/local/spark/python/:/notebooks:/usr/local/spark/python/lib/py4j-0.9-src.zip"

#Build Docs
RUN pip2.7 install --upgrade sphinx
ADD docs /notebooks/docs
RUN cd /notebooks/docs
RUN pwd
RUN easy_install cloud-sptheme
RUN python /notebooks/docs/patch_help_menu.py /usr/local

ADD server.sh /
ADD xframes /notebooks/xframes
ADD intro /notebooks/intro
ADD xframes/conf /notebooks/conf
ADD docker-setup /setup
RUN chmod u+x /server.sh
CMD ["/server.sh"]
