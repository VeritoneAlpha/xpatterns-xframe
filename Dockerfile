FROM ipython/scipyserver

MAINTAINER xFrames Project <xframes-dev@atigeo.com>

VOLUME /notebooks
WORKDIR /notebooks

EXPOSE 8888
EXPOSE 8080
EXPOSE 8081
EXPOSE 80

#xFrames Pre-reqs
RUN apt-get update
RUN apt-get install -y wget default-jdk
RUN apt-get install -y emacs23
RUN apt-get install -y nginx
RUN wget -q http://www.us.apache.org/dist/spark/spark-1.3.0/spark-1.3.0-bin-cdh4.tgz -O /tmp/spark-1.3.0-bin-cdh4.tgz
RUN tar -zxvf /tmp/spark-1.3.0-bin-cdh4.tgz --directory /tmp
RUN rm /tmp/spark-1.3.0-bin-cdh4.tgz
RUN mv /tmp/spark-1.3.0-bin-cdh4 /usr/local/spark
RUN easy_install prettytable

# Setting Environment Variables
ENV PEM_FILE /key.pem
# $PASSWORD will get `unset` within server.sh, turned into an IPython style hash
ENV PASSWORD Dont make this your default
ENV USE_HTTP 0
ENV XPATTERNS_HOME=/notebooks
ENV LIB=/notebooks
ENV SPARK_HOME=/usr/local/spark
ENV PYTHONPATH="/usr/local/spark/python/:/notebooks:/usr/local/spark/python/lib/py4j-0.8.2.1-src.zip"

#Build Docs
RUN pip install Sphinx
ADD docs /notebooks/docs
RUN cd /notebooks/docs
RUN pwd
RUN easy_install cloud-sptheme

ADD server.sh /
ADD xpatterns /notebooks/xpatterns
ADD examples /notebooks/examples
ADD MachineLearningWithSpark /notebooks/MachineLearningWithSpark
ADD misc-notebooks /notebooks/misc-notebooks
ADD docker-setup /setup
RUN chmod u+x /server.sh
CMD ["/server.sh"]


