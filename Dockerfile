FROM ipython/scipyserver

MAINTAINER xFrames Project <xframes-dev@atigeo.com>

VOLUME /notebooks
WORKDIR /notebooks

EXPOSE 8888
EXPOSE 8080
EXPOSE 8081

RUN apt-get update
RUN apt-get install -y wget default-jdk
RUN apt-get install -y emacs23
RUN wget -q http://www.us.apache.org/dist/spark/spark-1.3.0/spark-1.3.0-bin-cdh4.tgz -O /tmp/spark-1.3.0-bin-cdh4.tgz
RUN tar -zxvf /tmp/spark-1.3.0-bin-cdh4.tgz --directory /tmp
RUN rm /tmp/spark-1.3.0-bin-cdh4.tgz
RUN mv /tmp/spark-1.3.0-bin-cdh4 /usr/local/spark
RUN easy_install prettytable

# You can mount your own SSL certs as necessary here
ENV PEM_FILE /key.pem
# $PASSWORD will get `unset` within server.sh, turned into an IPython style hash
ENV PASSWORD Dont make this your default
ENV USE_HTTP 0

ADD server.sh /
ADD xpatterns /notebooks/xpatterns
ADD test /notebooks/test
ADD examples /notebooks
ADD MachineLearningWithSpark /notebooks
ADD misc-notebooks /notebooks
ADD docker-setup /setup
RUN chmod u+x /server.sh

CMD ["/server.sh"]


