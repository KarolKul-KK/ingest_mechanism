FROM jupyter/base-notebook

USER root

RUN apt-get update && \
    apt-get install -y openjdk-8-jdk mysql-server mysql-client && \
    rm -rf /var/lib/apt/lists/*

# Load the auth_socket plugin
RUN echo '[mysqld]' >> /etc/mysql/my.cnf && \
    echo 'plugin-load-add=auth_socket.so' >> /etc/mysql/my.cnf

# Set the password for the MySQL root user
RUN service mysql start && \
    mysqladmin -u root password '<here_provide_your_password' && \
    service mysql stop

ENV APACHE_SPARK_VERSION 3.3.2
ENV HADOOP_VERSION 3

COPY spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz /tmp/
RUN cd /tmp && \
    tar xzf spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /usr/local --owner root --group root --no-same-owner && \
    rm spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

ENV SPARK_HOME /usr/local/spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}
ENV PATH $PATH:${SPARK_HOME}/bin
ENV PYSPARK_PYTHON /opt/conda/bin/python
ENV PYSPARK_DRIVER_PsYTHON jupyter
ENV PYSPARK_DRIVER_PYTHON_OPTS "notebook --NotebookApp.token='' --NotebookApp.password='' --NotebookApp.allow_origin='*' --NotebookApp.base_url=${NB_PREFIX}"

RUN conda install -c conda-forge delta-spark

USER $NB_UID

COPY src /home/jovyan/work/src
COPY configs /home/jovyan/work/configs
COPY notebooks /home/jovyan/work/notebooks