# Set the base image
FROM daskdev/dask-notebook:0.18.1

# Dockerfile author 
MAINTAINER Jamie Farnes <jamie.farnes@oerc.ox.ac.uk>

# As root, set up a python3.5 conda environment, activate, and install dask:
USER root
RUN mkdir sdp
RUN conda install python=3.5 && conda install -c conda-forge dask=0.18.1 distributed=1.23.1 && conda install setuptools && conda install numpy && conda install -c conda-forge matplotlib && conda install -c conda-forge casacore=2.4.0 && conda install -c conda-forge python-casacore

# As root, install various essential packages
RUN apt-get update && apt-get install -y graphviz git && apt-get -y install build-essential && apt-get -y install libssl-dev libffi-dev

# Install git-lfs
RUN apt-get -y install curl
RUN curl -s https://packagecloud.io/install/repositories/github/git-lfs/script.deb.sh | sudo bash
RUN apt-get -y install git-lfs
RUN git lfs install

# Set working directory
WORKDIR /home/jovyan/sdp

# Download the SIP DPrepB-C pipeline
RUN git clone https://github.com/jamiefarnes/SKA-SIP-DPrepB-C-Pipeline

# Download the SKA Algorithm Reference Library (ARL)
RUN apt-get -y install libcfitsio-dev
RUN git clone https://github.com/SKA-ScienceDataProcessor/algorithm-reference-library &&\
    cd ./algorithm-reference-library/ &&\
    git checkout stable-for-SIP &&\
    python3 setup.py install &&\
    git-lfs pull &&\
    rm -rf ./data/vis &&\
    rm -rf ./data/models &&\
    rm -rf ./.git

# Set the environment variables in advance of python installs:
ENV PYTHONPATH=$PYTHONPATH:/home/jovyan/sdp/algorithm-reference-library/:/opt/conda/lib/python3.5/site-packages/:/usr/lib/python3/dist-packages/

# Download the requirements for ARL
# and uninstall conflicting version of numpy and reinstall
WORKDIR /home/jovyan/sdp/algorithm-reference-library
RUN pip install -r requirements.txt &&\ 
    pip uninstall -y numpy &&\  
    pip install numpy

# Setup/install the SIP MAPS Pipeline
WORKDIR /opt/conda/lib/python3.5/
RUN ln -s ~/sdp/SKA-SIP-DPrepB-C-Pipeline/DPrepB-C/ska_sip

WORKDIR /opt/conda/lib/python3.5/
RUN ln -s ~/sdp/algorithm-reference-library/data
RUN ln -s ~/sdp/algorithm-reference-library/data_models
RUN ln -s ~/sdp/algorithm-reference-library/processing_components
RUN ln -s ~/sdp/algorithm-reference-library/libs
RUN ln -s ~/sdp/algorithm-reference-library/workflows


# Now install Zookeeper and Confluent Kafka
# to setup Queues here on the Producer
USER root
WORKDIR /home/jovyan/sdp/

# Install Java (JDK 8)
RUN apt-get -y install software-properties-common
RUN add-apt-repository -y ppa:webupd8team/java
RUN apt-get update
RUN echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | debconf-set-selections 
RUN apt-get -y install oracle-java8-installer

# Install zookeeper, which will be turned on from the docker-compose entrypoint
RUN apt-get -y install zookeeperd

# Then install confluent-kafka
RUN apt-get -y install wget
RUN wget -qO - http://packages.confluent.io/deb/3.3/archive.key | sudo apt-key add -
RUN add-apt-repository "deb [arch=amd64] http://packages.confluent.io/deb/3.3 stable main"
RUN apt-get update
RUN apt-get -y install librdkafka1 librdkafka-dev
RUN conda install -c conda-forge python-confluent-kafka

# Download and install Kafka, which will be turned on from the docker-compose entrypoint
RUN wget "http://www-us.apache.org/dist/kafka/0.11.0.2/kafka_2.11-0.11.0.2.tgz" --directory-prefix=/home/jovyan/sdp/
RUN mkdir Kafka
RUN tar -xvf /home/jovyan/sdp/kafka_2.11-0.11.0.2.tgz -C Kafka/

# Define the initial working directory
WORKDIR /home/jovyan/sdp/
