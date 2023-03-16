FROM jupyter/scipy-notebook:python-3.10.9

# Manually install libssl1.1 - Ubuntu22 has migrated to libssl3, but we use 1.1
# to be compatible with google colab now.
USER root
COPY libssl1.deb /tmp/.
RUN dpkg -i /tmp/libssl1.deb

# Create user with a home directory
USER jovyan
ENV USER jovyan
ENV HOME /home/jovyan

WORKDIR ${HOME}

# Install the pulsar python client
RUN pip install pulsar-client

# Install the kaskada python client
RUN pip install kaskada==0.1.1a7

# # Copy over the sample notebook and dataset
COPY --chown=jovyan --chmod=755 ["./Materialize_to_Pulsar.ipynb", "./Materialize_to_Pulsar.ipynb"]
COPY --chown=jovyan --chmod=755 ["./out1.parquet", "./data/transactions_part1.parquet"]
COPY --chown=jovyan --chmod=755 ["./out2.parquet", "./data/transactions_part2.parquet"]
COPY --chown=jovyan --chmod=755 ["./out3.parquet", "./data/transactions_part3.parquet"]


