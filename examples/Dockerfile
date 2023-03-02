FROM jupyter/scipy-notebook

USER jovyan
# Install the latest alpha client (hard coded to 0.1.1a5)
RUN pip install kaskada==0.1.1a5
# # Install the dependencies for the examples
RUN pip install names
# # Install the pulsar client
RUN pip install pulsar-client

# Copy the release binaries
COPY --chown=jovyan --chmod=755 ["./kaskada-engine-linux-amd64", "/home/jovyan/.cache/kaskada/bin/kaskada-engine"]
COPY --chown=jovyan --chmod=755 ["./kaskada-manager-linux-amd64", "/home/jovyan/.cache/kaskada/bin/kaskada-manager"]
