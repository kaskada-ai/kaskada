FROM jupyter/scipy-notebook

USER jovyan
# Copy the example notebook
COPY --chmod=755 ["./Kaskada with CSV.ipynb", "/home/jovyan/"]
# Install the latest alpha client (hard coded to 0.1.1a5)
RUN echo "Installing the Kaskada Client"
RUN pip install kaskada==0.1.1a5
# Install the dependencies for the examples
RUN pip install names

# Copy the release binaries
COPY --chmod=755 ["./kaskada-engine-linux-amd64", "/home/jovyan/.cache/kaskada/bin/kaskada-engine"]
COPY --chmod=755 ["./kaskada-manager-linux-amd64", "/home/jovyan/.cache/kaskada/bin/kaskada-manager"]

RUN chmod -R 755 /home/jovyan/.cache/kaskada
