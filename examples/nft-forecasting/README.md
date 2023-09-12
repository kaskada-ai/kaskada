# NFT Forecasting example

This notebook and dataset are intended to demonstrate a use case of Kaskada -
predicting the value of newly-created NFT's.

The included notebook sets up Kaskada, loads some NFT transaction data, and
builds a model to predict what an NFT will sell for.

You can use the notebook in Docker by running the following command in this directory, which will download a docker container with Jupyter and Kaskada pre-installed and launch the Jupyter server.


```sh
docker run --rm -p 8888:8888 -v "$PWD:/home/jovyan/example" kaskadaio/jupyter
````

At the end of the log output you should see a URL like `http://127.0.0.1:8888/lab?token=d7f0cab9929e1b499b66fd3308357ed62dbb524db1ffe394`:

```
...
[I 2023-05-03 14:41:29.593 ServerApp] Jupyter Server 2.5.0 is running at:
[I 2023-05-03 14:41:29.593 ServerApp] http://756b93a11d10:8888/lab?token=d7f0cab9929e1b499b66fd3308357ed62dbb524db1ffe394
[I 2023-05-03 14:41:29.593 ServerApp]     http://127.0.0.1:8888/lab?token=d7f0cab9929e1b499b66fd3308357ed62dbb524db1ffe394
[I 2023-05-03 14:41:29.593 ServerApp] Use Control-C to stop this server and shut down all kernels (twice to skip confirmation).
[C 2023-05-03 14:41:29.595 ServerApp]

    To access the server, open this file in a browser:
        file:///home/jovyan/.local/share/jupyter/runtime/jpserver-7-open.html
    Or copy and paste one of these URLs:
        http://756b93a11d10:8888/lab?token=d7f0cab9929e1b499b66fd3308357ed62dbb524db1ffe394
        http://127.0.0.1:8888/lab?token=d7f0cab9929e1b499b66fd3308357ed62dbb524db1ffe394
```

Copy the URL into your brower, and you should see the Jupyter UI. In the file browser on the left, open the `example` folder and double-click on `Notebook.ipynb`. 

Run the cells in the notebook to setup Kaskada and build a model.
