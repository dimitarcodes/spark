## Setting up docker container for working with spark

### Misc
The original docker container provided by the course in 2021 is no longer available. You can find all the docker containers here - https://hub.docker.com/u/rubigdata 

We aren't using hdfs, so for this demo we're only gonna use the `spark-slim` container.

### Get docker container
```
    docker create --name hey-spark -it -p 8080:8080 -p 9001:9001 -p 4040:4040 rubigdata/spark-slim
```
### Start container
```
docker start hey-spark
```

You can see what's happening by attaching to the container:

```
docker attach hey-spark
```

You can detach using the `^p^q` key sequence. Make sure to avoid `^C` because Zeppelin will stop!

### Open Zeppelin

Connect to the [Zeppelin UI](http://localhost:9001) from your browser using the port defined in the container initialization, import the notebooks, and learn more about RDDs.

Screenshot from importing a notebook below; type the desired name, click on JSON and select 
the corresponding `.zpln` file from your cloned assignment repository using the file browser.

![Import Notebook](import-notebook.png)
