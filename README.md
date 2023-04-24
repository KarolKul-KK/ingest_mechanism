# ingest_mechanism

This project demonstrates how to migrate from an SQL database, APIs, and files into a Delta Lakehouse.

## How to Run

1. Build a Docker image for the ingest mechanism by running the following command:

```. docker build --no-cache -t delta_lakehouse .```
2. After creating the image, run the container with the following command:

```run -v league-db:/var/lib/mysql -p 8899:8888 --name delta_lakehouse_v2 delta_lakehouse```

`league-db` is a volume where the Docker MySQL server is available. In this case, I use a prepared database from my other project [here](https://github.com/KarolKul-KK/League_Pro_Games_Analysis). The whole idea of this project is to migrate from an SQL database, APIs, and files into a Delta Lakehouse.

Note that the port is mapped to `8899` because your localhost probably already use `8888`. As a result, the URL for Jupyter has changed to `http://localhost:8899/`. You can then provide the token and start using Jupyter Notebook.

3. To get a token, run the following commands:

```docker exec -it (docker_id) bash```
```jupyter server list```

Docker will prepare the whole environment for you.
