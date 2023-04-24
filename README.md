# ingest_mechanism

This project demonstrates how to migrate from an SQL database, APIs, and files into a Delta Lakehouse.

## How to create environment

1. Build a Docker image for the ingest mechanism by running the following command:

```. docker build --no-cache -t delta_lakehouse .```

2. After creating the image, run the container with the following command:

```run -v league-db:/var/lib/mysql -p 8899:8888 --name delta_lakehouse_v2 delta_lakehouse```

`league-db` is a volume where the MySQL server is available. In this case, I use a prepared database from my other project [here](https://github.com/KarolKul-KK/League_Pro_Games_Analysis). The whole idea of this project is to migrate from an SQL database, APIs, and files into a Delta Lakehouse.

Note that the port is mapped to `8899` because your localhost probably already use `8888`. As a result, the URL for Jupyter has changed to `http://localhost:8899/`. You can then provide the token and start using Jupyter Notebook.

3. To get a token, run the following commands:

```docker exec -it (docker_id) bash```

```jupyter server list```

Docker will prepare the whole environment for you.

## Configuration Files

This project is managed by configuration files. The main configuration file is `job_config.yml`.

Here's the structure of the `job_config.yml` file:

```
sources:
  sql:
    [
      table_1_ingest.yml,
      table_2_ingest.yml,
      table_3_ingest.yml
    ]
  api:
    [
      endpoint_1_ingest.yml,
      endpoint_2_ingest.yml
    ]
  files:
    [
      file_1_ingest.yml,
      file_2_ingest.yml
    ]
 ```
 
 Configuration files are managed by the config generator inside the Config module. You can find sample structures for each config file in the configs directory.

## How to run the pipeline

To run the pipeline use command:```python3 main.py```
