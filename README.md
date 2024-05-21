# python-data-engineering

zookeeper.yml: Contains the Zookeeper service.
kafka.yml: Contains Kafka broker and schema registry services.
airflow_spark_cassandra.yml: Contains Control Center, Airflow components (webserver and scheduler), PostgreSQL, Spark components, and Cassandra.
master-compose.yml: The master Compose file that includes and extends services from the other files.

 # Use docker-compose -f master-compose.yml up to start the services with dependencies managed across files.

 

docker exec -it cassandra cqlsh -u cassandra -p cassandra localhost 9042