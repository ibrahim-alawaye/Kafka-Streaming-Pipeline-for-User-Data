# Kafka Streaming Pipeline for User Data

This pipeline streams user data from the [Random User API](https://randomuser.me/), processes it, and stores it in Apache Cassandra after deep processing with Apache Spark. The entire process is managed using Kafka and Zookeeper to ensure seamless data streaming and fault tolerance.

## Overview

The pipeline consists of the following steps:
1. **Data Fetching**: Fetches random user data from the Random User API using `requests` on Apache Airflow.
2. **Data Formatting**: Formats the data to extract essential user details.
3. **Kafka Streaming**: Streams the formatted data to a Kafka topic.
4. **Data Processing with Spark**: Consumes the Kafka stream and processes it using Apache Spark.
5. **Data Storage in Cassandra**: Inserts the processed data into a Cassandra database for long-term storage.

## Architecture

The architecture use Docker to spin infrastructure and leverages several components:

- **API Data Source**: Fetches user data using Airflow.
- **Kafka**: Streams the processed user data.
- **Zookeeper**: Manages Kafka brokers and handles coordination tasks.
- **Spark**: Processes data in real-time and performs transformations.
- **Cassandra**: Stores processed user data for efficient querying and retrieval.

## Technologies Used

- **Apache Kafka**: For real-time streaming and message brokering.
- **Apache Zookeeper**: For Kafka broker coordination.
- **Apache Spark**: For real-time data processing.
- **Apache Cassandra**: As the NoSQL database for storing user data.
- **Apache Airflow**: To provide random user data for the streaming pipeline.
- **Python (requests)**: To fetch data from the API.

## Future Improvements

- Implementing fault tolerance and retries in the data fetching process.
- Scaling the architecture to handle larger data streams.

---

