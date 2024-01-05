# Running Kafka with Docker Compose

Simple java-based Kafka producer and consumer application to send and receive messages from kafka broker. The producer connects to the broker, creates a kafka topic named `random-quotes`, and publishes string records (random famous quotes) to the topic. The consumer subscribes to the created topic and continuously polls for new records from kafka.

## Getting Started

### Dependencies

* Docker 
* Docker Compose
* OpenJDK 21 (make sure to set JAVA_HOME to location of JDK home dir)

### Installing

* Clone the kafka project.

* Use the maven wrapper to install dependencies.
  ```
  ./mvnw install
  ```

### Executing program

* To run kafka in kraft mode, use the `docker-compose-kraft.yml` file. To run kafka in zookeeper mode, use the `docker-compose-zookeeper.yml` file.
  ```
  docker-compose -f docker-compose-kraft.yml up -d
  docker-compose -f docker-compose-zookeeper.yml up -d
  ```
* Check if the containers are running successfully. For zookeeper mode, make sure that zookeeper is also up.
  ```
  docker ps
  ```
* Open a terminal window to run the producer application. You should see messages being sent to the kafka topic.
  ```
  ./mvnw exec:java -Dexec.mainClass="Producer"
  ```
* Now, open a second terminal window to run the consumer application. You should see the messages from the kafka topic.
  ```
  ./mvnw exec:java -Dexec.mainClass="Consumer"
  ```
  

# Inspiration, code snippets, etc.

* [kafka tutorial playlist](https://www.youtube.com/playlist?list=PLa6iDxjj_9qVGTh3jia-DAnlQj9N-VLGp)
* [simple kafka producer application](https://strimzi.io/blog/2023/10/03/kafka-producer-client-essentials/)
* [simple kafka consumer application](https://strimzi.io/blog/2023/11/09/kafka-consumer-client-essentials/)
* [bitnami kafka docker image and compose files](https://hub.docker.com/r/bitnami/kafka)
