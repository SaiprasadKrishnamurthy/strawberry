```

                      \VW/
                    .::::::.
                    ::::::::
                    '::::::'
                     '::::'
                       `"`
  ____  _                      _                           
 / ___|| |_ _ __ __ ___      _| |__   ___ _ __ _ __ _   _  
 \___ \| __| '__/ _` \ \ /\ / / '_ \ / _ \ '__| '__| | | | 
  ___) | |_| | | (_| |\ V  V /| |_) |  __/ |  | |  | |_| | 
 |____/ \__|_|  \__,_| \_/\_/ |_.__/ \___|_|  |_|   \__, | 
                                                    |___/


```
## PREREQUISITES (FOR A DEV SETUP)##
* Java 8
* Maven 3
* MongoDB v 3.2.1
* Elasticsearch v 2.4.0
* Zookeeper v 3.4.9
* Apache Kafka 2.11-0.9.0.1
* Apache Storm 1.0.2
* Kibana 4.6.1


## Steps to start (high level steps) ##
* Start MongoDB
* Start Zookeeper
* Start Kafka pointing to the Zookeeper
* Create a new Database in mongo called strawberry (Use RoboMongo tool for this, quite easy to use)
* Open the REST project in your IDE (eg: Eclipse)
* Run Main.java in your eclise as a Java Application.
* Try to access http://localhost:9090/swagger-ui.html#/  (You should see the swagger UI).
* You have successfully set up the REST by now.


## Kafka handy commands ##
```
./kafka-topics.sh --create --topic vehicle-camera-sensor-stream --replication-factor 1 --partitions 3 --zookeeper localhost:2181
```

![Alt text](basic_arch.png?raw=true "Some draft arch")






 
