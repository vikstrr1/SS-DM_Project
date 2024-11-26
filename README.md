# SS-DM_Project
Course project for scalable systems and data management
run through 
``docker-compose up --build``
new terminal window to start flink job

``docker exec -it ss-dm_project-flink-jobmanager-1 /bin/bash``
``cd ..``
``./bin/flink run -py /opt/flink/jobs/flink_processor.py``


## Configuration 
Explanation of 

Flink, Elasticsearch, Kibana and 

## Kubernetes
```
eval $(minikube -p minikube docker-env)

docker build -t apache-flink-project:v1 . 

kubectl apply -f kubernetes/flink-jobmanager-deployment.yaml
kubectl apply -f kubernetes/flink-jobmanager-service.yaml
kubectl apply -f kubernetes/flink-taskmanager.yaml           
kubectl apply -f kubernetes/flink-taskmanager-service.yaml

kubectl port-forward deployment/flink-jobmanager 8081:8081
```
New terminal window

```
kubectl exec -it flink-jobmanager-5d4c5bdb9b-8qds4 -- bash -c "cd /opt/flink && ./bin/flink run -m flink-jobmanager:8081 -py /opt/flink/jobs/flink_processor.py"

```

## ElasticSearch and Kibana

ElasticSearch is selected as a temporary storage database of user-selected storage data. This is only stored for visualization. 

TODO Add more description on how the 

Kibana is selected as the visualization tool for displaying the indexes created by the flink job. 
## With grafana and influxdb
Go to http://localhost:8086/ and login and obtain token for python processor.py script it should be avaible under python config
After that has been obtainend and put into python file
Run following command
``docker-compose up --build kafka_to_influx``
Data will no be put into influxdb bucket and saved for one hour
login to both influx and grafana with user admin and password admin123 
grafana dashboard avaible at http://localhost:3000/

## Grafana

Go to Grafan and add datasource.
First need to obtain a api key from influxdB named GRafana for example
After that add
host http://influxdb:8086/

and the token then click save and test.

Then choose the dashboard that is already up in grafana and follow the data
##

### Configuration of ElasticSearch and Kibana
Currently the ElasticSearch and Kibana needs to be configured on startup. Password and username can later  be set by docker-compose.yml file. 


After starting the docker instance which brings up in addition to the flink instance two pods (elasticsearch and kibana) go to to localhost:5601

Generate a elasticsearch token for the kibana instance by navigating to the elasticsearch pod. 

``docker exec -it ss-dm_project-elasticsearch-1 /bin/bash``

Generate the elasticsearch token ``bin/elasticsearch-create-enrollment-token --scope kibana``

Generate a Kibana token from the kibana pod 
``docker exec -it ss-dm_project-kibana-1  /bin/bash``
run
``bin/kibana-verification-code``

The user ``elastic`` should be created by default with password ``password123`` by the docker-compose.yml file. otherwise it can be recreated and password reset by entering the the elasticsearch pod. 

The elastic database and kibana is mainly in charge of displaying the ticker over time to the user



#### Questions on Q&A
- Is the current visualization tool for stocks sufficient? Or should you as an user be able to change which stocks are monitored after starting the 
- Currently the specified tickers are sent to elasticsearch to be visualized in kibana. 
- Is it sufficient to just insert from csv file. What kind of improvements could we make here. Currently we are kind of frontrunning the "clock" because we have an historic dataset. 
- 
