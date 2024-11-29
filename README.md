# SS-DM_Project
Course project for scalable systems and data management
run through 
``docker-compose up --build``
new terminal window to start flink job

``docker exec -it ss-dm_project-flink-jobmanager-1 sh -c "cd .. && ./bin/flink run -py /opt/flink/jobs/flink_processor.py"``


## Configuration 
Explanation of 

Flink, Grafanan influxdb

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


#### Questions on Q&A
- Is the current visualization tool for stocks sufficient? Or should you as an user be able to change which stocks are monitored after starting the 
- Currently the specified tickers are sent to elasticsearch to be visualized in kibana. 
- Is it sufficient to just insert from csv file. What kind of improvements could we make here. Currently we are kind of frontrunning the "clock" because we have an historic dataset. 
- 
