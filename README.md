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
Go to http://localhost:8086/ and login and obtain token 
after the first start up the same token be used by setting it in the env file used for the processor.py script as well for grafana to obtain data from infulx

Run following command
``docker-compose up --build kafka_to_influx``
Just to make sure the script start succesfully as it is built from a different dockerfile than rest of containers

Data will no be put into influxdb bucket and saved for one hour
login to both influx and grafana with user admin and password admin123 
grafana dashboard avaible at http://localhost:3000/ 

## Grafana

Go to Grafan and add datasource.
First need to obtain a api key from influxdB named Grafana for example
After that add
host http://influxdb:8086/
and the rest like organistaion: ticker_org
and bucket: ticker_bucket

then the data source should be configured make sure the dahsboard uses that datasource!

The data source need to be added manually use thee same token as the kafka to influx script uses.

After that the dahsboard needs to be uploaded manually, after the dahsboard has been set in .env file!

After that will need to update every view manully by selecting the datasource and executing the query!!


and the token then click save and test.

Then choose the dashboard that is already up in grafana and follow the data


## Sort files 

sort_file.py will sort the input files by time for testing porpuses.

