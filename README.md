# SS-DM_Project
Course project for scalable systems and data management
run through 
´´´docker-compose up --build´´´
new terminal window to start flink job

´´´docker exec -it ss-dm_project-flink-jobmanager-1 /bin/bash´´´
´´cd ..´´
´´./bin/flink run -py /opt/flink/jobs/flink_processor.py´´´

## Kubernetes
´´´
eval $(minikube -p minikube docker-env)

docker build -t apache-flink-project:v1 . 

kubectl apply -f kubernetes/flink-jobmanager-deployment.yaml
kubectl apply -f kubernetes/flink-jobmanager-service.yaml
kubectl apply -f kubernetes/flink-taskmanager.yaml           
kubectl apply -f kubernetes/flink-taskmanager-service.yaml

´´´
