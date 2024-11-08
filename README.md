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

kubectl port-forward deployment/flink-jobmanager 8081:8081

´´´
New terminal window

´´´
kubectl exec -it flink-jobmanager-5d4c5bdb9b-8qds4 -- bash -c "cd /opt/flink && ./bin/flink run -m flink-jobmanager:8081 -py /opt/flink/jobs/flink_processor.py"

´´´

