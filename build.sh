docker build -t sea-client -f client.Dockerfile .
docker build -t sea-master -f master.Dockerfile .

docker images | grep "<none>" | gsed 's/\s\s*/ /g' | cut -d" " -f3 | xargs docker rmi

kubectl delete deploy sea-master sea-deployment
kubectl delete svc sea-master

kubectl create -f k8s_cfgs/job-tracker.yaml
kubectl create -f k8s_cfgs/job-tracker-svc.yaml
kubectl create -f k8s_cfgs/task-tracker.yaml
