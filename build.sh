echo "building images..."
docker build -t sea-client -f client.Dockerfile .
docker build -t sea-master -f master.Dockerfile .

echo "clean old images..."
docker images | grep "<none>" | gsed 's/\s\s*/ /g' | cut -d" " -f3 | xargs docker rmi

echo "restart pods..."
kubectl delete pods -l "app in (sea-job-tracker, sea-task-tracker)"