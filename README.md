# Distributed Systems 2025 Course Project

```sh
./compile.sh
docker-compose up -d --build
```

You need to link the Prometheus to Grafana in port 3000

```path
Prometheus
http://localhost:9090/targets
Grafana
http://localhost:3000
```

```optimization
docker inspect --format '{{.State.Pid}}' control
docker-compose run pyspy record -o myprofile.svg --pid <id>
```

## Deployment

```sh
# on host machine
# compile proto files
./compile.sh

# build containers
docker compose pull && docker compose build

# replace credentials in charts
cd charts
grep "<REPLACE>" *


# deploy to cluster
sudo kubectl apply -f .

# remove from cluster
sudo kubectl delete -f  .
```
for svc in $(kubectl get svc --no-headers -o custom-columns=":metadata.name"); do
  minikube service "$svc" --url &
done
