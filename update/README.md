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
```optimazation
docker inspect --format '{{.State.Pid}}' control
docker-compose run pyspy record -o myprofile.svg --pid <id>
```

```mermaidjs

```
