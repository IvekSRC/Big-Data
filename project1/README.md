## Pomocu sledece komande pokrecemo visekontejnersku aplikaciju

`sh script.sh docker-compose`

### Dodavanje seta podataka na HDFS

`docker cp oslo-bikes.csv namenode:/data`
`docker exec -it namenode bash`
`hdfs dfs -mkdir /dir`
`hdfs dfs -put /data/oslo-bikes.csv /dir`
`hdfs dfs -ls /dir`

## Pokretanje u CLUSTER-u

### Build

`sh script.sh docker-build`

### Run

`sh script.sh docker-run`
