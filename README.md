# README

## Spark ETL process for restaurant weather

### Launching an etl in docker container
Clone project from GitHub:\
`git clone https://github.com/frozjeee/MTS.git`

Build docker container (On Windows ensure that End of line sequence is set to LF in entrypoint.sh file):\
`docker build -t spark_etl .`

To run container:\
`docker run -v $(pwd):/host_output spark_etl`

If error occurs write absolute path(In my case for Windows):\
`docker run -v C:\Users\{username}\Desktop\spark:/host_output spark_etl`
