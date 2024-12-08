# README

## Spark ETL process for restaurant weather

### Launching an ETL in docker container
Clone project from GitHub:\
`git clone https://github.com/frozjeee/epam_spark.git`

Create .env file(you may not specify, it will work anyway):\
`OPENCAGE_API_URL=https://api.opencagedata.com/geocode/v1`\
`OPENCAGE_API_TOKEN={Token}`

Build docker container (On Windows ensure that End of line sequence is set to LF in entrypoint.sh file):\
`docker build -t spark_etl .`

To run container:\
`docker run -v $(pwd):/host_output spark_etl`

If error occurs write absolute path(In my case for Windows):\
`docker run -v C:\Users\{username}\Desktop\spark:/host_output spark_etl`
