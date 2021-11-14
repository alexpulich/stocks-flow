# Stocks flow
This is a simple project I created to learn prefect, dask and other Data Engineering stuff

## How to run
I assume that you created a virtualenv and installed everything from `requirements.txt`
1. First, configure your [prefect backend](https://docs.prefect.io/orchestration/getting-started/set-up.html#prefect-cloud-and-core-server)
2. Create a prefect project `prefect create project "stocks"`
3. Register flows with `/bin/sh register_flows.sh`
