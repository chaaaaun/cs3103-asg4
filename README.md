# gameNetAPI

## Setting Up

1. [Install uv](https://docs.astral.sh/uv/getting-started/installation/)
2. Run `uv run scripts/client.py` and/or `uv run scripts/server.py`

## Developing

- The `gameNetAPI` source code is contained within the `src/gamenetapi/` directory
- If you need to edit the test client or servers, find the files in `scripts/`
- With this setup we can import and use functions from the API in client/server just like we would normal packages.

# from repo root
export PYTHONPATH=src

# create venv in WSL home (fast)
python3 -m venv ~/.venvs/ass4
source ~/.venvs/ass4/bin/activate
python -m pip install --upgrade pip
python -m pip install matplotlib

## Testing / Running HUDP / Baseline UDP
```shell
uv run scripts/client.py --protocol udp/hudp

usage: client.py [-h] --protocol {udp,unreliable,reliable,hybrid} [--addr ADDR] [--port PORT] [--pps PPS] [--duration DURATION] [--payload PAYLOAD]

example command usage: 

# Baseline UDP
uv run scripts/client.py --protocol udp         --addr 127.0.0.1 --port 9999 --pps 200 --duration 10 --payload 64

# HUDP (unreliable only)
uv run scripts/client.py --protocol unreliable  --addr 127.0.0.1 --port 9999 --pps 200 --duration 10 --payload 64

# HUDP (reliable only)
uv run scripts/client.py --protocol reliable    --addr 127.0.0.1 --port 9999 --pps 200 --duration 10 --payload 64

# HUDP (hybrid mix)
uv run scripts/client.py --protocol hybrid      --addr 127.0.0.1 --port 9999 --pps 200 --duration 10 --payload 64

pps = packets per second (send rate)
duration = seconds to run
payload = user payload bytes
```

```shell
uv run scripts/server.py --protocol udp/hudp

usage: server.py [-h] --protocol {udp,hudp} [--addr ADDR] [--port PORT] [--timeout TIMEOUT]
server.py: error: the following arguments are required: --protocol/-p

example command usage: uv run scripts/server.py --protocol hudp --addr 127.0.0.1 --port 9999 --timeout 60

pps refers to packets per second, i.e. the sending rate
```

## Analysis to generate CSV and graphs

```shell
1st step: Ensure that matplotlib is install first, see create venv in WSL home.

It might help to run export PYTHONPATH=src beforehand. 

2nd step: Start the client by running the command: uv run scripts/server.py --protocol hudp --addr 127.0.0.1 --port 9999 --timeout 60.

you can tune the parameters as needed.

3rd step: Run the analysis file by running the command: uv run scripts/analyse_client.py --mode hudp-r --addr 127.0.0.1 --port 9999 --pps 200 --duration 30 --payload 64

example usage:

# Reliable pipelined (ACK-based)
uv run scripts/analyze_client.py --mode hudp-r --addr 127.0.0.1 --port 9999 --pps 200 --duration 30 --payload 64

# Unreliable echo
uv run scripts/analyze_client.py --mode hudp-u --addr 127.0.0.1 --port 9999 --pps 200 --duration 30 --payload 64

# Baseline UDP echo
uv run scripts/analyze_client.py --mode udp    --addr 127.0.0.1 --port 9999 --pps 200 --duration 30 --payload 64

you can tune the parameters as needed.

4th step: Generate the plotted graphs by running the command: PYTHONPATH=src uv run scripts/plot_metrics.py or uv run scripts/plot_metrics.py.

5th step: If you want to generate the metrics to plot the graphs again, remove the metrics.csv file first by running the command: rm -f metrics.csv in the folder that the metrics.csv file is in. Then you can run the program from step 2 again.
```

Network modes
```shell
linux environment only to simulate packet loss network conditions

# Good
sudo tc qdisc add dev lo root netem delay 50ms 10ms loss 1%

# Average
sudo tc qdisc add dev lo root netem delay 200ms 25ms loss 5%

# Poor
sudo tc qdisc add dev lo root netem delay 500ms 50ms loss 15%

# Restore default network conditions
sudo tc qdisc del dev lo root
``` 
