# RedisClusterWitness
watch redis cluster and failover when needed.
## Disclaimer
#### This method is not official way by Redis.
it may cause split brain in rare cases:

when master node is up and reachable from applications but it's not reachable from both slave node and witness node.

## Installation
### How to install in linux:
```
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```
### How to install in windows:
```
python -m venv venv
.\venv\Scripts\activate.bat
pip install -r requirements.txt
```

## Usage
before start you need to modify config.ini file.
```
python main.py
```
