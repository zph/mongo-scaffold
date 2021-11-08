# Mongo scaffold

A test bed for local testing of sharded clusters using mlaunch + keyhole + bash.

```
# Setup a virtual env with your favorite tool, I use direnv's "layout functionality"
# Or yolo it and do systemwide install if you skip the venv stage
make setup

# Load fake data in keyhole db
make seed

# Connect to mongo sharded cluster
make connect

# Cleanup and stop running cluster
make clean
```
