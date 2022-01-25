# Schema generator

This `generate_schema.py` script can be used to generate schema files for the vtiger
connector. This is experimental and may change a lot or go away.

## Installation
To be able to run you require the `curl`, `jq` and the `genson` command. `curl` and `jq` should be available via your favourite package manager. Instructions to install `genson` can be found here: https://github.com/wolverdude/genson/#installation

## How to run it
```
python3 ./scripts/generate_schema.py -h
```
for example (if you are in the root of the connector folder)
```
python ./scripts/generate_schema.py ./secrets/config.json source_vtiger/schemas

```
*Please remember to add the list of modules you want to generate schemas for in the script (vtiger_types) before you run it!*