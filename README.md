# gridappsd-distributed-static-ybus-service:
This is a service program for the GridAPPS-D distributed field bus. It
calculates the Ybus for a GridAPPS-D distributed area in a static electrical feeder model and provides the result to any other GridAPPS-D distributed field bus applications that make a request to the service.
## Requirements:
`gridappsd-distributed-static-ybus-service` requires a Python version >= 3.10 and < 4. No testing has be done with Python 4 to date.
## Installation:
`gridappsd-distributed-static-ybus-service` is available through pip.
```shell
pip3 install gridappsd-distributed-static-ybus-service
```
## Usage:
Invoke `distributed_static_ybus_service.py` from the command line with a combination of the following keyword assinged arguments:
Keyword                         | Value
------------------------------- | --------------------------------------------
SIMULATION_ID                   | Simulation id
SYSTEM_BUS_CONFIG_FILE          | Full file path of the system bus configuration file
FEEDER_BUS_CONFIG_FILE          | Full file path of the feeder bus configuration file
SWITCH_BUS_CONFIG_FILE_DIR      | Full path to the directory of the switch bus configuration file(s)
SECONDARY_BUS_CONFIG_FILE_DIR   | Full path to the director of the secondary bus configuration files(s)

### Examples:
To start a single feeder level static ybus service agent:
```shell
python3 distributed_static_ybus_service.py SYSTEM_BUS_CONFIG_FILE=/home/usr/my_system_bus_config_file.yml FEEDER_BUS_CONFIG_FILE=/home/usr/my_feeder_bus_config_file.yml
```
To start a single switch level static ybus service agent:
```shell
python3 distributed_static_ybus_service.py FEEDER_BUS_CONFIG_FILE=/home/usr/my_system_bus_config_file.yml SWITCH_BUS_CONFIG_FILE_DIR=/home/usr/my_switch_bus_configs_dir
```
To start a single secondary level static ybus service agent:
```shell
python3 distributed_static_ybus_service.py SWITCH_BUS_CONFIG_FILE_DIR=/home/usr/my_switch_bus_configs_dir SECONDARY_BUS_CONFIG_FILE_DIR=/home/usr/my_secondary_bus_configs_dir
```

#### Existing Configurations:
Within this repository is a directory named `service_config_files`. This directory contains all configuration files for all the field bus areas that exist for the `ieee13nodeckt`, `ieee13nodecktassets`, `ieee123`, and `ieee123pv` models.
