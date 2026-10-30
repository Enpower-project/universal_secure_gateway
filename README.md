# Universal Secure Gateway

Universal Secure Gateway (USG), developed by **Comsensus d.o.o.**, connects energy-device drivers and simulations through NATS. 

Docker Compose runs a NATS server and a Python process manager. The manager loads [config/config.yaml](config/config.yaml) and launches plugins, the status manager, the optional ECMP MQTT interface, and asset drivers as separate processes.

## Repository layout

| Path | Purpose |
| --- | --- |
| [core/usg.py](core/usg.py) | Configuration, process startup, process checks, and log consumption |
| [core/status.py](core/status.py) | Status request handler with example service data |
| [config/config.yaml](config/config.yaml) | Gateway identity, assets, plugins, and MQTT settings |
| [drivers/](drivers/) | Battery simulations, workbook replay, and device integrations |
| [plugins/](plugins/) | Clock, traffic logger, MQTT bridge, and DNP3 executable |
| [utils/nats_logger.py](utils/nats_logger.py) | JSON logging over NATS |
| [Dockerfile](Dockerfile) | Python 3.14 image and certificate installation |
| [docker-compose.yml](docker-compose.yml) | NATS and USG services on the `usg` network |
| [requirements.in](requirements.in) / [requirements.txt](requirements.txt) | Dependency inputs / pinned dependencies with hashes |

## Prerequisites and preparation

Use Docker with Compose v2 and a builder supporting Dockerfile cache mounts. The build downloads the base image, system packages, and Python dependencies. 

Before building:

1. Review `config/config.yaml`. Replace the example MQTT hostname and credentials, or replace the entire ECMP mapping with `ecmp: null` to disable MQTT.
2. Supply `certs/ca-chain.cert.pem`, `certs/client.cert.pem`, and `certs/client.key.pem`. Dockerfile copies them unconditionally, so they are required for the current build even when MQTT is disabled. Use certificates issued for your broker deployment when enabling MQTT.
3. If DNP3 is enabled, ensure `plugins/dnp3/dnp3-usg-north` exists, is executable, and supports the container platform. This checkout contains the executable without its build sources.
4. For workbook replay, make the XLSX file available inside the container and configure its container path.

Certificates are ignored by Git but copied into image layers. Treat built images as containing the client private key. Compose publishes NATS and DNP3 ports without host-address restrictions and configures no NATS authentication; choose host bindings and network access appropriate to the deployment. Configuration values can appear in debug logs.

## How to run Universal Secure Gateway

In command-line (e.g., PowerShell) go to the target directory and run the following commands:

```
docker compose build
docker compose up -d   
```

To stop the USG deployment now write the following command:

```
docker compose down
```

## What is exposed on DNP3 interface?

Battery storage simulation telemetry is exposed where telemetry data is updated on a 5s basis.

```yaml
  analog_inputs:
    - { index: 0, subject: "744b3131-fc8e-4290-b6d6-bfc708e5b340.telemetry.active_power" }
    - { index: 1, subject: "744b3131-fc8e-4290-b6d6-bfc708e5b340.telemetry.state_of_charge" }
```

The mapping above is the original two-point example, placed under the DNP3 plugin’s `config.mapping`. The supplied battery polling interval is five seconds.

### TCP endpoint the outstation will bind (exposed to the DNP3 master)

```yaml
listen_addr: "0.0.0.0:20000"
```

### DNP3 link-layer addresses (master ↔ outstation)

```yaml
link:
  destination: 1024  # this outstation's address
  source: 1          # expected master address
```


## DEV: Installing and managing requirements

Edit requirements.in file for changes of requirements

### Install pip-tools

```sh
pip3 install pip-tools
```

### Compile requirements file

```sh
pip-compile --generate-hashes requirements.in
```

## Operating the deployment

Check service state and follow logs:

```sh
docker compose ps
docker compose logs -f usg
docker compose logs -f natsio
```

Source and configuration are copied into the image, with no working-directory bind mount. After changing them, rebuild and recreate USG:

```sh
docker compose up -d --build usg
```

Both services use `restart: unless-stopped`. This restarts exited containers, not individual drivers that exit while the manager remains alive. Compose defines no readiness health checks or startup dependencies.

## Configuration reference

The manager reads `code/config/config.yaml` relative to its working directory, corresponding to `/code/config/config.yaml` in the supplied container. Keep these top-level keys present:

| Key | Meaning |
| --- | --- |
| `usg_id` | Gateway identifier for MQTT controls |
| `root_topic` | MQTT control prefix; not a universal NATS prefix |
| `version` | Metadata, currently not validated |
| `status` | Status-process `exec` mapping, or `null` to disable |
| `ecmp` | MQTT settings and `exec`, or `null` to disable |
| `assets` | Asset list; use `[]` for no assets |
| `plugins` | Plugin list; use `[]` or `null` for no plugins |

The manager splits `exec` strings on whitespace and runs them without a shell. Shell syntax and paths containing spaces are not supported by this parsing. The executable selects the driver; `type`, `driver`, and `app` are descriptive metadata.

### Assets and drivers

Each asset entry supplies `id`, `name`, `exec`, `device_interface.config`, `device_config`, `polling_interval`, and `topics`. See the complete battery example in [config/config.yaml](config/config.yaml).

The manager exports `ASSET_ID`, `ASSET_NAME`, `IF_CONFIG`, `DEV_CONFIG`, `TOPICS`, `ROOT_TOPIC`, and `POLLING_INTERVAL`. Interface, device, and topic configurations are JSON encoded. Even drivers ignoring device configuration require that key because the manager accesses it at startup. Topic lists generally do not change the hardcoded subjects in drivers.

| Driver | Behavior and configuration |
| --- | --- |
| [generic_bss](drivers/generic_bss/asset.py) | Configurable battery, local polling, JSON `active_power` and `state_of_charge` telemetry |
| [generic_bss_dual_power](drivers/generic_bss_dual_power/asset.py) | Fixed 110 kWh battery, separate charge/discharge channels, JSON telemetry; ignores device configuration |
| [generic_bss_with_clock](drivers/generic_bss_with_clock/asset.py) | Configurable battery triggered by clock messages; each due update advances one polling interval |
| [file_stream_with_clock](drivers/file_stream_with_clock/asset.py) | First-worksheet XLSX replay; interface needs `file`; exact clock-to-row matching |
| [hoval_efhk](drivers/hoval_efhk/asset.py) | Modbus TCP heater; interface keys `ip`, `port`, `url`, `user`, `password`; aggregate MessagePack telemetry |
| [sonnen_performance_10](drivers/sonnen_performance_10/asset.py) | HTTP `/api/v2/status`; interface keys `url`, `user`, `password`; aggregate MessagePack telemetry |

Configurable battery device settings are `soc` (initial fraction, 0–1), `capacity` (Wh), `max_charge_power` and `max_discharge_power` (W), and `charge_efficiency` and `discharge_efficiency` (fractions). Positive signed power means discharge; negative means charge. Use a positive polling interval in seconds. The simulation drivers require interface keys `url`, `user`, and `password`, although they make no physical-device requests.

Workbook replay expects numeric Excel timestamps, supported numeric measurement columns, and inline-string headers. Supported headers, sparse-row limitations, and timezone behavior are described in its module docstring. All linked driver modules contain detailed configuration and method documentation.

### Plugins and clock

Plugin entries require `id`, `name`, `exec`, and `config`. The manager exports `PLUGIN_ID`, `PLUGIN_NAME`, and JSON-encoded `PLUGIN_CONFIG`.

Example clock plugin:

```yaml
- id: "clock"
  name: "clock"
  exec: "python3 /code/plugins/clock/main.py"
  config:
    tick: 1
    timestep: 900
    start_datetime: "2026-09-29T22:00:00"
    end_datetime: "2026-09-30T22:00:00"
```

Add this entry under `plugins`. After each one-second sleep, simulation time advances 900 seconds and the clock publishes JSON such as `{"timestamp": "2026-09-29T22:15:00"}` on `clock`. The starting timestamp is not published. `end_datetime` is parsed but does not stop publication.

Controls use `<plugin-id>.control.*` and `{"k": "clock_active", "v": "False"}`. Values are case-sensitive strings, not JSON booleans. An iteration already sleeping can publish once more after deactivation. Setting `"True"` after the publisher exits does not restart it.

The clock-based battery does not catch up across large clock jumps and timestamps telemetry with wall-clock UTC time. Workbook replay instead uses the matched workbook timestamp. Choose clock increments and update intervals accordingly.

### ECMP MQTT

Configure `ecmp.host`, TLS `port`, `cert`, and the proxy `exec`. The certificate directory must end in a slash; the supplied `/etc/ssl/certs/` contains the three files copied during the build. The manager exports `MQTT_HOST`, `MQTT_PORT`, `MQTT_CERTS`, `USG_ID`, and `ROOT_TOPIC`.

The proxy uses a CA certificate, client certificate, and private key. Configured `username` and `password` are currently unused. Replace the entire mapping with `ecmp: null` to disable it. Leaving `host:` blank creates Python `None` and fails subprocess startup. The existing shutdown routine still assumes an ECMP process exists when the interface is disabled.

## Messages and integration compatibility

| Producer | NATS subject | Encoding and shape |
| --- | --- | --- |
| Battery simulations / workbook replay | `<asset-id>.telemetry.<channel>` | JSON: `aid` (UUID string), `ts` (Unix microseconds), `k`, `v`, `u` |
| Hoval / Sonnen | `<asset-id>.telemetry` | MessagePack: `asset_id` (UUID bytes), `timestamp` (Unix microseconds), `data` (channel list) |
| Clock | `clock` | JSON with timestamp string |
| NATS logger utility | `logs.<process-name>` | JSON: `timestamp`, `level`, `process`, `message` |

The MQTT proxy consumes only three-token JSON telemetry matching `*.telemetry.*`. It converts `aid` to UUID bytes, encodes MessagePack, and publishes on the fixed MQTT topic `enpower/telemetry`, ignoring `root_topic` for outbound telemetry.

Inbound MQTT controls use `<root_topic>/<usg_id>/control` with MessagePack fields `aid` (UUID bytes), `k`, `v`, and `u`. The proxy converts the UUID to a string and publishes JSON on the exact NATS subject `<asset-id>.control`.

`generic_bss` and `generic_bss_with_clock` subscribe to `<asset-id>.control.*`; the proxy’s two-token control subject does not reach them. `generic_bss_dual_power` subscribes to `<asset-id>.control`. Hoval and Sonnen have no enabled control subscriptions. Check subjects, units, and payload formats across the full integration path.

The [traffic logger](plugins/logs/logger.py) decodes JSON on `*.telemetry.>`, `*.control.>`, `*.status.>`, and `clock`, forwarding it to `logs.nats_logger`. Those wildcard subjects require a suffix and exclude aggregate two-token subjects. The manager consumes `logs.>` and writes messages through standard logging.

The status manager serves JSON requests on `status.get`. Its `auth` and `billing` responses are example data; telemetry/control handlers do not yet maintain live asset health.

### Current DNP3 configuration

The checked-in configuration currently uses link values `destination: 100` and `source: 101`, maps scaled state of charge at analog input **48**, and configures CROB **18** and AO32 **93**. Coordinate addresses and point mappings with the DNP3 master. Compose publishes container TCP port 20000 on host port 20000.

The configured DNP3 control subjects target `enable_charge_discharge_mode` and `charge_discharge_active_power_target`. The generic battery expects the payload key `active_power` in watts. An agreed control mapping or adapter is needed to connect these conventions; a configured point alone does not establish compatibility.

## Troubleshooting

| Symptom | Check |
| --- | --- |
| Build cannot find certificate files | Supply all three Dockerfile certificate inputs, even with MQTT disabled |
| `TypeError` mentioning `NoneType` during subprocess creation | Required string settings, especially an empty `ecmp.host` |
| Shutdown raises an ECMP or asset cleanup exception | The manager unconditionally terminates ECMP and iterates asset dictionary keys as processes; inspect the preceding exception for the initial failure |
| Container repeatedly restarts | Read `docker compose logs usg` for the first startup exception |
| Changes have no effect | Rebuild and recreate USG; no source bind mount is configured |
| Telemetry or controls do not reach a consumer | Compare subject token counts, JSON/MessagePack formats, UUID representations, and units |
| Missing clock-based telemetry | Check clock progression, update thresholds, workbook timestamps, and publisher activation |
| Hoval Modbus argument errors | Reads use `slave`, while the write helper uses `device_id`; verify the installed pymodbus API |
| Plugin reports a missing environment variable | Compare the executable’s expected interface with `PLUGIN_ID`, `PLUGIN_NAME`, and `PLUGIN_CONFIG` supplied by the manager |

## Development notes

The Docker build installs `requirements.txt` using `pip3 install --require-hashes`. Keep generated hashes and dependency versions together. Use the pip-tools commands above after editing `requirements.in`.

Generate dependencies in a controlled environment matching the target Python and platform. The Dockerfile targets Python 3.14; the current lock-file header records generation with Python 3.10. Rebuild the image to validate dependency changes, then verify relevant integrations with a simulator or controlled device setup.

For contributions, update affected module documentation and configuration examples, explain message-format or unit changes, and describe validation performed. Keep credentials, private keys, and local deployment settings out of contributions.

## License

Copyright © 2026 Comsensus d.o.o.

This repository is licensed under the **Apache License, Version 2.0** (`Apache-2.0`). See [LICENSE](LICENSE) for the full text and [NOTICE](NOTICE) for project attribution. The official license is available from the [Apache Software Foundation](https://www.apache.org/licenses/LICENSE-2.0).

Unless required by applicable law or agreed to in writing, the software is distributed on an **“AS IS” basis, without warranties or conditions of any kind**, as described in the license. Third-party dependencies and components retain their respective licenses and notices.
