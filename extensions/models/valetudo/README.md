# @mgreten/valetudo

A general-purpose [Swamp](https://swamp.club) model extension for discovering,
monitoring, and controlling robot vacuums running
[Valetudo](https://valetudo.cloud/) through Valetudo's MQTT interface. It reads
the Homie metadata published by each robot, so it does not assume that every
vendor or model exposes the same capabilities, properties, or preset values.

The extension talks directly to an MQTT broker. Home Assistant, Node-RED, and a
particular Mosquitto deployment are not required. Broker credentials are
optional and can be supplied with a Swamp vault expression.

## Features

- Discover advertised capabilities, properties, types, ranges, and enum values
- Read normalized robot, battery, dock, attachment, preset, map-segment, and
  event status
- Start, stop, pause, dock, locate, manually empty the dock, and clean segments
- Set any advertised settable property, with Homie enum/range validation
- Refuse commands to missing or non-settable properties
- Avoid broad MQTT subscriptions that would unnecessarily download map payloads

## Prerequisites

Enable MQTT in Valetudo under **Connectivity → MQTT connectivity**. Record the
broker URL, topic prefix, and robot identifier shown there. The machine running
Swamp must be able to reach the MQTT broker.

## Installation

```sh
swamp extension pull @mgreten/valetudo
```

## Model creation

For an anonymous local broker:

```sh
swamp model create @mgreten/valetudo robot-vacuum \
  --global-arg brokerUrl=mqtt://192.0.2.10:1883 \
  --global-arg topicPrefix=valetudo \
  --global-arg identifier=my-robot
```

For an authenticated broker, store the password in a vault and pass it lazily:

```sh
swamp model create @mgreten/valetudo robot-vacuum \
  --global-arg brokerUrl=mqtts://mqtt.example.net:8883 \
  --global-arg username=valetudo \
  --global-arg 'password=${{ vault.get(mqtt-secrets, VALETUDO_PASSWORD) }}' \
  --global-arg topicPrefix=valetudo \
  --global-arg identifier=my-robot
```

## Discovery and status

Discover the exact capabilities exposed by this robot before building reusable
workflows around it:

```sh
swamp model method run robot-vacuum discover
swamp model method run robot-vacuum getStatus
```

The discovery output records the Homie `$datatype`, `$settable`, `$format`, and
other metadata for every advertised property. Status output is normalized for
common Valetudo attributes while tolerating optional capabilities.

## Robot actions

Basic controls:

```sh
swamp model method run robot-vacuum action --input action=start
swamp model method run robot-vacuum action --input action=pause
swamp model method run robot-vacuum action --input action=home
swamp model method run robot-vacuum action --input action=locate
```

Clean selected map segments:

```sh
swamp model method run robot-vacuum action \
  --input action=clean-segments \
  --input 'segmentIds=["20","18","16"]' \
  --input iterations=2 \
  --input customOrder=true
```

Set a robot-specific property after discovery. The extension checks that the
property is advertised as settable and validates enum or integer-range metadata
before publishing:

```sh
swamp model method run robot-vacuum action \
  --input action=set-property \
  --input capability=FanSpeedControlCapability \
  --input property=preset \
  --input value=medium
```

Commands use QoS 1 and are never retained. The model records the validated topic
and payload after MQTT confirms publication; this is not a guarantee that the
physical robot completed the requested action. Run `getStatus` afterward when a
workflow needs observed state.

## Security

Prefer a broker account restricted to the selected robot's topic tree. Store
passwords in Swamp vault, not model YAML. The extension does not log credentials
or include them in output resources. `set-property` rejects MQTT wildcards and
will not publish unless Valetudo advertises the target property as settable.

## License

MIT — see `LICENSE.txt`.
