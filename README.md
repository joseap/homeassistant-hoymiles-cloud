# Hoymiles Cloud Integration for Home Assistant

This custom integration for Home Assistant allows you to monitor and control your Hoymiles solar inverter system through the Hoymiles Cloud API.

## Features

- **Data Monitoring:**
  - Solar PV power generation
  - Battery power (charge/discharge)
  - Battery state of charge
  - Grid power import/export
  - Load power consumption
  - Daily and total energy generation
  
- **Control Functions:**
  - Set battery operation mode (Self-Consumption, Economy, Backup, Off-Grid, Peak Shaving, Time of Use)
  - Configure battery reserve state of charge
  - Peak Shaving Mode specific settings (max_soc, meter_power)

## Development Status

This is the first version of the integration, with focus on:
- Reading basic data from Hoymiles inverters
- Setting the battery mode
- Configuring the self-consumption SOC and other basic parameters

Not all available sensors and settings are implemented yet, and additional functionality is still being worked on. This integration is a work in progress and will be expanded in future updates.

## Installation

### HACS (Recommended)

1. Make sure you have [HACS](https://hacs.xyz/) installed
2. Go to HACS → Integrations → Plus Icon → "Add Custom Repository"
3. Enter the URL: `https://github.com/Philra94/homeassistant-hoymiles-cloud`
4. Select category: "Integration"
5. Click "Add"
6. Find and install "Hoymiles Cloud"
7. Restart Home Assistant

### Manual Installation

1. Download this repository
2. Copy the `custom_components/hoymiles_cloud` directory to your Home Assistant `custom_components` directory
3. Restart Home Assistant

## Configuration

1. Go to Home Assistant → Settings → Devices & Services → Add Integration
2. Search for "Hoymiles Cloud"
3. Enter your Hoymiles Cloud login credentials (same as used in the S-Miles Cloud app/website)
4. Click "Submit"

## Usage

After configuration, the integration will create:

- A device for each Hoymiles station with sensors for power, energy, and battery levels
- Controls for battery mode and reserve capacity settings

The sensors will update every minute by default, but this can be changed in the integration options.

## Notes

- Password encryption/hashing is simplified in this version. For production use, it should be properly implemented according to the actual encryption method used by Hoymiles.
- Error handling is basic. The integration will log errors but may not handle all error cases gracefully.
- API endpoints and data structures are based on the observed behavior of the official Hoymiles Cloud API as of 2023-2024.

## Contributing

Contributions are welcome! If you have recommendations, improvements, or additions to this repository, please feel free to:
- Open an issue with your suggestions
- Create a pull request with your changes
- Share your feedback on what could be improved

Please note that this code was developed with the assistance of AI, which may explain why some parts are not always the prettiest or most straightforward. Your help in improving and refining the codebase would be greatly appreciated.

## Troubleshooting

- Check Home Assistant logs for details about any errors
- Verify your Hoymiles Cloud credentials
- Ensure your Home Assistant instance has internet access to reach the Hoymiles Cloud API

## Support

For bugs or feature requests, please [open an issue on GitHub](https://github.com/Philra94/homeassistant-hoymiles-cloud/issues).

## Disclaimer

This integration is not affiliated with, endorsed by, or connected to Hoymiles Power Electronics Inc. This is a third-party integration developed for personal use. 

## Dashboard Batería

Sección del dashboard `lovelace` para controlar los modos de batería:

```yaml
# Sección "Batería" - views[4] del dashboard lovelace
type: sections
title: Bateria
path: Bateria
sections:
  # Backup (SOC 10%)
  - type: grid
    cards:
      - type: heading
        heading: New section
      - type: vertical-stack
        cards:
          - type: custom:mushroom-title-card
            title: Hoymiles - Backup
            subtitle: Reserva SOC + activar modo
          - type: custom:mushroom-number-card
            entity: input_number.hoymiles_backup_reserve_soc
            name: Reserve SOC (Backup)
            icon: mdi:battery-lock
          - type: custom:mushroom-template-card
            primary: Activar Backup
            secondary: Aplica SOC y cambia a Backup
            icon: mdi:battery-check
            tap_action:
              action: call-service
              service: script.hoymiles_backup_aplicar_y_activar
          - type: custom:mushroom-title-card
            title: " "
            subtitle: " "
          - type: custom:mushroom-title-card
            title: Hoymiles - Autoconsumo
            subtitle: Reserva SOC + activar modo
          - type: custom:mushroom-number-card
            entity: input_number.hoymiles_self_consumption_reserve_soc
            name: Reserve SOC (Autoconsumo)
            icon: mdi:home-lightning-bolt
          - type: custom:mushroom-template-card
            primary: Activar Autoconsumo
            secondary: Aplica SOC y cambia a Autoconsumo
            icon: mdi:home-lightning-bolt
            tap_action:
              action: call-service
              service: script.hoymiles_autoconsumo_aplicar_y_activar

  # TOU 2 periodos
  - type: grid
    cards:
      - type: heading
        heading: New section
      - type: vertical-stack
        cards:
          - type: custom:mushroom-title-card
            title: Hoymiles - TOU (Modo 8)
            subtitle: 2 periodos (una llamada)
          - type: custom:mushroom-title-card
            title: Parámetro global
            subtitle: SOC reservado
          - type: custom:mushroom-number-card
            entity: input_number.tou_reserve_soc
            name: Reserve SOC (%)
            icon: mdi:battery-lock
          - type: custom:mushroom-title-card
            title: Periodo 1
            subtitle: Carga / Descarga + Potencias/SOC
          - type: entities
            entities:
              - entity: input_datetime.tou_charge_start
                name: Carga inicio
              - entity: input_datetime.tou_charge_end
                name: Carga fin
              - entity: input_number.tou_charge_power
                name: Potencia carga (%)
              - entity: input_number.tou_charge_soc
                name: SOC carga (%)
              - entity: input_datetime.tou_discharge_start
                name: Descarga inicio
              - entity: input_datetime.tou_discharge_end
                name: Descarga fin
              - entity: input_number.tou_discharge_power
                name: Potencia descarga (%)
              - entity: input_number.tou_discharge_soc
                name: SOC descarga (%)
          - type: custom:mushroom-title-card
            title: Periodo 2
            subtitle: Carga2 / Descarga2 + Potencias/SOC
          - type: entities
            entities:
              - entity: input_datetime.tou_charge2_start
                name: Carga2 inicio
              - entity: input_datetime.tou_charge2_end
                name: Carga2 fin
              - entity: input_number.tou_charge2_power
                name: Potencia carga2 (%)
              - entity: input_number.tou_charge2_soc
                name: SOC carga2 (%)
              - entity: input_datetime.tou_discharge2_start
                name: Descarga2 inicio
              - entity: input_datetime.tou_discharge2_end
                name: Descarga2 fin
              - entity: input_number.tou_discharge2_power
                name: Potencia descarga2 (%)
              - entity: input_number.tou_discharge2_soc
                name: SOC descarga2 (%)
          - type: custom:mushroom-template-card
            primary: Aplicar TOU (2 periodos)
            secondary: Envía el JSON completo a Hoymiles
            icon: mdi:battery-clock
            tap_action:
              action: call-service
              service: script.hoymiles_aplicar_tou_2_periodos

  # TOU visual
  - type: grid
    cards:
      - type: heading
        heading: New section
      - type: vertical-stack
        cards:
          - type: custom:mushroom-title-card
            title: Hoymiles – TOU (Modo 8)
            subtitle: Configura horarios, potencia y SOC
          - type: custom:mushroom-title-card
            title: Carga
          - type: grid
            columns: 2
            square: false
            cards:
              - type: custom:mushroom-entity-card
                entity: input_datetime.tou_charge_start
                name: Inicio
                icon: mdi:clock-start
              - type: custom:mushroom-entity-card
                entity: input_datetime.tou_charge_end
                name: Fin
                icon: mdi:clock-end
              - type: custom:mushroom-entity-card
                entity: input_number.tou_charge_power
                name: Potencia (%)
                icon: mdi:flash
              - type: custom:mushroom-entity-card
                entity: input_number.tou_charge_soc
                name: Detener SOC (%)
                icon: mdi:battery-80
          - type: custom:mushroom-title-card
            title: Descarga
          - type: grid
            columns: 2
            square: false
            cards:
              - type: custom:mushroom-entity-card
                entity: input_datetime.tou_discharge_start
                name: Inicio
                icon: mdi:clock-start
              - type: custom:mushroom-entity-card
                entity: input_datetime.tou_discharge_end
                name: Fin
                icon: mdi:clock-end
              - type: custom:mushroom-entity-card
                entity: input_number.tou_discharge_power
                name: Potencia (%)
                icon: mdi:flash-outline
              - type: custom:mushroom-entity-card
                entity: input_number.tou_discharge_soc
                name: Detener SOC (%)
                icon: mdi:battery-30
          - type: custom:mushroom-entity-card
            entity: input_number.tou_reserve_soc
            name: SOC reserva (%)
            icon: mdi:shield-battery
          - type: custom:mushroom-template-card
            primary: Aplicar TOU
            secondary: Aplica el modo TOU (modo 8) y el horario configurado
            icon: mdi:battery-clock
            tap_action:
              action: call-service
              service: script.aplicar_tou_hoymiles
            color: blue

  # Respaldo (SOC 95%)
  - type: grid
    cards:
      - type: heading
        heading: New section
      - type: vertical-stack
        cards:
          - type: custom:mushroom-title-card
            title: Hoymiles - Respaldo
            subtitle: Reserva SOC + activar modo
          - type: custom:mushroom-number-card
            entity: input_number.hoymiles_respaldo_reserve_soc
            name: Reserve SOC (Respaldo)
            icon: mdi:battery-lock
          - type: custom:mushroom-template-card
            primary: Activar Respaldo
            secondary: Aplica SOC y cambia a Respaldo
            icon: mdi:battery-check
            tap_action:
              action: call-service
              service: script.hoymiles_respaldo_aplicar_y_activar
```

## Scripts

### Hoymiles - Backup
```yaml
alias: Hoymiles - Backup - aplicar SOC y activar
mode: single
sequence:
  - data:
      station_id: 11659660
      reserve_soc: "{{ states('input_number.hoymiles_backup_reserve_soc') | int }}"
    action: hoymiles_cloud.set_backup_mode
```

### Hoymiles - Autoconsumo
```yaml
alias: Hoymiles - Autoconsumo - aplicar SOC y activar
mode: single
sequence:
  - data:
      station_id: 11659660
      reserve_soc: "{{ states('input_number.hoymiles_self_consumption_reserve_soc') | int }}"
    action: hoymiles_cloud.set_self_consumption_mode
```

### Hoymiles - Respaldo
```yaml
alias: Hoymiles - Respaldo - aplicar SOC y activar
mode: single
sequence:
  - data:
      station_id: 11659660
      reserve_soc: "{{ states('input_number.hoymiles_respaldo_reserve_soc') | int }}"
    action: hoymiles_cloud.set_backup_mode
```

## Helpers

| Entity | Type | Min | Max | Default | Unit |
|--------|------|-----|-----|---------|------|
| `input_number.hoymiles_backup_reserve_soc` | input_number | 10 | 100 | 10 | % |
| `input_number.hoymiles_self_consumption_reserve_soc` | input_number | 10 | 100 | 10 | % |
| `input_number.hoymiles_respaldo_reserve_soc` | input_number | 10 | 100 | 95 | % |
| `input_number.tou_reserve_soc` | input_number | 0 | 100 | 10 | % |