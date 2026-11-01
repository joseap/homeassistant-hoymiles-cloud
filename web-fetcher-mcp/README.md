# Hoymiles S-Miles Cloud MCP Server

MCP server para monitorizar y controlar plantas solares Hoymiles a través de la API de S-Miles Cloud.

## Tools

| Tool | Descripción |
|------|-------------|
| `hoymiles_login` | Login con user/pass. Devuelve token y lista de plantas |
| `hoymiles_realtime` | Datos en tiempo real: potencia, energía, batería, flujos |
| `hoymiles_station_info` | Info de la planta: capacidad, ubicación, propietario |
| `hoymiles_flow_data` | Flujos de energía: solar→cargas/batería/red |
| `hoymiles_power_day` | Producción horaria de un día |
| `hoymiles_set_mode` | Cambiar modo de batería |
| `hoymiles_list_plants` | Listar plantas accesibles |

## Modos de batería

| Modo | Código | Descripción |
|------|--------|-------------|
| `autoconsumo` | 1 | Solar→cargas→batería→red |
| `economico` | 2 | TOU, usa batería en horas pico |
| `respaldo` | 3 | Solo descarga en cortes |
| `fuera_red` | 4 | Sin acceso a red |
| `reduccion_picos` | 5 | Forzar carga batería |
| `temporizado` | 8000 | Prioridad configurable |

## Configuración Kilo

```json
{
  "mcpServers": {
    "hoymiles": {
      "command": "uv",
      "args": ["run", "--project", "/home/joseap/Proyectos/Ask/web-fetcher-mcp", "python", "-m", "web_fetcher_mcp.server"],
      "cwd": "/home/joseap/Proyectos/Ask/web-fetcher-mcp"
    }
  }
}
```

## Ejemplos de uso

### Login
```json
{"session_key": "default", "username": "joseapg", "password": "tu_password"}
```

### Datos en tiempo real
```json
{"session_key": "default", "station_id": 11659660}
```

### Cambiar modo a autoconsumo
```json
{"session_key": "default", "station_id": 11659660, "mode": "autoconsumo"}
```

### Flujos de energía de hoy
```json
{"session_key": "default", "station_id": 11659660}
```

## Polling cada minuto

Para obtener datos cada minuto, usa una automatización o script que llame `hoymiles_realtime` periódicamente. El token dura 1 hora.
