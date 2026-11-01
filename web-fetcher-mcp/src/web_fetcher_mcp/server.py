"""Hoymiles S-Miles Cloud MCP Server - Monitor and control solar plants."""

import asyncio
import base64
import hashlib
import json
import time
from typing import Any

import httpx
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent

app = Server("hoymiles-mcp")

API_BASE = "https://neapi.hoymiles.com"
DEFAULT_HEADERS = {
    "accept": "application/json",
    "content-type": "application/json; charset=UTF-8",
    "language": "es-es",
    "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
    "referer": "https://global.hoymiles.com/",
}

BATTERY_MODES = {
    "autoconsumo": 1,
    "economico": 2,
    "respaldo": 3,
    "fuera_red": 4,
    "reduccion_picos": 5,
    "temporizado": 8000,
}

_sessions: dict[str, dict] = {}


def encrypt_password(password: str, nonce: str) -> str:
    """Encrypt password using V2 method: md5(password).base64(sha256(password))."""
    md5_hash = hashlib.md5(password.encode("utf-8")).hexdigest()
    sha256_b64 = base64.b64encode(hashlib.sha256(password.encode("utf-8")).digest()).decode()
    return f"{md5_hash}.{sha256_b64}"


async def api_post(session: httpx.AsyncClient, path: str, body: dict, token: str | None = None) -> dict:
    """Make authenticated API call."""
    headers = dict(DEFAULT_HEADERS)
    if token:
        headers["authorization"] = token
    resp = await session.post(f"{API_BASE}{path}", json=body, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json()


async def login(username: str, password: str) -> dict:
    """Login and return session data with token."""
    async with httpx.AsyncClient() as client:
        pre_resp = await api_post(client, "/iam/pub/3/auth/pre-insp", {"u": username})
        if pre_resp.get("status") != "0":
            raise ValueError(f"pre-insp failed: {pre_resp}")
        nonce = pre_resp["data"]["n"]
        encrypted_pw = encrypt_password(password, nonce)
        login_resp = await api_post(client, "/iam/pub/3/auth/login", {"u": username, "ch": encrypted_pw, "n": nonce})
        if login_resp.get("status") != "0":
            raise ValueError(f"Login failed: {login_resp}")
        token = login_resp["data"]["token"]
        user_resp = await api_post(client, "/iam/api/1/user/me", {}, token=token)
        user_data = user_resp.get("data", {})
        plants_resp = await api_post(client, "/pvm/api/0/station/select_by_page", {"page": 1, "rows": 100}, token=token)
        plants = plants_resp.get("data", {}).get("list", [])
        return {"token": token, "username": username, "plants": plants, "user": user_data}


def get_session(session_key: str) -> dict:
    """Get or create a session."""
    if session_key not in _sessions:
        raise ValueError(f"No session found for '{session_key}'. Login first.")
    entry = _sessions[session_key]
    if time.time() - entry["login_time"] > 3600:
        raise ValueError("Session expired. Login again.")
    return entry


@app.list_tools()
async def list_tools() -> list[Tool]:
    return [
        Tool(
            name="hoymiles_login",
            description="Login to Hoymiles S-Miles Cloud. Returns token and plant list.",
            inputSchema={
                "type": "object",
                "properties": {
                    "session_key": {"type": "string", "description": "Session identifier (e.g. 'default' or your name)"},
                    "username": {"type": "string", "description": "Hoymiles username/email"},
                    "password": {"type": "string", "description": "Hoymiles password"},
                },
                "required": ["session_key", "username", "password"],
            },
        ),
        Tool(
            name="hoymiles_realtime",
            description="Get real-time plant data: power, energy today/month/year/lifetime, battery SOC, flow data, CO2 savings.",
            inputSchema={
                "type": "object",
                "properties": {
                    "session_key": {"type": "string", "description": "Session key from login"},
                    "station_id": {"type": "integer", "description": "Plant/station ID"},
                },
                "required": ["session_key", "station_id"],
            },
        ),
        Tool(
            name="hoymiles_station_info",
            description="Get station configuration: capacity, battery capacity, location, owner, installer info.",
            inputSchema={
                "type": "object",
                "properties": {
                    "session_key": {"type": "string"},
                    "station_id": {"type": "integer"},
                },
                "required": ["session_key", "station_id"],
            },
        ),
        Tool(
            name="hoymiles_flow_data",
            description="Get energy flow data: solar to loads/battery/grid, battery in/out, grid import/export.",
            inputSchema={
                "type": "object",
                "properties": {
                    "session_key": {"type": "string"},
                    "station_id": {"type": "integer"},
                    "date": {"type": "string", "description": "Date in YYYY-MM-DD format (default: today)"},
                },
                "required": ["session_key", "station_id"],
            },
        ),
        Tool(
            name="hoymiles_power_day",
            description="Get power production data for a specific day with 5-min intervals.",
            inputSchema={
                "type": "object",
                "properties": {
                    "session_key": {"type": "string"},
                    "station_id": {"type": "integer"},
                    "date": {"type": "string", "description": "Date in YYYY-MM-DD format (default: today)"},
                },
                "required": ["session_key", "station_id"],
            },
        ),
        Tool(
            name="hoymiles_set_mode",
            description=f"Change battery operating mode. Modes: {', '.join(BATTERY_MODES.keys())}.",
            inputSchema={
                "type": "object",
                "properties": {
                    "session_key": {"type": "string"},
                    "station_id": {"type": "integer"},
                    "mode": {"type": "string", "enum": list(BATTERY_MODES.keys()), "description": "Battery mode name"},
                    "reserve_soc": {"type": "integer", "description": "Reserved SOC percentage (10-100, default: 10)", "default": 10},
                },
                "required": ["session_key", "station_id", "mode"],
            },
        ),
        Tool(
            name="hoymiles_list_plants",
            description="List all plants accessible to the logged-in user.",
            inputSchema={
                "type": "object",
                "properties": {
                    "session_key": {"type": "string"},
                },
                "required": ["session_key"],
            },
        ),
    ]


@app.call_tool()
async def call_tool(name: str, arguments: Any) -> list[TextContent]:
    if name == "hoymiles_login":
        return await tool_login(arguments)
    elif name == "hoymiles_realtime":
        return await tool_realtime(arguments)
    elif name == "hoymiles_station_info":
        return await tool_station_info(arguments)
    elif name == "hoymiles_flow_data":
        return await tool_flow_data(arguments)
    elif name == "hoymiles_power_day":
        return await tool_power_day(arguments)
    elif name == "hoymiles_set_mode":
        return await tool_set_mode(arguments)
    elif name == "hoymiles_list_plants":
        return await tool_list_plants(arguments)
    raise ValueError(f"Unknown tool: {name}")


async def tool_login(args: dict) -> list[TextContent]:
    session_key = args["session_key"]
    username = args["username"]
    password = args["password"]
    data = await login(username, password)
    _sessions[session_key] = {**data, "login_time": time.time()}
    result = {
        "session_key": session_key,
        "token_preview": data["token"][:20] + "...",
        "plants": [{"id": p["id"], "name": p["name"], "capacity": p.get("capacitor")} for p in data["plants"]],
    }
    return [TextContent(type="text", text=json.dumps(result, indent=2, ensure_ascii=False))]


async def tool_realtime(args: dict) -> list[TextContent]:
    session = get_session(args["session_key"])
    sid = args["station_id"]
    token = session["token"]
    async with httpx.AsyncClient() as client:
        data_resp = await api_post(client, "/pvm-data/api/0/station/data/count_station_real_data", {"sid": sid}, token=token)
    if data_resp.get("status") != "0":
        return [TextContent(type="text", text=json.dumps({"error": data_resp}, indent=2))]
    d = data_resp["data"]
    result = {
        "station_id": sid,
        "data_time": d.get("data_time"),
        "real_power_w": float(d.get("real_power", 0)),
        "today_kwh": float(d.get("today_eq", 0)) / 1000,
        "month_kwh": float(d.get("month_eq", 0)) / 1000,
        "year_kwh": float(d.get("year_eq", 0)) / 1000,
        "lifetime_kwh": float(d.get("total_eq", 0)) / 1000,
        "capacity_kw": d.get("capacitor"),
        "battery_capacity_kwh": d.get("bms_capacitor"),
        "co2_reduction_kg": float(d.get("co2_emission_reduction", 0)) / 1000,
        "trees_equivalent": d.get("plant_tree"),
    }
    if "reflux_station_data" in d:
        r = d["reflux_station_data"]
        result["battery"] = {
            "soc_percent": int(r.get("bms_soc", 0)),
            "power_w": float(r.get("bms_power", 0)),
            "today_in_kwh": float(r.get("bms_in_eq", 0)) / 1000,
            "today_out_kwh": float(r.get("bms_out_eq", 0)) / 1000,
        }
        result["flows"] = {
            "solar_to_loads_kwh": float(r.get("pv_to_load_eq", 0)) / 1000,
            "solar_to_battery_kwh": float(r.get("meter_b_in_eq", 0)) / 1000,
            "solar_to_grid_kwh": float(r.get("meter_b_out_eq", 0)) / 1000,
            "battery_to_loads_kwh": float(r.get("bms_out_eq", 0)) / 1000,
            "grid_to_loads_kwh": float(r.get("grid_in_eq", {}).get("today_eq", 0)) / 1000 if isinstance(r.get("grid_in_eq"), dict) else 0,
        }
        result["work_mode"] = r.get("work_mode")
    return [TextContent(type="text", text=json.dumps(result, indent=2, ensure_ascii=False))]


async def tool_station_info(args: dict) -> list[TextContent]:
    session = get_session(args["session_key"])
    sid = args["station_id"]
    token = session["token"]
    async with httpx.AsyncClient() as client:
        resp = await api_post(client, "/pvm/api/0/station/find", {"id": sid}, token=token)
    if resp.get("status") != "0":
        return [TextContent(type="text", text=json.dumps({"error": resp}, indent=2))]
    d = resp["data"]
    result = {
        "id": d["id"],
        "name": d["name"],
        "capacity_kw": d.get("capacitor"),
        "battery_capacity_kwh": d.get("bms_capacitor"),
        "type": d.get("classify"),
        "status": d.get("status"),
        "created": d.get("create_at"),
        "installer": d.get("create_by_name"),
        "timezone": d.get("timezone", {}).get("dis_name"),
        "money_unit": d.get("money_unit"),
    }
    if d.get("owner_list"):
        result["owner"] = d["owner_list"][0].get("name")
    if d.get("group"):
        result["installer_group"] = d["group"].get("name")
    return [TextContent(type="text", text=json.dumps(result, indent=2, ensure_ascii=False))]


async def tool_flow_data(args: dict) -> list[TextContent]:
    session = get_session(args["session_key"])
    sid = args["station_id"]
    date = args.get("date", time.strftime("%Y-%m-%d"))
    token = session["token"]
    async with httpx.AsyncClient() as client:
        resp = await api_post(client, "/pvm-data/api/0/station/data_fd/stat_g_a", {"sid": sid, "mode": 1, "date": date, "type": 6}, token=token)
    if resp.get("status") != "0":
        return [TextContent(type="text", text=json.dumps({"error": resp}, indent=2))]
    d = resp["data"]
    result = {
        "date": date,
        "station_id": sid,
        "solar_to_battery_wh": d.get("p2b", 0),
        "solar_to_grid_wh": d.get("p2g", 0),
        "solar_to_loads_wh": d.get("p2l", 0),
        "loads_from_solar_wh": d.get("lfp", 0),
        "loads_from_battery_wh": d.get("lfb", 0),
        "loads_from_grid_wh": d.get("lfg", 0),
        "last_data_time": d.get("last_data_time"),
    }
    return [TextContent(type="text", text=json.dumps(result, indent=2, ensure_ascii=False))]


async def tool_power_day(args: dict) -> list[TextContent]:
    session = get_session(args["session_key"])
    sid = args["station_id"]
    date = args.get("date", time.strftime("%Y-%m-%d"))
    token = session["token"]
    async with httpx.AsyncClient() as client:
        resp = await api_post(client, "/pvm-data/api/0/station/data3/count_power_by_day", {"date": date, "sid": sid, "pb_ver": 1, "tpp": 1}, token=token)
    if resp.get("status") != "0":
        return [TextContent(type="text", text=json.dumps({"error": resp}, indent=2))]
    d = resp.get("data", {})
    result = {"date": date, "station_id": sid, "data_points": d.get("list", [])}
    return [TextContent(type="text", text=json.dumps(result, indent=2, ensure_ascii=False))]


async def tool_set_mode(args: dict) -> list[TextContent]:
    session = get_session(args["session_key"])
    sid = args["station_id"]
    mode_name = args["mode"]
    reserve_soc = args.get("reserve_soc", 10)
    token = session["token"]
    mode_code = BATTERY_MODES.get(mode_name)
    if mode_code is None:
        return [TextContent(type="text", text=json.dumps({"error": f"Unknown mode: {mode_name}. Valid: {list(BATTERY_MODES.keys())}"}, indent=2))]
    body = {"action": 1013, "data": {"sid": sid, "data": {"mode": mode_code, "data": {"reserve_soc": reserve_soc}}}}
    async with httpx.AsyncClient() as client:
        resp = await api_post(client, "/pvm-ctl/api/0/dev/setting/write", body, token=token)
    result = {"station_id": sid, "mode": mode_name, "mode_code": mode_code, "reserve_soc": reserve_soc, "api_response": resp}
    return [TextContent(type="text", text=json.dumps(result, indent=2, ensure_ascii=False))]


async def tool_list_plants(args: dict) -> list[TextContent]:
    session = get_session(args["session_key"])
    plants = [{"id": p["id"], "name": p["name"], "capacity_kw": p.get("capacitor"), "status": p.get("status")} for p in session["plants"]]
    return [TextContent(type="text", text=json.dumps({"plants": plants}, indent=2, ensure_ascii=False))]


def main():
    asyncio.run(stdio_server(app))


if __name__ == "__main__":
    main()
