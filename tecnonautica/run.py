#!/usr/bin/env python3
"""
Tecnonautica RS485 Gateway for Home Assistant
Protocol: TN-Bus V1.14 (RS485, 19200 baud, 8N1)
"""

import serial
import threading
import queue
import time
import json
import os
import re
import warnings
from dataclasses import dataclass, field
from typing import Dict, List, Optional

# Suppress paho-mqtt v2 deprecation warning BEFORE importing
warnings.filterwarnings("ignore", category=DeprecationWarning, module="paho.mqtt.client")

import paho.mqtt.client as mqtt


# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

CONFIG_PATH = "/data/options.json"
BOARDS_PATH = "/data/boards.json"

with open(CONFIG_PATH) as f:
    _cfg = json.load(f)

PORT        = _cfg.get("serial_port", "/dev/ttyUSB0")
BAUD        = 19200
MQTT_HOST   = _cfg.get("mqtt_host", "core-mosquitto")
MQTT_PORT   = _cfg.get("mqtt_port", 1883)
MQTT_USER   = _cfg.get("mqtt_user", "")
MQTT_PASS   = _cfg.get("mqtt_pass", "")
BOARD_NAMES = _cfg.get("board_names", {})

SCAN_TOPIC  = "tecnonautica/scan"


# ═══════════════════════════════════════════════════════════════════════════════
# PROTOCOL CONSTANTS (TN-Bus V1.14)
# ═══════════════════════════════════════════════════════════════════════════════

class MsgType:
    PING    = "P"
    QUERY   = "Q"
    COMMAND = "S"
    RESET   = "R"
    ANSWER  = "A"
    CONFIRM = "C"
    RCONF   = "Z"
    ERROR   = "E"

class MachineType:
    T2 = "T2"
    T1 = "T1"
    PM = "PM"
    AL = "AL"
    SP = "SP"
    SL = "SL"

MIN_INTERFRAME_MS = 180
BURST_INTERVAL_MS = 500

QUERIES = {
    MachineType.T2: ["ST", "FB", "CO"],
    MachineType.T1: ["ST", "FB", "CO"],
    MachineType.PM: ["ME", "ST", "FB", "CO"],
    MachineType.AL: ["AS", "LS"],
    MachineType.SP: ["ST"],
    MachineType.SL: ["LS", "KB"],
}


# ═══════════════════════════════════════════════════════════════════════════════
# DATA MODELS
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class BoardInfo:
    machine:    str
    address:    str
    channels:   int
    btype:      str
    model:      str
    feedback:   int = 0
    sensors:    int = 0
    lights:     int = 0
    channel_modes: List[str] = field(default_factory=list)
    _poll_index: int = field(default=0, repr=False)

    def board_id(self) -> str:
        return f"{self.machine}_{self.address}"

    def display_name(self) -> str:
        return BOARD_NAMES.get(self.board_id(), self.model)

    def next_query(self) -> Optional[str]:
        services = QUERIES.get(self.machine, [])
        if not services:
            return None
        svc = services[self._poll_index % len(services)]
        self._poll_index += 1
        return svc


MACHINE_META = {
    MachineType.T2: {"model": "TN218", "channels": 6,  "btype": "switch",  "feedback": 6},
    MachineType.T1: {"model": "TN222", "channels": 10, "btype": "switch",  "feedback": 10},
    MachineType.PM: {"model": "TN267", "channels": 6,  "btype": "hybrid",  "feedback": 6, "sensors": 2},
    MachineType.AL: {"model": "TN234", "channels": 16, "btype": "alarm",   "feedback": 4, "lights": 2},
    MachineType.SP: {"model": "TN223", "channels": 10, "btype": "status",  "feedback": 0},
    MachineType.SL: {"model": "TN224", "channels": 6,  "btype": "light",   "feedback": 6},
}


# ═══════════════════════════════════════════════════════════════════════════════
# CHECKSUM & FRAME BUILDING
# ═══════════════════════════════════════════════════════════════════════════════

def checksum(data: str) -> str:
    cs = 0
    for c in data:
        cs ^= ord(c)
    return f"{cs:02X}"

def build_frame(msg_type: str, machine: str, addr: str, data: str) -> str:
    body = f"{msg_type}{machine}{addr}{data}KK"
    return f"[{body}*{checksum(body)}]"

def parse_frame(raw: str) -> Optional[dict]:
    if not raw.startswith("[") or not raw.endswith("]"):
        return None
    inner = raw[1:-1]
    if "*" not in inner:
        return None
    body, cs_recv = inner.rsplit("*", 1)
    if not body.endswith("KK"):
        return None
    body_no_kk = body[:-2]
    cs_calc = checksum(body)
    if cs_calc != cs_recv:
        return None
    if len(body_no_kk) < 5:
        return None
    return {
        "raw": raw, "type": body_no_kk[0], "machine": body_no_kk[1:3],
        "addr": body_no_kk[3:5], "data": body_no_kk[5:], "valid": True
    }


# ═══════════════════════════════════════════════════════════════════════════════
# SERIAL I/O
# ═══════════════════════════════════════════════════════════════════════════════

class SerialMaster:
    def __init__(self, port: str, baud: int):
        self.port = port
        self.baud = baud
        self.ser: Optional[serial.Serial] = None
        self.lock = threading.Lock()
        self._connect()

    def _connect(self):
        while True:
            try:
                self.ser = serial.Serial(
                    self.port, self.baud,
                    timeout=0.3,
                    write_timeout=1.0
                )
                self.ser.reset_input_buffer()
                print(f"Serial connected: {self.port} @ {self.baud}", flush=True)
                return
            except serial.SerialException as e:
                print(f"Serial error: {e}, retrying in 2s...", flush=True)
                time.sleep(2)

    def _ensure_connected(self):
        if self.ser is None or not self.ser.is_open:
            self._connect()

    def send_and_receive(self, frame: str, timeout: float = 0.5) -> Optional[dict]:
        with self.lock:
            self._ensure_connected()
            try:
                self.ser.reset_input_buffer()
                self.ser.write(frame.encode('ascii'))
                print(f"TX: {frame}", flush=True)
            except serial.SerialException as e:
                print(f"TX failed: {e}, reconnecting...", flush=True)
                self.ser.close()
                self.ser = None
                return None

            buffer = ""
            deadline = time.time() + timeout
            while time.time() < deadline:
                try:
                    chunk = self.ser.read(64)
                    if chunk:
                        buffer += chunk.decode('ascii', errors='replace')
                        while True:
                            start = buffer.find('[')
                            if start == -1:
                                buffer = ""
                                break
                            end = buffer.find(']', start)
                            if end == -1:
                                buffer = buffer[start:]
                                break
                            candidate = buffer[start:end+1]
                            buffer = buffer[end+1:]
                            parsed = parse_frame(candidate)
                            if parsed:
                                if parsed["type"] in (MsgType.ANSWER, MsgType.CONFIRM, MsgType.RCONF, MsgType.ERROR):
                                    print(f"RX: {candidate}", flush=True)
                                    return parsed
                                else:
                                    print(f"  (echo: {candidate})", flush=True)
                except serial.SerialException:
                    self.ser.close()
                    self.ser = None
                    return None
                time.sleep(0.01)
            return None

    def close(self):
        if self.ser and self.ser.is_open:
            self.ser.close()


# ═══════════════════════════════════════════════════════════════════════════════
# MQTT PUBLISHERS
# ═══════════════════════════════════════════════════════════════════════════════

class MqttPublisher:
    def __init__(self, client: mqtt.Client):
        self.client = client

    def _topic(self, *parts: str) -> str:
        return "/".join(["tecnonautica"] + list(parts))

    def switch(self, board_id: str, ch: int, state: bool):
        self.client.publish(self._topic(board_id, f"canale{ch}", "state"),
                           "ON" if state else "OFF", retain=True)

    def light(self, board_id: str, ch: int, state: bool):
        self.client.publish(self._topic(board_id, f"luce{ch}", "state"),
                           "ON" if state else "OFF", retain=True)

    def relay(self, board_id: str, num: int, state: bool):
        self.client.publish(self._topic(board_id, f"rele{num}", "state"),
                           "ON" if state else "OFF", retain=True)

    def feedback(self, board_id: str, num: int, state: bool):
        self.client.publish(self._topic(board_id, f"fb{num}", "state"),
                           "ON" if state else "OFF", retain=True)

    def alarm(self, board_id: str, ch: int, raw: str):
        state = "ON" if raw in ("A", "C") else "OFF"
        self.client.publish(self._topic(board_id, f"allarme{ch}", "state"), state, retain=True)
        self.client.publish(self._topic(board_id, f"allarme{ch}", "raw"), raw, retain=True)

    def sensor(self, board_id: str, num: int, value: str):
        self.client.publish(self._topic(board_id, f"sensore{num}", "state"), value, retain=True)

    def spia(self, board_id: str, ch: int, state: bool):
        self.client.publish(self._topic(board_id, f"spia{ch}", "state"),
                           "ON" if state else "OFF", retain=True)

    def anchor(self, board_id: str, _num: int, state: bool):
        self.client.publish(self._topic(board_id, "anchor", "state"),
                           "ON" if state else "OFF", retain=True)

    def navlights(self, board_id: str, _num: int, state: bool):
        self.client.publish(self._topic(board_id, "navlights", "state"),
                           "ON" if state else "OFF", retain=True)

    def scan_result(self, count: int):
        self.client.publish(self._topic("scan", "result"),
                           f"Trovate {count} schede", retain=False)


# ═══════════════════════════════════════════════════════════════════════════════
# HOME ASSISTANT DISCOVERY
# ═══════════════════════════════════════════════════════════════════════════════

class Discovery:
    def __init__(self, client: mqtt.Client):
        self.client = client

    def _device(self, info: BoardInfo) -> dict:
        return {
            "identifiers": [f"tecnonautica_{info.board_id()}"],
            "name": info.display_name(),
            "model": info.model,
            "manufacturer": "Tecnonautica"
        }

    def _publish(self, component: str, uid: str, payload: dict):
        topic = f"homeassistant/{component}/{uid}/config"
        self.client.publish(topic, json.dumps(payload), retain=True)

    def switch(self, board_id: str, info: BoardInfo, ch: int, name_suffix: str = "Canale"):
        uid = f"tecnonautica_{board_id}_ch{ch}"
        self._publish("switch", uid, {
            "name": f"{info.display_name()} {name_suffix} {ch}",
            "unique_id": uid,
            "state_topic":   f"tecnonautica/{board_id}/canale{ch}/state",
            "command_topic": f"tecnonautica/{board_id}/canale{ch}/set",
            "payload_on": "ON", "payload_off": "OFF",
            "retain": True, "optimistic": False,
            "device": self._device(info)
        })

    def light_entity(self, board_id: str, info: BoardInfo, ch: int):
        uid = f"tecnonautica_{board_id}_luce{ch}"
        self._publish("light", uid, {
            "name": f"{info.display_name()} Luce {ch}",
            "unique_id": uid,
            "state_topic":   f"tecnonautica/{board_id}/luce{ch}/state",
            "command_topic": f"tecnonautica/{board_id}/luce{ch}/set",
            "payload_on": "ON", "payload_off": "OFF",
            "retain": True, "optimistic": False,
            "device": self._device(info)
        })

    def relay(self, board_id: str, info: BoardInfo, num: int):
        uid = f"tecnonautica_{board_id}_relay{num}"
        self._publish("switch", uid, {
            "name": f"{info.display_name()} Relè {num}",
            "unique_id": uid,
            "state_topic":   f"tecnonautica/{board_id}/rele{num}/state",
            "command_topic": f"tecnonautica/{board_id}/rele{num}/set",
            "payload_on": "ON", "payload_off": "OFF",
            "retain": True, "optimistic": False,
            "device": self._device(info)
        })

    def sensor(self, board_id: str, info: BoardInfo, num: int):
        uid = f"tecnonautica_{board_id}_sensore{num}"
        self._publish("sensor", uid, {
            "name": f"{info.display_name()} Sensore {num}",
            "unique_id": uid,
            "state_topic": f"tecnonautica/{board_id}/sensore{num}/state",
            "device": self._device(info)
        })

    def binary_sensor(self, board_id: str, info: BoardInfo, num: int,
                      name: str, suffix: str, dev_class: str = None):
        uid = f"tecnonautica_{board_id}_{suffix}{num}"
        payload = {
            "name": f"{info.display_name()} {name} {num}",
            "unique_id": uid,
            "state_topic": f"tecnonautica/{board_id}/{suffix}{num}/state",
            "payload_on": "ON", "payload_off": "OFF",
            "device": self._device(info)
        }
        if dev_class:
            payload["device_class"] = dev_class
        self._publish("binary_sensor", uid, payload)

    def alarm_sensor(self, board_id: str, info: BoardInfo, ch: int):
        uid = f"tecnonautica_{board_id}_allarme{ch}"
        self._publish("binary_sensor", uid, {
            "name": f"{info.display_name()} Allarme {ch}",
            "unique_id": uid,
            "state_topic": f"tecnonautica/{board_id}/allarme{ch}/state",
            "payload_on": "ON", "payload_off": "OFF",
            "device_class": "safety",
            "device": self._device(info)
        })

    def button(self, board_id: str, info: BoardInfo, uid_suffix: str,
               name: str, cmd_payload: str):
        uid = f"tecnonautica_{board_id}_{uid_suffix}"
        self._publish("button", uid, {
            "name": f"{info.display_name()} {name}",
            "unique_id": uid,
            "command_topic": f"tecnonautica/{board_id}/cmd",
            "payload_press": cmd_payload,
            "device": self._device(info)
        })

    def scan_button(self):
        uid = "tecnonautica_scan_button"
        self._publish("button", uid, {
            "name": "Scansiona Bus",
            "unique_id": uid,
            "command_topic": SCAN_TOPIC,
            "payload_press": "SCAN",
            "device": {
                "identifiers": ["tecnonautica_gateway"],
                "name": "Tecnonautica Gateway",
                "model": "RS485 Bridge",
                "manufacturer": "Tecnonautica"
            }
        })

    def setup_board(self, board_id: str, info: BoardInfo):
        btype = info.btype
        if btype == "switch":
            for ch in range(1, info.channels + 1):
                self.switch(board_id, info, ch)
            for fb in range(1, info.feedback + 1):
                self.binary_sensor(board_id, info, fb, "Stato", "fb", "power")
        elif btype == "light":
            for ch in range(1, info.channels + 1):
                self.light_entity(board_id, info, ch)
            for fb in range(1, info.feedback + 1):
                self.binary_sensor(board_id, info, fb, "Stato", "fb", "power")
        elif btype == "hybrid":
            for s in range(1, info.sensors + 1):
                self.sensor(board_id, info, s)
            for r in range(1, info.channels + 1):
                self.relay(board_id, info, r)
            for fb in range(1, info.feedback + 1):
                self.binary_sensor(board_id, info, fb, "Stato", "fb", "power")
        elif btype == "status":
            for ch in range(1, info.channels + 1):
                self.binary_sensor(board_id, info, ch, "Spia", "spia", "power")
        elif btype == "alarm":
            for ch in range(1, info.channels + 1):
                self.alarm_sensor(board_id, info, ch)
            if info.lights >= 1:
                uid_a = f"tecnonautica_{board_id}_anchor"
                self._publish("switch", uid_a, {
                    "name": f"{info.display_name()} Ancora",
                    "unique_id": uid_a,
                    "state_topic":   f"tecnonautica/{board_id}/anchor/state",
                    "command_topic": f"tecnonautica/{board_id}/anchor/set",
                    "payload_on": "ON", "payload_off": "OFF",
                    "retain": True, "device": self._device(info)
                })
            if info.lights >= 2:
                uid_n = f"tecnonautica_{board_id}_navlights"
                self._publish("switch", uid_n, {
                    "name": f"{info.display_name()} Luci Navigazione",
                    "unique_id": uid_n,
                    "state_topic":   f"tecnonautica/{board_id}/navlights/state",
                    "command_topic": f"tecnonautica/{board_id}/navlights/set",
                    "payload_on": "ON", "payload_off": "OFF",
                    "retain": True, "device": self._device(info)
                })
            for fb in range(1, info.feedback + 1):
                self.binary_sensor(board_id, info, fb, "Stato", "fb", "power")
            self.button(board_id, info, "clear", "Azzera Allarmi", "CL")
            self.button(board_id, info, "smoke", "Reset Fumo", "SM")
        def subscribe_commands(self, client: mqtt.Client, board_id: str, info: BoardInfo):
            """Subscribe to MQTT command topics for a board."""
            btype = info.btype
            if btype == "switch":
                for ch in range(1, info.channels + 1):
                    client.subscribe(f"tecnonautica/{board_id}/canale{ch}/set")
            elif btype == "light":
                for ch in range(1, info.channels + 1):
                    client.subscribe(f"tecnonautica/{board_id}/luce{ch}/set")
            elif btype == "hybrid":
                for r in range(1, info.channels + 1):
                    client.subscribe(f"tecnonautica/{board_id}/rele{r}/set")
            elif btype == "alarm":
                if info.lights >= 1:
                    client.subscribe(f"tecnonautica/{board_id}/anchor/set")
                if info.lights >= 2:
                    client.subscribe(f"tecnonautica/{board_id}/navlights/set")
                client.subscribe(f"tecnonautica/{board_id}/cmd")
            # SP (status) has no commands


# ═══════════════════════════════════════════════════════════════════════════════
# STATE MANAGER
# ═══════════════════════════════════════════════════════════════════════════════

class StateManager:
    def __init__(self, pub: MqttPublisher):
        self.pub = pub
        self.states: Dict[str, any] = {}
        self.lock = threading.Lock()

    def _key(self, board_id: str, entity: str, num: int) -> str:
        return f"{board_id}_{entity}_{num}"

    def update(self, board_id: str, entity: str, num: int, value, force: bool = False):
        key = self._key(board_id, entity, num)
        with self.lock:
            if not force and self.states.get(key) == value:
                return False
            self.states[key] = value
        method = getattr(self.pub, entity, None)
        if method:
            method(board_id, num, value)
        else:
            print(f"  Unknown entity type: {entity}", flush=True)
        return True

    def get(self, board_id: str, entity: str, num: int) -> any:
        return self.states.get(self._key(board_id, entity, num))


# ═══════════════════════════════════════════════════════════════════════════════
# RESPONSE PARSERS
# ═══════════════════════════════════════════════════════════════════════════════

class ResponseParser:
    def __init__(self, states: StateManager, boards: Dict[str, BoardInfo]):
        self.states = states
        self.boards = boards

    def parse(self, frame: dict) -> bool:
        mm = frame["machine"]
        addr = frame["addr"]
        data = frame["data"]
        board_id = f"{mm}_{addr}"
        info = self.boards.get(board_id)
        if not info:
            return False
        btype = info.btype

        if frame["type"] == MsgType.ANSWER:
            if "ST" in data and btype in ("switch", "light", "hybrid", "status"):
                idx = data.find("ST") + 2
                bits = ""
                for c in data[idx:]:
                    if c in "01":
                        bits += c
                    else:
                        break
                if btype == "hybrid" and len(bits) >= 6:
                    for i in range(6):
                        self.states.update(board_id, "relay", i+1, bits[i] == "1")
                elif btype == "status" and len(bits) >= info.channels:
                    for i in range(info.channels):
                        self.states.update(board_id, "spia", i+1, bits[i] == "1")
                elif len(bits) >= info.channels:
                    for i in range(info.channels):
                        self.states.update(board_id, "switch" if btype == "switch" else "light",
                                          i+1, bits[i] == "1")
                return True

            if "FB" in data and btype in ("switch", "light", "hybrid", "alarm"):
                idx = data.find("FB") + 2
                bits = ""
                for c in data[idx:]:
                    if c in "01":
                        bits += c
                    else:
                        break
                fb_count = info.feedback
                if fb_count > 0 and len(bits) >= fb_count:
                    for i in range(fb_count):
                        self.states.update(board_id, "feedback", i+1, bits[i] == "1")
                return True

            if "AS" in data and btype == "alarm":
                idx = data.find("AS") + 2
                stati = data[idx:idx+info.channels]
                for i, s in enumerate(stati):
                    if s in ("D", "A", "C", "R", "N"):
                        self.states.update(board_id, "alarm", i+1, s)
                return True

            if "LS" in data:
                idx = data.find("LS") + 2
                if btype == "alarm" and len(data) >= idx + 2:
                    self.states.update(board_id, "anchor", 1, data[idx] == "1")
                    self.states.update(board_id, "navlights", 1, data[idx+1] == "1")
                elif btype == "light" and len(data) >= idx + info.channels:
                    for i in range(info.channels):
                        self.states.update(board_id, "light", i+1, data[idx+i] == "1")
                return True

            if "ME" in data and btype == "hybrid":
                match = re.search(r'A([+-]\d{5})B([+-]\d{5})', data)
                if match:
                    v1, v2 = match.group(1), match.group(2)
                    if v1.replace('+', '').replace('-', '').strip('0'):
                        self.states.update(board_id, "sensor", 1, v1)
                    if v2.replace('+', '').replace('-', '').strip('0'):
                        self.states.update(board_id, "sensor", 2, v2)
                return True

            if "CO" in data and btype in ("switch", "light", "hybrid"):
                idx = data.find("CO") + 2
                modes = ""
                for c in data[idx:]:
                    if c in "BT":
                        modes += c
                    else:
                        break
                if len(modes) == info.channels:
                    info.channel_modes = list(modes)
                    print(f"  {board_id} modes = {modes}", flush=True)
                return True

        if frame["type"] == MsgType.CONFIRM:
            print(f"  Confirm: {data}", flush=True)
            return True

        if frame["type"] == MsgType.ERROR:
            print(f"  ERROR from {board_id}: {data}", flush=True)
            return True

        return False


# ═══════════════════════════════════════════════════════════════════════════════
# BURST MANAGER
# ═══════════════════════════════════════════════════════════════════════════════

class BurstManager:
    def __init__(self, controller: 'BusController'):
        self.controller = controller
        self.active: Dict[str, threading.Event] = {}
        self.lock = threading.Lock()

    def start(self, board_id: str, ch: int, info: BoardInfo):
        key = f"{board_id}_{ch}"
        with self.lock:
            if key in self.active:
                return
            stop_event = threading.Event()
            self.active[key] = stop_event

        stay_frame_data = f"S{ch}"

        def burst_loop():
            print(f"Burst START {board_id}/ch{ch}", flush=True)
            while not stop_event.is_set():
                self.controller.enqueue_command(board_id, stay_frame_data, [])
                stop_event.wait(BURST_INTERVAL_MS / 1000)
            self.controller.enqueue_command(board_id, f"R{ch}", ["ST", "FB"])
            print(f"Burst STOP {board_id}/ch{ch}", flush=True)
            with self.lock:
                self.active.pop(key, None)

        thread = threading.Thread(target=burst_loop, daemon=True)
        thread.start()

    def stop(self, board_id: str, ch: int):
        key = f"{board_id}_{ch}"
        with self.lock:
            evt = self.active.get(key)
            if evt:
                evt.set()


# ═══════════════════════════════════════════════════════════════════════════════
# BUS CONTROLLER
# ═══════════════════════════════════════════════════════════════════════════════

class BusController:
    def __init__(self, master: SerialMaster, boards: Dict[str, BoardInfo],
                 parser: ResponseParser, states: StateManager):
        self.master = master
        self.boards = boards
        self.parser = parser
        self.states = states
        self.cmd_queue = queue.Queue()
        self.running = True
        self.scanning = False
        self.last_cmd_time = 0
        self.last_ping_time = 0
        self.burst = BurstManager(self)

    def _send(self, frame: str, timeout: float = 0.5) -> Optional[dict]:
        return self.master.send_and_receive(frame, timeout)

    def _query_board(self, board_id: str, info: BoardInfo, service: str) -> Optional[dict]:
        frame = build_frame(MsgType.QUERY, info.machine, info.address, service)
        return self._send(frame)

    def _command_board(self, board_id: str, info: BoardInfo, data: str) -> bool:
        frame = build_frame(MsgType.COMMAND, info.machine, info.address, data)
        resp = self._send(frame, timeout=0.8)
        if resp and resp["type"] in (MsgType.CONFIRM, MsgType.ANSWER):
            self.last_cmd_time = time.time()
            return True
        return False

    def run(self):
        while self.running:
            # ── HIGH PRIORITY: Commands from HA ──
            try:
                cmd = self.cmd_queue.get(timeout=0.02)
                board_id, info, data, post_queries = cmd
                if not self.scanning:
                    print(f"CMD: {board_id} -> {data}", flush=True)
                    if self._command_board(board_id, info, data):
                        for svc in post_queries:
                            resp = self._query_board(board_id, info, svc)
                            if resp:
                                self.parser.parse(resp)
                            time.sleep(MIN_INTERFRAME_MS / 1000)
                self.cmd_queue.task_done()
                continue
            except queue.Empty:
                pass

            if self.scanning:
                time.sleep(0.05)
                continue

            # ── PING every 30 seconds ──
            now = time.time()
            if now - self.last_ping_time >= 30:
                ping_n = int(now * 10) % 100
                ping_frame = build_frame(MsgType.PING, "", "", f"{ping_n:02d}")
                self._send(ping_frame)
                self.last_ping_time = now
                time.sleep(MIN_INTERFRAME_MS / 1000)
                continue

            # ── FAST POLLING: one query per board per cycle ──
            for board_id, info in list(self.boards.items()):
                if not self.cmd_queue.empty():
                    break
                svc = info.next_query()
                if svc:
                    resp = self._query_board(board_id, info, svc)
                    if resp:
                        self.parser.parse(resp)
                time.sleep(MIN_INTERFRAME_MS / 1000)

            time.sleep(0.05)

    def stop(self):
        self.running = False

    def enqueue_command(self, board_id: str, data: str,
                        post_queries: List[str] = None):
        info = self.boards.get(board_id)
        if not info:
            return
        self.cmd_queue.put((board_id, info, data, post_queries or []))


# ═══════════════════════════════════════════════════════════════════════════════
# MQTT COMMAND HANDLER
# ═══════════════════════════════════════════════════════════════════════════════

class MqttCommandHandler:
    def __init__(self, controller: BusController, states: StateManager,
                 boards: Dict[str, BoardInfo]):
        self.ctrl = controller
        self.states = states
        self.boards = boards

    def handle(self, topic: str, payload: str):
        if not payload:
            return
        if topic == SCAN_TOPIC and payload == "SCAN":
            return

        parts = topic.split("/")
        if len(parts) < 3:
            return

        board_id = parts[1]
        entity = parts[2]
        info = self.boards.get(board_id)
        if not info:
            return

        btype = info.btype

        if btype == "alarm":
            if entity == "anchor":
                self._toggle_light(board_id, info, "FD", "anchor", payload == "ON")
                return
            if entity == "navlights":
                self._toggle_light(board_id, info, "NA", "navlights", payload == "ON")
                return
            if entity == "cmd":
                self.ctrl.enqueue_command(board_id, payload, ["AS", "LS"])
                return

        if btype == "hybrid" and entity.startswith("rele"):
            relay_num = int(entity.replace("rele", ""))
            self._handle_relay(board_id, info, relay_num, payload)
            return

        if btype == "light" and entity.startswith("luce"):
            ch = int(entity.replace("luce", ""))
            self._handle_light(board_id, info, ch, payload)
            return

        if btype == "switch" and entity.startswith("canale"):
            ch = int(entity.replace("canale", ""))
            self._handle_switch(board_id, info, ch, payload)
            return

        print(f"  Unhandled: {topic} = {payload}", flush=True)

    def _handle_switch(self, board_id: str, info: BoardInfo, ch: int, payload: str):
        current = self.states.get(board_id, "switch", ch) or False
        want_on = payload == "ON"
        if want_on == current:
            print(f"  {board_id}/canale{ch} already {payload}", flush=True)
            return
        modes = info.channel_modes or ["T"] * info.channels
        mode = modes[ch - 1] if ch <= len(modes) else "T"
        self.states.update(board_id, "switch", ch, want_on)
        if mode == "B":
            if want_on:
                self.ctrl.burst.start(board_id, ch, info)
            else:
                self.ctrl.burst.stop(board_id, ch)
        else:
            self.ctrl.enqueue_command(board_id, f"P{ch}", ["ST", "FB"])

    def _handle_relay(self, board_id: str, info: BoardInfo, num: int, payload: str):
        current = self.states.get(board_id, "relay", num) or False
        want_on = payload == "ON"
        if want_on == current:
            print(f"  {board_id}/rele{num} already {payload}", flush=True)
            return
        modes = info.channel_modes or ["T"] * info.channels
        mode = modes[num - 1] if num <= len(modes) else "T"
        self.states.update(board_id, "relay", num, want_on)
        if mode == "B":
            if want_on:
                self.ctrl.burst.start(board_id, num, info)
            else:
                self.ctrl.burst.stop(board_id, num)
        else:
            self.ctrl.enqueue_command(board_id, f"P{num}", ["ST", "FB"])

    def _handle_light(self, board_id: str, info: BoardInfo, ch: int, payload: str):
        current = self.states.get(board_id, "light", ch) or False
        want_on = payload == "ON"
        if want_on == current:
            print(f"  {board_id}/luce{ch} already {payload}", flush=True)
            return
        self.states.update(board_id, "light", ch, want_on)
        if want_on:
            self.ctrl.enqueue_command(board_id, f"A{ch}", ["LS"])
        else:
            self.ctrl.enqueue_command(board_id, f"S{ch}", ["LS"])

    def _toggle_light(self, board_id: str, info: BoardInfo,
                      cmd: str, entity: str, want_on: bool):
        current = self.states.get(board_id, entity, 1) or False
        if want_on == current:
            pass
        self.states.update(board_id, entity, 1, want_on)
        self.ctrl.enqueue_command(board_id, cmd, ["LS"])


# ═══════════════════════════════════════════════════════════════════════════════
# BUS SCANNER
# ═══════════════════════════════════════════════════════════════════════════════

class BusScanner:
    def __init__(self, master: SerialMaster):
        self.master = master

    def scan(self) -> Dict[str, BoardInfo]:
        print("\n=== SCANSIONE BUS TECNONAUTICA ===", flush=True)
        found = {}
        for mm, meta in MACHINE_META.items():
            for addr in range(33):
                aa = f"{addr:02d}"
                frame = build_frame(MsgType.QUERY, mm, aa, "ID")
                resp = self.master.send_and_receive(frame, timeout=0.4)
                if resp and resp["type"] == MsgType.ANSWER and "ID" in resp["data"]:
                    board_id = f"{mm}_{aa}"
                    info = BoardInfo(
                        machine=mm, address=aa,
                        channels=meta["channels"],
                        btype=meta["btype"],
                        model=meta["model"],
                        feedback=meta.get("feedback", 0),
                        sensors=meta.get("sensors", 0),
                        lights=meta.get("lights", 0)
                    )
                    found[board_id] = info
                    print(f"  ✓ {meta['model']} addr={aa}", flush=True)
                time.sleep(0.05)
        if not found:
            print("  Nessuna scheda trovata!", flush=True)
        print(f"=== FINE SCANSIONE: {len(found)} schede ===\n", flush=True)
        return found


# ═══════════════════════════════════════════════════════════════════════════════
# PERSISTENCE
# ═══════════════════════════════════════════════════════════════════════════════

def save_boards(boards: Dict[str, BoardInfo]):
    data = {
        bid: {
            "machine": b.machine, "address": b.address,
            "channels": b.channels, "type": b.btype,
            "model": b.model, "feedback": b.feedback,
            "sensors": b.sensors, "lights": b.lights,
            "channel_modes": b.channel_modes
        }
        for bid, b in boards.items()
    }
    with open(BOARDS_PATH, "w") as f:
        json.dump(data, f, indent=2)
    print(f"Schede salvate: {list(boards.keys())}", flush=True)

def load_boards() -> Optional[Dict[str, BoardInfo]]:
    if not os.path.exists(BOARDS_PATH):
        return None
    with open(BOARDS_PATH) as f:
        data = json.load(f)
    boards = {}
    for bid, d in data.items():
        info = BoardInfo(
            machine=d["machine"], address=d["address"],
            channels=d["channels"], btype=d["type"],
            model=d["model"], feedback=d.get("feedback", 0),
            sensors=d.get("sensors", 0), lights=d.get("lights", 0),
            channel_modes=d.get("channel_modes", [])
        )
        boards[bid] = info
    print(f"Schede caricate: {list(boards.keys())}", flush=True)
    return boards


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN APPLICATION
# ═══════════════════════════════════════════════════════════════════════════════

class TecnonauticaGateway:
    def __init__(self):
        self.running = True
        self.boards: Dict[str, BoardInfo] = {}
        self.master: Optional[SerialMaster] = None
        self.mqtt_client: Optional[mqtt.Client] = None
        self.mqtt_ready = threading.Event()
        self.controller: Optional[BusController] = None
        self.handler: Optional[MqttCommandHandler] = None
        self.states: Optional[StateManager] = None
        self.discovery: Optional[Discovery] = None
        self.scanning = False

    def _on_mqtt_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print("MQTT connesso", flush=True)
            client.subscribe(SCAN_TOPIC)
            self.mqtt_ready.set()
        else:
            print(f"MQTT errore: {rc}", flush=True)

    def _on_mqtt_disconnect(self, client, userdata, rc):
        self.mqtt_ready.clear()
        if rc != 0:
            print(f"MQTT disconnesso: {rc}", flush=True)

    def _on_mqtt_message(self, client, userdata, msg):
        try:
            topic = msg.topic
            payload = msg.payload.decode()
            if not payload:
                return
            print(f"MQTT RX: {topic} = {payload}", flush=True)
            if topic == SCAN_TOPIC and payload == "SCAN":
                threading.Thread(target=self._do_scan, daemon=True).start()
                return
            if self.handler:
                self.handler.handle(topic, payload)
        except Exception as e:
            print(f"Errore MQTT message: {e}", flush=True)

    def _setup_mqtt(self):
        try:
            self.mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
        except (AttributeError, TypeError):
            self.mqtt_client = mqtt.Client()
        if MQTT_USER:
            self.mqtt_client.username_pw_set(MQTT_USER, MQTT_PASS)
        self.mqtt_client.on_connect = self._on_mqtt_connect
        self.mqtt_client.on_disconnect = self._on_mqtt_disconnect
        self.mqtt_client.on_message = self._on_mqtt_message
        print(f"Connessione MQTT {MQTT_HOST}:{MQTT_PORT}...", flush=True)
        self.mqtt_client.connect(MQTT_HOST, MQTT_PORT, 60)
        self.mqtt_client.loop_start()

    def _setup_boards(self, boards: Dict[str, BoardInfo]):
        self.boards = boards
        for bid, info in boards.items():
            print(f"Setup {bid} ({info.btype})...", flush=True)
            self.discovery.setup_board(bid, info)
            self.discovery.subscribe_commands(self.mqtt_client, bid, info)
            if info.btype == "hybrid" and not info.channel_modes:
                info.channel_modes = ["T"] * info.channels

    def _do_scan(self):
        if self.scanning:
            return
        self.scanning = True
        print("Avvio scansione...", flush=True)
        scanner = BusScanner(self.master)
        found = scanner.scan()
        if not found:
            found["T2_00"] = BoardInfo(
                machine="T2", address="00", channels=6,
                btype="switch", model="TN218", feedback=6
            )
        self.boards = found
        save_boards(found)
        self._setup_boards(found)
        if self.controller:
            self.controller.boards = found
        self.scanning = False
        if self.states:
            self.states.pub.scan_result(len(found))

    def run(self):
        self.master = SerialMaster(PORT, BAUD)
        self._setup_mqtt()
        self.mqtt_ready.wait(timeout=30)

        pub = MqttPublisher(self.mqtt_client)
        self.states = StateManager(pub)
        self.discovery = Discovery(self.mqtt_client)
        self.discovery.scan_button()

        boards = load_boards()
        if boards:
            self._setup_boards(boards)
        else:
            self._do_scan()

        parser = ResponseParser(self.states, self.boards)
        self.controller = BusController(self.master, self.boards, parser, self.states)
        self.handler = MqttCommandHandler(self.controller, self.states, self.boards)

        bus_thread = threading.Thread(target=self.controller.run, daemon=True)
        bus_thread.start()

        print("Gateway avviato!", flush=True)

        try:
            while self.running:
                time.sleep(1)
                if not self.mqtt_ready.is_set():
                    print("MQTT non connesso, in attesa...", flush=True)
        except KeyboardInterrupt:
            print("Shutdown...", flush=True)
        finally:
            self.running = False
            if self.controller:
                self.controller.stop()
            if self.mqtt_client:
                self.mqtt_client.loop_stop()
                self.mqtt_client.disconnect()
            if self.master:
                self.master.close()


# ═══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    gateway = TecnonauticaGateway()
    gateway.run()
