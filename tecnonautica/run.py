#!/usr/bin/env python3
"""Tecnonautica TN-Bus to MQTT Bridge - Home Assistant Add-on / Windows Test"""
import serial
import threading
import queue
import time
import paho.mqtt.client as mqtt
import json
import os
import logging

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURAZIONE (compatibile HA e Windows)
# ─────────────────────────────────────────────────────────────────────────────
OPTIONS_PATH = "/data/options.json" if os.path.exists("/data/options.json") else "options.json"
try:
    with open(OPTIONS_PATH) as f:
        cfg = json.load(f)
except Exception as e:
    print(f"⚠ Impossibile leggere {OPTIONS_PATH}: {e}. Uso default.", flush=True)
    cfg = {}

PORT      = cfg.get("serial_port", "COM3" if os.name == "nt" else "/dev/ttyUSB0")
BAUD      = 19200
MQTT_HOST = cfg.get("mqtt_host", "127.0.0.1" if os.name == "nt" else "core-mosquitto")
MQTT_PORT = cfg.get("mqtt_port", 1883)
MQTT_USER = cfg.get("mqtt_user", "")
MQTT_PASS = cfg.get("mqtt_pass", "")
BOARD_NAMES = cfg.get("board_names", {})
BOARDS_FILE = "/data/boards.json" if os.path.exists("/data") else "boards.json"
SCAN_TOPIC  = "tecnonautica/scan"

MACHINE_TYPES = {
    "T2": {"name": "TN218",   "channels": 6,  "type": "switch",   "feedback": 6},
    "T1": {"name": "TN222",   "channels": 10, "type": "switch",   "feedback": 10},
    "PM": {"name": "TN267",   "channels": 6,  "type": "hybrid",   "feedback": 6},
    "AL": {"name": "TN234",   "channels": 16, "type": "alarm",    "feedback": 4,  "switches": 4},
    "SP": {"name": "TN223",   "channels": 10, "type": "status",   "feedback": 0},
    "SL": {"name": "TN224",   "channels": 6,  "type": "light",    "feedback": 6},
}

# ─────────────────────────────────────────────────────────────────────────────
# STATO GLOBALE & LOCK
# ─────────────────────────────────────────────────────────────────────────────
running            = True
scanning           = False
tx_queue           = queue.Queue()
mqtt_ready         = threading.Event()
last_tx_time       = 0.0
bus_lock           = threading.Lock()  # Protegge detected_boards, channel_states, burst_active
detected_boards    = {}
channel_states     = {}
burst_active       = {}
enable_commands_at = time.time() + 5.0
last_sensor_values = {}

print(f"Apertura porta {PORT} @ {BAUD}...", flush=True)
ser = serial.Serial(PORT, BAUD, timeout=0.1, write_timeout=1)
print("Porta aperta!", flush=True)

# ─────────────────────────────────────────────────────────────────────────────
# PROTOCOLLO
# ─────────────────────────────────────────────────────────────────────────────
def checksum(data):
    cs = 0
    for c in data:
        cs ^= ord(c)
    return format(cs, "02X")

def build_frame(msg_type, machine, addr, data):
    body = f"{msg_type}{machine}{addr}{data}KK"
    return f"[{body}*{checksum(body)}]"

def validate_checksum(frame: str) -> bool:
    try:
        start = frame.index('[') + 1
        sep   = frame.index('*')
        end   = frame.index(']', sep)
        payload = frame[start:sep]
        rx_cs   = frame[sep+1:end].strip().upper()
        return checksum(payload) == rx_cs
    except ValueError:
        return False

def send_direct(frame, timeout=0.8):
    """Invia frame e attendi risposta — usato solo durante scansione"""
    global last_tx_time
    ser.reset_input_buffer()
    ser.write(frame.encode('ascii'))
    last_tx_time = time.time()
    buffer, deadline = "", time.time() + timeout
    while time.time() < deadline:
        data = ser.read(64)
        if data:
            buffer += data.decode('ascii', errors='replace')
            if '[' in buffer and ']' in buffer:
                s, e = buffer.find('['), buffer.find(']', buffer.find('['))
                if e != -1: return buffer[s:e+1]
    return None

# ─────────────────────────────────────────────────────────────────────────────
# THREAD TX (Timing rigoroso >180ms)
# ─────────────────────────────────────────────────────────────────────────────
def tx_thread():
    global last_tx_time
    print("TX thread avviato", flush=True)
    while running:
        try:
            frame = tx_queue.get(timeout=0.2)
            with bus_lock:
                elapsed = time.time() - last_tx_time
                if elapsed < 0.19:  # >180ms come da protocollo par. 2.1
                    time.sleep(0.19 - elapsed)
                ser.write(frame.encode('ascii'))
                ser.flush()
                last_tx_time = time.time()
                print(f"TX: {frame}", flush=True)
                tx_queue.task_done()
        except queue.Empty:
            pass
        except Exception as e:
            print(f"TX errore: {e}", flush=True)
            time.sleep(1)

# ─────────────────────────────────────────────────────────────────────────────
# THREAD RX & PARSER ROBUSTO
# ─────────────────────────────────────────────────────────────────────────────
def rx_thread():
    print("RX thread avviato", flush=True)
    buffer = ""
    while running:
        if scanning:
            time.sleep(0.1)
            continue
        try:
            data = ser.read(128)
            if not  continue
            buffer += data.decode('ascii', errors='replace')

            while '[' in buffer and ']' in buffer:
                s, e = buffer.find('['), buffer.find(']', buffer.find('['))
                frame = buffer[s:e+1]
                buffer = buffer[e+1:]
                print(f"RX: {frame}", flush=True)
                
                if not validate_checksum(frame):
                    print("  ⚠ Checksum invalido, scarto", flush=True)
                    continue
                parse_frame(frame)
        except Exception as e:
            if running: print(f"RX errore: {e}", flush=True)
            time.sleep(0.01)

def parse_frame(msg):
    # Parsing posizionale sicuro (nessuna guardia temporale che blocca il feedback)
    inner = msg[1:msg.index('*')]
    if len(inner) < 7: return
    
    t_type = inner[0]
    mm     = inner[1:3]
    aa     = inner[3:5]
    data_part = inner[5:-2]  # Rimuove KK finale

    with bus_lock:
        board = next((b for b in detected_boards.values() 
                      if b["machine"] == mm and b["address"] == aa), None)
        if not board: return
        bid, btype = board["id"], board["type"]

    if t_type == "E":
        print(f"  ⚠ Errore slave [{bid}]: {data_part}", flush=True)
        return

    if t_type == "A":
        svc = data_part[:2]
        payload = data_part[2:]

        with bus_lock:
            # ST
            if svc == "ST" and btype in ("switch", "light", "hybrid", "status", "alarm"):
                states = [c for c in payload if c in "01"]
                if btype == "hybrid":
                    for i, s in enumerate(states[:6]):
                        key = f"{bid}_relay_{i+1}"
                        nuovo = (s == "1")
                        if channel_states.get(key) != nuovo:
                            channel_states[key] = nuovo
                            publish_relay_state(bid, i+1, nuovo)
                elif btype == "status":
                    for i, s in enumerate(states[:board["channels"]]):
                        key = f"{bid}_spia_{i+1}"
                        nuovo = (s == "1")
                        if channel_states.get(key) != nuovo:
                            channel_states[key] = nuovo
                            publish_spia_state(bid, i+1, nuovo)
                elif btype == "alarm":
                    for i, s in enumerate(states[:board.get("switches", 4)]):
                        key = f"{bid}_switch_{i+1}"
                        nuovo = (s == "1")
                        if channel_states.get(key) != nuovo:
                            channel_states[key] = nuovo
                            publish_switch_alarm_state(bid, i+1, nuovo)
                else:
                    for i, s in enumerate(states[:board["channels"]]):
                        key = f"{bid}_{i+1}"
                        nuovo = (s == "1")
                        if channel_states.get(key) != nuovo:
                            channel_states[key] = nuovo
                            publish_state(bid, i+1, nuovo)

            # FB
            elif svc == "FB" and btype in ("switch", "light", "hybrid", "alarm"):
                states = [c for c in payload if c in "01"]
                fb_max = board.get("feedback", len(states))
                for i, s in enumerate(states[:fb_max]):
                    key = f"{bid}_fb_{i+1}"
                    nuovo = (s == "1")
                    if channel_states.get(key) != nuovo:
                        channel_states[key] = nuovo
                        publish_feedback_state(bid, i+1, nuovo)

            # AS
            elif svc == "AS" and btype == "alarm":
                for i, s in enumerate(payload[:board["channels"]]):
                    if s in "DACRN":
                        publish_alarm_state(bid, i+1, s)

            # LS
            elif svc == "LS" and btype == "alarm":
                if len(payload) >= 2:
                    mqtt_client.publish(f"tecnonautica/{bid}/anchor/state", "ON" if payload[0]=='1' else "OFF", retain=True)
                    mqtt_client.publish(f"tecnonautica/{bid}/navlights/state", "ON" if payload[1]=='1' else "OFF", retain=True)

            # CO
            elif svc == "CO" and btype == "hybrid":
                modes = [c for c in payload if c in "BT"]
                if len(modes) == 6:
                    if board.get("channel_modes") != modes:
                        board["channel_modes"] = modes
                        print(f"  {bid} channel_modes aggiornate: {''.join(modes)}", flush=True)

            # ME
            elif svc == "ME" and btype == "hybrid":
                for ch, prefix in [(1, "A"), (2, "B")]:
                    for sign in ["+", "-"]:
                        idx = payload.find(f"{prefix}{sign}")
                        if idx != -1:
                            raw = payload[idx:idx+8]
                            val = ''.join(c for c in raw if c in "0123456789+-.")
                            if val and val.strip("0+-.") != "":
                                publish_sensor_value(bid, ch, val)
                            break

# ─────────────────────────────────────────────────────────────────────────────
# PUBBLICAZIONE STATI
# ─────────────────────────────────────────────────────────────────────────────
def publish_state(bid, ch, stato):
    mqtt_client.publish(f"tecnonautica/{bid}/canale{ch}/state", "ON" if stato else "OFF", retain=True)
    print(f"HA <- {bid}/canale{ch} = {'ON' if stato else 'OFF'}", flush=True)

def publish_relay_state(bid, relay, stato):
    mqtt_client.publish(f"tecnonautica/{bid}/rele{relay}/state", "ON" if stato else "OFF", retain=True)

def publish_feedback_state(bid, fb, stato):
    mqtt_client.publish(f"tecnonautica/{bid}/fb{fb}/state", "ON" if stato else "OFF", retain=True)

def publish_spia_state(bid, ch, stato):
    mqtt_client.publish(f"tecnonautica/{bid}/spia{ch}/state", "ON" if stato else "OFF", retain=True)

def publish_switch_alarm_state(bid, sw, stato):
    mqtt_client.publish(f"tecnonautica/{bid}/switch{sw}/state", "ON" if stato else "OFF", retain=True)

def publish_alarm_state(bid, ch, stato):
    mqtt_client.publish(f"tecnonautica/{bid}/allarme{ch}/state", "ON" if stato in "AC" else "OFF", retain=True)
    mqtt_client.publish(f"tecnonautica/{bid}/allarme{ch}/raw", stato, retain=True)

def publish_sensor_value(bid, ch, value):
    mqtt_client.publish(f"tecnonautica/{bid}/sensore{ch}/state", value, retain=True)
    print(f"HA <- {bid}/sensore{ch} = {value}", flush=True)

# ─────────────────────────────────────────────────────────────────────────────
# BURST
# ─────────────────────────────────────────────────────────────────────────────
def burst_loop(bid, ch, mm, aa, stop_event):
    stay = build_frame("S", mm, aa, f"S{ch}")
    release = build_frame("S", mm, aa, f"R{ch}")
    print(f"Burst start {bid}/canale{ch}", flush=True)
    while not stop_event.is_set():
        tx_queue.put(stay)
        stop_event.wait(0.5)
    tx_queue.put(release)
    print(f"Burst stop {bid}/canale{ch}", flush=True)

# ─────────────────────────────────────────────────────────────────────────────
# SCANSIONE & SETUP
# ─────────────────────────────────────────────────────────────────────────────
def board_display_name(bid, info):
    return BOARD_NAMES.get(bid, info.get("name", bid))

def scan_bus():
    global scanning
    scanning = True
    print("\n=== SCANSIONE BUS TECNONAUTICA ===", flush=True)
    found = {}
    for mm, info in MACHINE_TYPES.items():
        for addr in range(33):
            aa = f"{addr:02d}"
            resp = send_direct(build_frame("Q", mm, aa, "ID"), timeout=0.4)
            if resp and "ID" in resp and mm in resp:
                bid = f"{mm}_{aa}"
                found[bid] = {
                    "id": bid, "machine": mm, "address": aa,
                    "channels": info["channels"], "type": info["type"],
                    "name": f"{info['name']} addr={aa}", "model": info["name"],
                    "feedback": info.get("feedback", 0), "switches": info.get("switches", 0)
                }
                print(f"  ✓ Trovata {info['name']} indirizzo {aa}", flush=True)
            time.sleep(0.05)
    scanning = False
    print("=== FINE SCANSIONE ===\n", flush=True)
    return found

def save_boards(boards):
    with open(BOARDS_FILE, "w") as f: json.dump(boards, f, indent=2)
    print(f"Schede salvate in {BOARDS_FILE}", flush=True)

def load_boards():
    if os.path.exists(BOARDS_FILE):
        with open(BOARDS_FILE) as f: return json.load(f)
    return None

def setup_boards(boards):
    with bus_lock:
        detected_boards.clear()
        detected_boards.update(boards)
        channel_states.clear()

    for bid, info in boards.items():
        print(f"Setup {bid} ({info['type']})...", flush=True)
        if info["type"] == "hybrid":
            info.setdefault("channel_modes", ["T"] * 6)
            for ch in range(1, 3): publish_discovery_sensor(bid, info, ch)
            for r in range(1, 7):
                channel_states[f"{bid}_relay_{r}"] = False
                publish_discovery_relay(bid, info, r)
                mqtt_client.subscribe(f"tecnonautica/{bid}/rele{r}/set")
        elif info["type"] in ("switch", "light"):
            for ch in range(1, info["channels"] + 1):
                channel_states[f"{bid}_{ch}"] = False
                (publish_discovery_switch if info["type"]=="switch" else publish_discovery_light)(bid, info, ch)
                mqtt_client.subscribe(f"tecnonautica/{bid}/canale{ch}/set")
            for fb in range(1, info.get("feedback", info["channels"]) + 1):
                channel_states[f"{bid}_fb_{fb}"] = False
                publish_discovery_feedback(bid, info, fb)
        elif info["type"] == "status":
            for ch in range(1, info["channels"] + 1):
                channel_states[f"{bid}_spia_{ch}"] = False
                publish_discovery_status(bid, info, ch)
        elif info["type"] == "alarm":
            for ch in range(1, info["channels"] + 1): publish_discovery_alarm(bid, info, ch)
            for sw in range(1, info.get("switches", 4) + 1):
                channel_states[f"{bid}_switch_{sw}"] = False
                publish_discovery_switch_alarm(bid, info, sw)
                mqtt_client.subscribe(f"tecnonautica/{bid}/switch{sw}/set")
            for fb in range(1, info.get("feedback", 4) + 1):
                channel_states[f"{bid}_fb_{fb}"] = False
                publish_discovery_feedback(bid, info, fb)
            publish_discovery_alarm_controls(bid, info)
            mqtt_client.subscribe(f"tecnonautica/{bid}/cmd")

def do_scan():
    global enable_commands_at
    print("Scansione richiesta da HA!", flush=True)
    enable_commands_at = time.time() + 9999
    found = scan_bus()
    if not found:
        found["T2_00"] = {"id":"T2_00", "machine":"T2", "address":"00", "channels":6, "type":"switch", "name":"TN218 addr=00", "model":"TN218", "feedback":6}
    save_boards(found)
    setup_boards(found)
    enable_commands_at = time.time() + 3
    publish_scan_button()
    mqtt_client.publish(SCAN_TOPIC + "/result", f"Trovate {len(found)} schede", retain=False)

# ─────────────────────────────────────────────────────────────────────────────
# MQTT DISCOVERY
# ─────────────────────────────────────────────────────────────────────────────
def _dev(bid, info):
    return {"identifiers":[f"tn_{bid}"], "name":board_display_name(bid, info), "model":info.get("model",""), "manufacturer":"Tecnonautica"}

def publish_discovery_switch(bid, info, ch):
    mqtt_client.publish(f"homeassistant/switch/tn_{bid}_ch{ch}/config", json.dumps({
        "name":f"{board_display_name(bid,info)} Canale {ch}", "unique_id":f"tn_{bid}_ch{ch}",
        "state_topic":f"tecnonautica/{bid}/canale{ch}/state", "command_topic":f"tecnonautica/{bid}/canale{ch}/set",
        "payload_on":"ON", "payload_off":"OFF", "device":_dev(bid,info)
    }), retain=True)

def publish_discovery_light(bid, info, ch):
    mqtt_client.publish(f"homeassistant/light/tn_{bid}_luce{ch}/config", json.dumps({
        "name":f"{board_display_name(bid,info)} Luce {ch}", "unique_id":f"tn_{bid}_luce{ch}",
        "state_topic":f"tecnonautica/{bid}/canale{ch}/state", "command_topic":f"tecnonautica/{bid}/canale{ch}/set",
        "payload_on":"ON", "payload_off":"OFF", "device":_dev(bid,info)
    }), retain=True)

def publish_discovery_relay(bid, info, r):
    mqtt_client.publish(f"homeassistant/switch/tn_{bid}_rele{r}/config", json.dumps({
        "name":f"{board_display_name(bid,info)} Rele {r}", "unique_id":f"tn_{bid}_rele{r}",
        "state_topic":f"tecnonautica/{bid}/rele{r}/state", "command_topic":f"tecnonautica/{bid}/rele{r}/set",
        "payload_on":"ON", "payload_off":"OFF", "device":_dev(bid,info)
    }), retain=True)

def publish_discovery_sensor(bid, info, ch):
    mqtt_client.publish(f"homeassistant/sensor/tn_{bid}_sensore{ch}/config", json.dumps({
        "name":f"{board_display_name(bid,info)} Sensore {ch}", "unique_id":f"tn_{bid}_sensore{ch}",
        "state_topic":f"tecnonautica/{bid}/sensore{ch}/state", "device":_dev(bid,info)
    }), retain=True)

def publish_discovery_alarm(bid, info, ch):
    mqtt_client.publish(f"homeassistant/binary_sensor/tn_{bid}_allarme{ch}/config", json.dumps({
        "name":f"{board_display_name(bid,info)} Allarme {ch}", "unique_id":f"tn_{bid}_allarme{ch}",
        "state_topic":f"tecnonautica/{bid}/allarme{ch}/state", "payload_on":"ON", "payload_off":"OFF",
        "device_class":"safety", "device":_dev(bid,info)
    }), retain=True)

def publish_discovery_feedback(bid, info, fb):
    mqtt_client.publish(f"homeassistant/binary_sensor/tn_{bid}_fb{fb}/config", json.dumps({
        "name":f"{board_display_name(bid,info)} Stato {fb}", "unique_id":f"tn_{bid}_fb{fb}",
        "state_topic":f"tecnonautica/{bid}/fb{fb}/state", "payload_on":"ON", "payload_off":"OFF",
        "device_class":"power", "device":_dev(bid,info)
    }), retain=True)

def publish_discovery_status(bid, info, ch):
    mqtt_client.publish(f"homeassistant/binary_sensor/tn_{bid}_spia{ch}/config", json.dumps({
        "name":f"{board_display_name(bid,info)} Spia {ch}", "unique_id":f"tn_{bid}_spia{ch}",
        "state_topic":f"tecnonautica/{bid}/spia{ch}/state", "payload_on":"ON", "payload_off":"OFF",
        "device_class":"power", "device":_dev(bid,info)
    }), retain=True)

def publish_discovery_switch_alarm(bid, info, sw):
    mqtt_client.publish(f"homeassistant/switch/tn_{bid}_switch{sw}/config", json.dumps({
        "name":f"{board_display_name(bid,info)} Luce {sw}", "unique_id":f"tn_{bid}_switch{sw}",
        "state_topic":f"tecnonautica/{bid}/switch{sw}/state", "command_topic":f"tecnonautica/{bid}/switch{sw}/set",
        "payload_on":"ON", "payload_off":"OFF", "device":_dev(bid,info)
    }), retain=True)

def publish_discovery_alarm_controls(bid, info):
    for suf, nom, cmd in [("clear","Azzera Allarmi","CL"),("smoke","Reset Fumo","SM")]:
        mqtt_client.publish(f"homeassistant/button/tn_{bid}_{suf}/config", json.dumps({
            "name":f"{board_display_name(bid,info)} {nom}", "unique_id":f"tn_{bid}_{suf}",
            "command_topic":f"tecnonautica/{bid}/cmd", "payload_press":cmd, "device":_dev(bid,info)
        }), retain=True)

def publish_scan_button():
    mqtt_client.publish("homeassistant/button/tn_scan/config", json.dumps({
        "name":"Scansiona Bus", "unique_id":"tn_scan", "command_topic":SCAN_TOPIC, "payload_press":"SCAN",
        "device":{"identifiers":["tn_gateway"],"name":"TN-Bus Bridge","manufacturer":"Tecnonautica"}
    }), retain=True)

# ─────────────────────────────────────────────────────────────────────────────
# HEARTBEAT
# ─────────────────────────────────────────────────────────────────────────────
def heartbeat_thread():
    ping_counter = 0
    mqtt_ready.wait()
    time.sleep(2)
    print("Heartbeat avviato.", flush=True)
    while running:
        time.sleep(1.0)
        if scanning or not detected_boards: continue
        with bus_lock: boards_snap = dict(detected_boards)
        
        tx_queue.put(build_frame("P", "", "", f"{ping_counter:02d}"))
        ping_counter = (ping_counter + 1) % 100
        
        for bid, info in boards_snap.items():
            mm, aa, bt = info["machine"], info["address"], info["type"]
            if bt in ("switch","light"): tx_queue.put(build_frame("Q", mm, aa, "ST"))
            elif bt == "hybrid":
                tx_queue.put(build_frame("Q", mm, aa, "ME"))
                tx_queue.put(build_frame("Q", mm, aa, "ST"))
                tx_queue.put(build_frame("Q", mm, aa, "CO"))
            elif bt == "status": tx_queue.put(build_frame("Q", mm, aa, "ST"))
            elif bt == "alarm":
                tx_queue.put(build_frame("Q", mm, aa, "AS"))
                tx_queue.put(build_frame("Q", mm, aa, "LS"))

# ─────────────────────────────────────────────────────────────────────────────
# MQTT CALLBACKS
# ─────────────────────────────────────────────────────────────────────────────
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connesso a MQTT!", flush=True)
        client.subscribe(SCAN_TOPIC)
        mqtt_ready.set()
        publish_scan_button()
    else: print(f"Errore MQTT: {rc}", flush=True)

def on_message(client, userdata, msg):
    global enable_commands_at
    topic, payload = msg.topic, msg.payload.decode().strip()
    if not payload: return

    if topic == SCAN_TOPIC and payload == "SCAN":
        threading.Thread(target=do_scan, daemon=True).start()
        return
    if time.time() < enable_commands_at: return

    parts = topic.split("/")
    if len(parts) < 4: return
    bid = parts[1]
    with bus_lock: info = detected_boards.get(bid)
    if not info: return
    mm, aa, bt = info["machine"], info["address"], info["type"]

    if parts[2] == "cmd":
        tx_queue.put(build_frame("S", mm, aa, payload))
        return

    # Hybrid Relays
    if bt == "hybrid" and "rele" in parts[2]:
        relay = int(parts[2].replace("rele",""))
        key = f"{bid}_relay_{relay}"
        target = payload == "ON"
        with bus_lock:
            if channel_states.get(key) == target: return
            channel_states[key] = target
        mode = info.get("channel_modes", ["T"]*6)[relay-1] if relay <= 6 else "T"
        if mode == "B":
            if target:
                evt = threading.Event()
                with bus_lock: burst_active[key] = evt
                threading.Thread(target=burst_loop, args=(bid, relay, mm, aa, evt), daemon=True).start()
            else:
                with bus_lock: burst_active.pop(key, None).set()
        else:
            tx_queue.put(build_frame("S", mm, aa, f"P{relay}"))
        client.publish(f"tecnonautica/{bid}/rele{relay}/state", payload, retain=True)
        return

    # Alarm Switches
    if bt == "alarm" and "switch" in parts[2]:
        sw = int(parts[2].replace("switch",""))
        key = f"{bid}_switch_{sw}"
        target = payload == "ON"
        with bus_lock:
            if channel_states.get(key) == target: return
            channel_states[key] = target
        tx_queue.put(build_frame("S", mm, aa, f"P{sw}"))
        client.publish(f"tecnonautica/{bid}/switch{sw}/state", payload, retain=True)
        return

    # Standard Channels (switch/light)
    ch = int(parts[2].replace("canale",""))
    key = f"{bid}_{ch}"
    target = payload == "ON"
    with bus_lock:
        if channel_states.get(key) == target: return
        channel_states[key] = target
    mode = info.get("channel_modes", ["T"]*info["channels"])[ch-1] if bt=="hybrid" else "T"
    if mode == "B":
        if target:
            evt = threading.Event()
            with bus_lock: burst_active[key] = evt
            threading.Thread(target=burst_loop, args=(bid, ch, mm, aa, evt), daemon=True).start()
        else:
            with bus_lock: burst_active.pop(key, None).set()
    else:
        tx_queue.put(build_frame("S", mm, aa, f"P{ch}"))
    client.publish(f"tecnonautica/{bid}/canale{ch}/state", payload, retain=True)

# ─────────────────────────────────────────────────────────────────────────────
# AVVIO
# ─────────────────────────────────────────────────────────────────────────────
mqtt_client = mqtt.Client()
if MQTT_USER: mqtt_client.username_pw_set(MQTT_USER, MQTT_PASS)
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message
mqtt_client.connect(MQTT_HOST, MQTT_PORT, 60)
mqtt_client.loop_start()
mqtt_ready.wait(timeout=10)

boards = load_boards()
if boards:
    setup_boards(boards)
    print("Schede caricate da file.", flush=True)
else:
    print("Nessuna scheda salvata, avvio scansione...", flush=True)
    setup_boards(scan_bus())

enable_commands_at = time.time() + 2
print("Comandi abilitati.", flush=True)

threading.Thread(target=tx_thread, daemon=True).start()
threading.Thread(target=rx_thread, daemon=True).start()
threading.Thread(target=heartbeat_thread, daemon=True).start()
print("Tecnonautica bridge avviato!", flush=True)

try:
    while True: time.sleep(1)
except KeyboardInterrupt:
    running = False
    mqtt_client.loop_stop()
    mqtt_client.disconnect()
    ser.close()
