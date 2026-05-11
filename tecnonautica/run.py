import serial
import threading
import queue
import time
import paho.mqtt.client as mqtt
import json
import os

# Leggi configurazione da Home Assistant
with open("/data/options.json") as f:
    config = json.load(f)

PORT      = config.get("serial_port", "/dev/ttyUSB0")
BAUD      = 19200
MQTT_HOST = config.get("mqtt_host", "core-mosquitto")
MQTT_PORT = config.get("mqtt_port", 1883)
MQTT_USER = config.get("mqtt_user", "")
MQTT_PASS = config.get("mqtt_pass", "")
BOARD_NAMES = config.get("board_names", {})

BOARDS_FILE = "/data/boards.json"
SCAN_TOPIC  = "tecnonautica/scan"

MACHINE_TYPES = {
    "T2": {"name": "TN218",  "channels": 6,  "type": "switch",  "feedback": 6},
    "T1": {"name": "TN222",  "channels": 10, "type": "switch",  "feedback": 10},
    "PM": {"name": "TN267",  "channels": 6,  "type": "hybrid",  "feedback": 6},
    "AL": {"name": "TN234",  "channels": 16, "type": "alarm",   "feedback": 4, "switches": 4},
    "SP": {"name": "TN223",  "channels": 10, "type": "status",  "feedback": 0},
    "SL": {"name": "TN224",  "channels": 6,  "type": "light",   "feedback": 6},
}

detected_boards    = {}
running            = True
scanning           = False
channel_states     = {}
last_command_time  = 0
enable_commands_at = time.time() + 9999

# Utilizzo PriorityQueue per gestire le precedenze:
# 0: Comandi (Istantanei)
# 1: Richieste stato post-comando (Aggiornamento veloce)
# 2: Heartbeat/Polling (Traffico di fondo)
tx_queue           = queue.PriorityQueue() 

mqtt_ready         = threading.Event()
burst_active       = {}
last_sensor_values = {}

print(f"Apertura porta {PORT}...", flush=True)
ser = serial.Serial(PORT, BAUD, timeout=0.1)
print("Porta aperta!", flush=True)

def checksum(data):
    cs = 0
    for c in data:
        cs ^= ord(c)
    return format(cs, "02X")

def build_frame(msg_type, machine, addr, data):
    body = f"{msg_type}{machine}{addr}{data}KK"
    cs = checksum(body)
    return f"[{body}*{cs}]"

def send_direct(frame, timeout=0.3): # Timeout ridotto per scansione veloce
    ser.reset_input_buffer()
    ser.write(frame.encode('ascii'))
    buffer = ""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            data = ser.read(64)
            if data:
                buffer += data.decode('ascii', errors='replace')
                if '[' in buffer and ']' in buffer:
                    start = buffer.find('[')
                    end   = buffer.find(']', start)
                    if end != -1:
                        return buffer[start:end+1]
        except:
            pass
    return None

def board_display_name(board_id, board_info):
    return BOARD_NAMES.get(board_id, board_info["name"])

# ─────────────────────────────────────────
# PUBBLICAZIONE STATI
# ─────────────────────────────────────────
def publish_state(board_id, ch_num, stato):
    topic = f"tecnonautica/{board_id}/canale{ch_num}/state"
    payload = "ON" if stato else "OFF"
    mqtt_client.publish(topic, payload, retain=True)
    print(f"HA <- {board_id}/canale{ch_num} = {payload}", flush=True)

def publish_relay_state(board_id, relay_num, stato):
    topic = f"tecnonautica/{board_id}/rele{relay_num}/state"
    payload = "ON" if stato else "OFF"
    mqtt_client.publish(topic, payload, retain=True)
    print(f"HA <- {board_id}/rele{relay_num} = {payload}", flush=True)

def publish_alarm_state(board_id, ch_num, stato):
    topic = f"tecnonautica/{board_id}/allarme{ch_num}/state"
    payload = "ON" if stato in ["A", "C"] else "OFF"
    mqtt_client.publish(topic, payload, retain=True)
    mqtt_client.publish(f"tecnonautica/{board_id}/allarme{ch_num}/raw", stato, retain=True)

def publish_sensor_value(board_id, ch_num, value):
    topic = f"tecnonautica/{board_id}/sensore{ch_num}/state"
    if not value: return
    test_val = value.replace('+', '').replace('-', '').replace('.', '').strip()
    if not test_val or test_val == '0' * len(test_val): return
    mqtt_client.publish(topic, str(value), retain=True)
    last_sensor_values[f"{board_id}_{ch_num}"] = value
    print(f"HA <- {board_id}/sensore{ch_num} = {value}", flush=True)

def publish_feedback_state(board_id, fb_num, stato):
    topic = f"tecnonautica/{board_id}/fb{fb_num}/state"
    payload = "ON" if stato else "OFF"
    mqtt_client.publish(topic, payload, retain=True)
    print(f"HA <- {board_id}/fb{fb_num} = {payload}", flush=True)

def publish_spia_state(board_id, ch_num, stato):
    topic = f"tecnonautica/{board_id}/spia{ch_num}/state"
    payload = "ON" if stato else "OFF"
    mqtt_client.publish(topic, payload, retain=True)
    print(f"HA <- {board_id}/spia{ch_num} = {payload}", flush=True)

def publish_switch_alarm_state(board_id, sw_num, stato):
    topic = f"tecnonautica/{board_id}/switch{sw_num}/state"
    payload = "ON" if stato else "OFF"
    mqtt_client.publish(topic, payload, retain=True)
    print(f"HA <- {board_id}/switch{sw_num} = {payload}", flush=True)

# ─────────────────────────────────────────
# BURST (MODIFICATO CON PRIORITÀ ALTA)
# ─────────────────────────────────────────
def burst_loop(board_id, ch, mm, aa, stop_event):
    stay_frame = build_frame("S", mm, aa, f"S{ch}")
    while not stop_event.is_set():
        tx_queue.put((0, stay_frame))
        stop_event.wait(0.5)
    tx_queue.put((0, build_frame("S", mm, aa, f"R{ch}")))
    print(f"Burst stop {board_id}/canale{ch}", flush=True)

# ─────────────────────────────────────────
# SALVATAGGIO / CARICAMENTO SCHEDE
# ─────────────────────────────────────────
def save_boards(boards):
    with open(BOARDS_FILE, "w") as f:
        json.dump(boards, f, indent=2)
    print(f"Schede salvate in {BOARDS_FILE}", flush=True)

def load_boards():
    if os.path.exists(BOARDS_FILE):
        with open(BOARDS_FILE) as f:
            boards = json.load(f)
        print(f"Schede caricate: {list(boards.keys())}", flush=True)
        return boards
    return None

# ─────────────────────────────────────────
# DISCOVERY
# ─────────────────────────────────────────
def scan_bus():
    global scanning
    scanning = True
    time.sleep(0.3)
    print("\n=== SCANSIONE BUS TECNONAUTICA ===", flush=True)
    found = {}
    for mm, info in MACHINE_TYPES.items():
        for addr in range(33):
            aa = f"{addr:02d}"
            frame = build_frame("Q", mm, aa, "ID")
            resp = send_direct(frame, timeout=0.2)
            if resp and "ID" in resp and mm in resp:
                board_id = f"{mm}_{aa}"
                found[board_id] = {
                    "machine":  mm, "address":  aa,
                    "channels": info["channels"], "type": info["type"],
                    "name":     f"{info['name']} addr={aa}", "model": info["name"],
                    "feedback": info.get("feedback", 0), "switches": info.get("switches", 0),
                }
                print(f"  ✓ Trovata {info['name']} indirizzo {aa}", flush=True)
            time.sleep(0.02)
    scanning = False
    print("=== FINE SCANSIONE ===\n", flush=True)
    return found

# ─────────────────────────────────────────
# DISCOVERY MQTT (Invariato)
# ─────────────────────────────────────────
def publish_discovery_switch(board_id, board_info, ch):
    uid  = f"tecnonautica_{board_id}_ch{ch}"
    name = f"{board_display_name(board_id, board_info)} Canale {ch}"
    payload = json.dumps({
        "name": name, "unique_id": uid,
        "state_topic":   f"tecnonautica/{board_id}/canale{ch}/state",
        "command_topic": f"tecnonautica/{board_id}/canale{ch}/set",
        "payload_on": "ON", "payload_off": "OFF",
        "retain": True, "optimistic": False,
        "device": {
            "identifiers": [f"tecnonautica_{board_id}"],
            "name": board_display_name(board_id, board_info),
            "model": board_info["model"], "manufacturer": "Tecnonautica"
        }
    })
    mqtt_client.publish(f"homeassistant/switch/{uid}/config", payload, retain=True)

def publish_discovery_relay(board_id, board_info, relay_num):
    uid  = f"tecnonautica_{board_id}_relay{relay_num}"
    name = f"{board_display_name(board_id, board_info)} Rele {relay_num}"
    payload = json.dumps({
        "name": name, "unique_id": uid,
        "state_topic":   f"tecnonautica/{board_id}/rele{relay_num}/state",
        "command_topic": f"tecnonautica/{board_id}/rele{relay_num}/set",
        "payload_on": "ON", "payload_off": "OFF",
        "retain": True, "optimistic": False,
        "device": {
            "identifiers": [f"tecnonautica_{board_id}"],
            "name": board_display_name(board_id, board_info),
            "model": board_info["model"], "manufacturer": "Tecnonautica"
        }
    })
    mqtt_client.publish(f"homeassistant/switch/{uid}/config", payload, retain=True)

def publish_discovery_light(board_id, board_info, ch):
    uid  = f"tecnonautica_{board_id}_luce{ch}"
    name = f"{board_display_name(board_id, board_info)} Luce {ch}"
    payload = json.dumps({
        "name": name, "unique_id": uid,
        "state_topic":   f"tecnonautica/{board_id}/canale{ch}/state",
        "command_topic": f"tecnonautica/{board_id}/canale{ch}/set",
        "payload_on": "ON", "payload_off": "OFF",
        "retain": True, "optimistic": False,
        "device": {
            "identifiers": [f"tecnonautica_{board_id}"],
            "name": board_display_name(board_id, board_info),
            "model": board_info["model"], "manufacturer": "Tecnonautica"
        }
    })
    mqtt_client.publish(f"homeassistant/light/{uid}/config", payload, retain=True)

def publish_discovery_sensor(board_id, board_info, ch):
    uid  = f"tecnonautica_{board_id}_sensore{ch}"
    name = f"{board_display_name(board_id, board_info)} Sensore {ch}"
    payload = json.dumps({
        "name": name, "unique_id": uid,
        "state_topic": f"tecnonautica/{board_id}/sensore{ch}/state",
        "device": {
            "identifiers": [f"tecnonautica_{board_id}"],
            "name": board_display_name(board_id, board_info),
            "model": board_info["model"], "manufacturer": "Tecnonautica"
        }
    })
    mqtt_client.publish(f"homeassistant/sensor/{uid}/config", payload, retain=True)

def publish_discovery_alarm(board_id, board_info, ch):
    uid  = f"tecnonautica_{board_id}_allarme{ch}"
    name = f"{board_display_name(board_id, board_info)} Allarme {ch}"
    payload = json.dumps({
        "name": name, "unique_id": uid,
        "state_topic":  f"tecnonautica/{board_id}/allarme{ch}/state",
        "payload_on":   "ON", "payload_off": "OFF",
        "device_class": "safety",
        "device": {
            "identifiers": [f"tecnonautica_{board_id}"],
            "name": board_display_name(board_id, board_info),
            "model": board_info["model"], "manufacturer": "Tecnonautica"
        }
    })
    mqtt_client.publish(f"homeassistant/binary_sensor/{uid}/config", payload, retain=True)

def publish_discovery_feedback(board_id, board_info, fb_num):
    uid  = f"tecnonautica_{board_id}_fb{fb_num}"
    name = f"{board_display_name(board_id, board_info)} Stato {fb_num}"
    payload = json.dumps({
        "name": name, "unique_id": uid,
        "state_topic": f"tecnonautica/{board_id}/fb{fb_num}/state",
        "payload_on": "ON", "payload_off": "OFF",
        "device_class": "power",
        "device": {
            "identifiers": [f"tecnonautica_{board_id}"],
            "name": board_display_name(board_id, board_info),
            "model": board_info["model"], "manufacturer": "Tecnonautica"
        }
    })
    mqtt_client.publish(f"homeassistant/binary_sensor/{uid}/config", payload, retain=True)

def publish_discovery_status(board_id, board_info, ch):
    uid  = f"tecnonautica_{board_id}_spia{ch}"
    name = f"{board_display_name(board_id, board_info)} Spia {ch}"
    payload = json.dumps({
        "name": name, "unique_id": uid,
        "state_topic": f"tecnonautica/{board_id}/spia{ch}/state",
        "payload_on": "ON", "payload_off": "OFF",
        "device_class": "power",
        "device": {
            "identifiers": [f"tecnonautica_{board_id}"],
            "name": board_display_name(board_id, board_info),
            "model": board_info["model"], "manufacturer": "Tecnonautica"
        }
    })
    mqtt_client.publish(f"homeassistant/binary_sensor/{uid}/config", payload, retain=True)

def publish_discovery_switch_alarm(board_id, board_info, sw_num):
    uid  = f"tecnonautica_{board_id}_switch{sw_num}"
    name = f"{board_display_name(board_id, board_info)} Luce {sw_num}"
    payload = json.dumps({
        "name": name, "unique_id": uid,
        "state_topic":   f"tecnonautica/{board_id}/switch{sw_num}/state",
        "command_topic": f"tecnonautica/{board_id}/switch{sw_num}/set",
        "payload_on": "ON", "payload_off": "OFF",
        "retain": True, "optimistic": False,
        "device": {
            "identifiers": [f"tecnonautica_{board_id}"],
            "name": board_display_name(board_id, board_info),
            "model": board_info["model"], "manufacturer": "Tecnonautica"
        }
    })
    mqtt_client.publish(f"homeassistant/switch/{uid}/config", payload, retain=True)

def publish_discovery_alarm_controls(board_id, board_info):
    base_name = board_display_name(board_id, board_info)
    for uid_suffix, name_suffix, cmd in [("clear", "Azzera Allarmi", "CL"), ("smoke", "Reset Fumo", "SM")]:
        uid = f"tecnonautica_{board_id}_{uid_suffix}"
        payload = json.dumps({
            "name": f"{base_name} {name_suffix}", "unique_id": uid,
            "command_topic": f"tecnonautica/{board_id}/cmd", "payload_press": cmd,
            "device": {
                "identifiers": [f"tecnonautica_{board_id}"],
                "name": base_name, "model": board_info["model"], "manufacturer": "Tecnonautica"
            }
        })
        mqtt_client.publish(f"homeassistant/button/{uid}/config", payload, retain=True)

def publish_scan_button():
    payload = json.dumps({
        "name": "Scansiona Bus", "unique_id": "tecnonautica_scan_button",
        "command_topic": SCAN_TOPIC, "payload_press": "SCAN",
        "device": {
            "identifiers": ["tecnonautica_gateway"], "name": "Tecnonautica Gateway",
            "model": "RS485 Bridge", "manufacturer": "Tecnonautica"
        }
    })
    mqtt_client.publish("homeassistant/button/tecnonautica_scan/config", payload, retain=True)

# ─────────────────────────────────────────
# SETUP BOARD
# ─────────────────────────────────────────
def setup_boards(boards):
    global channel_states
    for board_id, info in boards.items():
        print(f"Setup {board_id} ({info['type']})...", flush=True)
        if info["type"] == "hybrid":
            info.setdefault("channel_modes", ["T"] * 6)
            for ch in range(1, 3): publish_discovery_sensor(board_id, info, ch)
            for r in range(1, 7):
                channel_states[f"{board_id}_relay_{r}"] = False
                publish_discovery_relay(board_id, info, r)
                mqtt_client.subscribe(f"tecnonautica/{board_id}/rele{r}/set")
            for f in range(1, info.get("feedback", 6) + 1):
                channel_states[f"{board_id}_fb_{f}"] = False
                publish_discovery_feedback(board_id, info, f)
        elif info["type"] in ["switch", "light"]:
            for ch in range(1, info["channels"] + 1):
                channel_states[f"{board_id}_{ch}"] = False
                if info["type"] == "switch": publish_discovery_switch(board_id, info, ch)
                else: publish_discovery_light(board_id, info, ch)
                mqtt_client.subscribe(f"tecnonautica/{board_id}/canale{ch}/set")
            for f in range(1, info.get("feedback", info["channels"]) + 1):
                channel_states[f"{board_id}_fb_{f}"] = False
                publish_discovery_feedback(board_id, info, f)
        elif info["type"] == "status":
            for ch in range(1, info["channels"] + 1):
                channel_states[f"{board_id}_spia_{ch}"] = False
                publish_discovery_status(board_id, info, ch)
        elif info["type"] == "alarm":
            for ch in range(1, info["channels"] + 1): publish_discovery_alarm(board_id, info, ch)
            for s in range(1, info.get("switches", 4) + 1):
                channel_states[f"{board_id}_switch_{s}"] = False
                publish_discovery_switch_alarm(board_id, info, s)
                mqtt_client.subscribe(f"tecnonautica/{board_id}/switch{s}/set")
            for f in range(1, info.get("feedback", 4) + 1):
                channel_states[f"{board_id}_fb_{f}"] = False
                publish_discovery_feedback(board_id, info, f)
            publish_discovery_alarm_controls(board_id, info)
            mqtt_client.subscribe(f"tecnonautica/{board_id}/cmd")

def do_scan():
    global detected_boards, enable_commands_at
    enable_commands_at = time.time() + 9999
    found = scan_bus()
    if not found:
        found["T2_00"] = {"machine": "T2", "address": "00", "channels": 6, "type": "switch", "name": "TN218 addr=00", "model": "TN218"}
    detected_boards = found
    save_boards(found)
    setup_boards(found)
    enable_commands_at = time.time() + 2
    mqtt_client.publish(SCAN_TOPIC + "/result", f"Trovate {len(found)} schede", retain=False)

# ─────────────────────────────────────────
# THREAD TX (GESTIONE PRIORITÀ)
# ─────────────────────────────────────────
def tx_thread():
    TX_INTERFRAME_DELAY = 0.04 # Ridotto
    TX_POST_QUERY_DELAY = 0.12 # Ridotto per reattività
    while running:
        try:
            priority, frame = tx_queue.get(timeout=0.1)
            if not scanning:
                ser.write(frame.encode('ascii'))
                # print(f"TX (P{priority}): {frame}", flush=True)
            tx_queue.task_done()
            is_query = "Q" in frame
            time.sleep(TX_POST_QUERY_DELAY if is_query else TX_INTERFRAME_DELAY)
        except queue.Empty: pass
        except Exception as e: print(f"TX errore: {e}", flush=True)

# ─────────────────────────────────────────
# THREAD RX
# ─────────────────────────────────────────
def rx_thread():
    buffer = ""
    while running:
        if scanning:
            time.sleep(0.1)
            continue
        try:
            data = ser.read(64)
            if data:
                buffer += data.decode('ascii', errors='replace')
                while '[' in buffer and ']' in buffer:
                    start = buffer.find('[')
                    end   = buffer.find(']', start)
                    if end != -1:
                        frame = buffer[start:end+1]
                        buffer = buffer[end+1:]
                        parse_frame(frame)
                    else: break
        except Exception as e:
            if running: print(f"RX errore: {e}", flush=True)
            time.sleep(0.01)

# ─────────────────────────────────────────
# PARSING FRAME (TEMPI RIDOTTI)
# ─────────────────────────────────────────
def parse_frame(msg):
    # Ignora eco solo per 100ms, non un secondo intero
    if time.time() - last_command_time < 0.2: 
        pass

    if "ST" in msg:
        for board_id, info in detected_boards.items():
            if info["type"] not in ["switch", "light", "hybrid", "status", "alarm"]: continue
            if info["machine"] in msg and f"{info['machine']}{info['address']}" in msg:
                try:
                    idx = msg.find("ST") + 2
                    stati = "".join(c for c in msg[idx:] if c in "01")
                    if info["type"] == "hybrid" and len(stati) >= 6:
                        for i in range(6):
                            key = f"{board_id}_relay_{i+1}"
                            nuovo = (stati[i] == "1")
                            if nuovo != channel_states.get(key):
                                channel_states[key] = nuovo
                                publish_relay_state(board_id, i+1, nuovo)
                    elif info["type"] == "status" and len(stati) >= info["channels"]:
                        for i in range(info["channels"]):
                            key = f"{board_id}_spia_{i+1}"
                            nuovo = (stati[i] == "1")
                            if nuovo != channel_states.get(key):
                                channel_states[key] = nuovo
                                publish_spia_state(board_id, i+1, nuovo)
                    elif info["type"] == "alarm":
                        sw_count = info.get("switches", 4)
                        if len(stati) >= sw_count:
                            for i in range(sw_count):
                                key = f"{board_id}_switch_{i+1}"
                                nuovo = (stati[i] == "1")
                                if nuovo != channel_states.get(key):
                                    channel_states[key] = nuovo
                                    publish_switch_alarm_state(board_id, i+1, nuovo)
                    elif len(stati) >= info["channels"]:
                        for i in range(info["channels"]):
                            key = f"{board_id}_{i+1}"
                            nuovo = (stati[i] == "1")
                            if nuovo != channel_states.get(key):
                                channel_states[key] = nuovo
                                publish_state(board_id, i+1, nuovo)
                except: pass
                break

    if "FB" in msg:
        for board_id, info in detected_boards.items():
            if info["type"] not in ["switch", "light", "hybrid", "alarm"]: continue
            if info["machine"] in msg and f"{info['machine']}{info['address']}" in msg:
                try:
                    idx = msg.find("FB") + 2
                    stati = "".join(c for c in msg[idx:] if c in "01")
                    fb_count = info.get("feedback", 0)
                    if fb_count > 0 and len(stati) >= fb_count:
                        for i in range(fb_count):
                            key = f"{board_id}_fb_{i+1}"
                            nuovo = (stati[i] == "1")
                            if nuovo != channel_states.get(key):
                                channel_states[key] = nuovo
                                publish_feedback_state(board_id, i+1, nuovo)
                except: pass
                break

    if "AS" in msg:
        for board_id, info in detected_boards.items():
            if info["type"] == "alarm" and info["machine"] in msg and f"{info['machine']}{info['address']}" in msg:
                idx = msg.find("AS") + 2
                stati = msg[idx:idx+info["channels"]]
                for i, s in enumerate(stati):
                    if s in "DACRN": publish_alarm_state(board_id, i+1, s)
                break

    if "LS" in msg:
        for board_id, info in detected_boards.items():
            if info["type"] == "alarm" and info["machine"] in msg and f"{info['machine']}{info['address']}" in msg:
                idx = msg.find("LS") + 2
                f_s, n_s = msg[idx], msg[idx+1]
                mqtt_client.publish(f"tecnonautica/{board_id}/anchor/state", "ON" if f_s == "1" else "OFF", retain=True)
                mqtt_client.publish(f"tecnonautica/{board_id}/navlights/state", "ON" if n_s == "1" else "OFF", retain=True)
                break

    if "ME" in msg:
        for board_id, info in detected_boards.items():
            if info["type"] == "hybrid" and info["machine"] in msg and f"{info['machine']}{info['address']}" in msg:
                for p, ch in [("A+", 1), ("A-", 1), ("B+", 2), ("B-", 2)]:
                    if p in msg:
                        idx = msg.find(p)
                        val = "".join(c for c in msg[idx:idx+10] if c in "0123456789+-.")[:7]
                        publish_sensor_value(board_id, ch, val)
                break

# ─────────────────────────────────────────
# THREAD HEARTBEAT (PRIORITÀ BASSA)
# ─────────────────────────────────────────
def heartbeat_thread():
    ping = 0
    mqtt_ready.wait()
    time.sleep(2)
    while running:
        if scanning or not tx_queue.empty() or time.time() - last_command_time < 1.5:
            time.sleep(0.5)
            continue
        
        # Ping di sistema (Priorità 2)
        tx_queue.put((2, build_frame("P", "", "", f"{ping:02d}")))
        ping = (ping + 1) % 100
        
        for board_id, info in detected_boards.items():
            m, a = info["machine"], info["address"]
            if info["type"] in ["switch", "light"]:
                tx_queue.put((2, build_frame("Q", m, a, "ST")))
                tx_queue.put((2, build_frame("Q", m, a, "FB")))
            elif info["type"] == "hybrid":
                tx_queue.put((2, build_frame("Q", m, a, "ME")))
                tx_queue.put((2, build_frame("Q", m, a, "ST")))
                tx_queue.put((2, build_frame("Q", m, a, "FB")))
            elif info["type"] == "status":
                tx_queue.put((2, build_frame("Q", m, a, "ST")))
            elif info["type"] == "alarm":
                tx_queue.put((2, build_frame("Q", m, a, "AS")))
                tx_queue.put((2, build_frame("Q", m, a, "LS")))
                tx_queue.put((2, build_frame("Q", m, a, "ST")))
            time.sleep(0.05)
        
        for _ in range(40):
            if not running or not tx_queue.empty(): break
            time.sleep(0.1)

# ─────────────────────────────────────────
# MQTT (GESTIONE COMANDI ISTANTANEI)
# ─────────────────────────────────────────
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        mqtt_client.subscribe(SCAN_TOPIC)
        mqtt_ready.set()

def on_message(client, userdata, msg):
    global last_command_time
    topic, payload = msg.topic, msg.payload.decode()
    if not payload or scanning: return

    if topic == SCAN_TOPIC and payload == "SCAN":
        threading.Thread(target=do_scan, daemon=True).start()
        return

    if time.time() < enable_commands_at: return

    try:
        parts = topic.split("/")
        board_id = parts[1]
        info = detected_boards.get(board_id)
        if not info: return
        mm, aa = info["machine"], info["address"]

        last_command_time = time.time()

        # 1. Comandi Generici
        if "cmd" in parts:
            tx_queue.put((0, build_frame("S", mm, aa, payload)))
            return

        # 2. Logica Relay / Switch / Light
        is_relay = "rele" in parts[2]
        is_alarm_sw = "switch" in parts[2]
        ch = int(parts[2].replace("rele","").replace("switch","").replace("canale",""))
        
        key = f"{board_id}_relay_{ch}" if is_relay else f"{board_id}_switch_{ch}" if is_alarm_sw else f"{board_id}_{ch}"
        vuole_on = (payload == "ON")
        
        if vuole_on != channel_states.get(key):
            channel_states[key] = vuole_on
            # Feedback immediato su HA (ottimistico)
            if is_relay: publish_relay_state(board_id, ch, vuole_on)
            elif is_alarm_sw: publish_switch_alarm_state(board_id, ch, vuole_on)
            else: publish_state(board_id, ch, vuole_on)

            # Invia Comando (Priorità 0)
            mode = info.get("channel_modes", ["T"]*16)[ch-1] if not is_alarm_sw else "T"
            if mode == "B":
                if vuole_on:
                    stop = threading.Event()
                    burst_active[key] = stop
                    threading.Thread(target=burst_loop, args=(board_id, ch, mm, aa, stop), daemon=True).start()
                elif key in burst_active:
                    burst_active[key].set()
                    del burst_active[key]
            else:
                tx_queue.put((0, build_frame("S", mm, aa, f"P{ch}")))
            
            # Richiedi subito stato aggiornato (Priorità 1)
            tx_queue.put((1, build_frame("Q", mm, aa, "ST")))
            tx_queue.put((1, build_frame("Q", mm, aa, "FB")))

    except Exception as e: print(f"Errore comando: {e}", flush=True)

mqtt_client = mqtt.Client()
if MQTT_USER: mqtt_client.username_pw_set(MQTT_USER, MQTT_PASS)
mqtt_client.on_connect, mqtt_client.on_message = on_connect, on_message

print(f"Connessione MQTT {MQTT_HOST}:{MQTT_PORT}...")
mqtt_client.connect(MQTT_HOST, MQTT_PORT, 60)
mqtt_client.loop_start()

mqtt_ready.wait(timeout=30)
publish_scan_button()

boards = load_boards()
if boards:
    detected_boards = boards
    setup_boards(boards)
else:
    do_scan()

enable_commands_at = time.time() + 2
threading.Thread(target=tx_thread, daemon=True).start()
threading.Thread(target=rx_thread, daemon=True).start()
threading.Thread(target=heartbeat_thread, daemon=True).start()

print("Tecnonautica bridge pronto!")

try:
    while True: time.sleep(1)
except KeyboardInterrupt:
    running = False
    mqtt_client.disconnect()
    ser.close()
