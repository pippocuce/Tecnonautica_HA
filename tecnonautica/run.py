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
tx_queue           = queue.Queue()
mqtt_ready         = threading.Event()
burst_active       = {}

# Memorizza ultimi valori validi dei sensori
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

def send_direct(frame, timeout=0.8):
    """Invia frame e attendi risposta — usato solo durante scansione"""
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
    
    if not value:
        print(f"  Ignoro valore vuoto per {board_id}/sensore{ch_num}", flush=True)
        return
    
    test_val = value.replace('+', '').replace('-', '').replace('.', '').strip()
    
    if not test_val or test_val == '0' * len(test_val):
        print(f"  Ignoro zero per {board_id}/sensore{ch_num} (raw: {value})", flush=True)
        return
    
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
# BURST
# ─────────────────────────────────────────
def burst_loop(board_id, ch, mm, aa, stop_event):
    stay_frame = build_frame("S", mm, aa, f"S{ch}")
    while not stop_event.is_set():
        tx_queue.put(stay_frame)
        stop_event.wait(0.5)
    tx_queue.put(build_frame("S", mm, aa, f"R{ch}"))
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
            resp = send_direct(frame, timeout=0.3)
            if resp and "ID" in resp and mm in resp:
                board_id = f"{mm}_{aa}"
                found[board_id] = {
                    "machine":  mm,
                    "address":  aa,
                    "channels": info["channels"],
                    "type":     info["type"],
                    "name":     f"{info['name']} addr={aa}",
                    "model":    info["name"],
                    "feedback": info.get("feedback", 0),
                    "switches": info.get("switches", 0),
                }
                print(f"  ✓ Trovata {info['name']} indirizzo {aa}", flush=True)
            time.sleep(0.05)
    if not found:
        print("  Nessuna scheda trovata!", flush=True)
    scanning = False
    print("=== FINE SCANSIONE ===\n", flush=True)
    return found

# ─────────────────────────────────────────
# DISCOVERY MQTT
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
            "model": board_info["model"],
            "manufacturer": "Tecnonautica"
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
            "model": board_info["model"],
            "manufacturer": "Tecnonautica"
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
            "model": board_info["model"],
            "manufacturer": "Tecnonautica"
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
            "model": board_info["model"],
            "manufacturer": "Tecnonautica"
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
            "model": board_info["model"],
            "manufacturer": "Tecnonautica"
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
            "model": board_info["model"],
            "manufacturer": "Tecnonautica"
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
            "model": board_info["model"],
            "manufacturer": "Tecnonautica"
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
            "model": board_info["model"],
            "manufacturer": "Tecnonautica"
        }
    })
    mqtt_client.publish(f"homeassistant/switch/{uid}/config", payload, retain=True)

def publish_discovery_alarm_controls(board_id, board_info):
    base_name = board_display_name(board_id, board_info)
    for uid_suffix, name_suffix, cmd in [
        ("clear",     "Azzera Allarmi",   "CL"),
        ("smoke",     "Reset Fumo",        "SM"),
    ]:
        uid = f"tecnonautica_{board_id}_{uid_suffix}"
        payload = json.dumps({
            "name": f"{base_name} {name_suffix}",
            "unique_id": uid,
            "command_topic": f"tecnonautica/{board_id}/cmd",
            "payload_press": cmd,
            "device": {
                "identifiers": [f"tecnonautica_{board_id}"],
                "name": base_name, "model": board_info["model"],
                "manufacturer": "Tecnonautica"
            }
        })
        mqtt_client.publish(f"homeassistant/button/{uid}/config", payload, retain=True)

def publish_scan_button():
    payload = json.dumps({
        "name": "Scansiona Bus",
        "unique_id": "tecnonautica_scan_button",
        "command_topic": SCAN_TOPIC,
        "payload_press": "SCAN",
        "device": {
            "identifiers": ["tecnonautica_gateway"],
            "name": "Tecnonautica Gateway",
            "model": "RS485 Bridge",
            "manufacturer": "Tecnonautica"
        }
    })
    mqtt_client.publish("homeassistant/button/tecnonautica_scan/config", payload, retain=True)
    print("Pulsante scansione pubblicato", flush=True)

# ─────────────────────────────────────────
# SETUP BOARD
# ─────────────────────────────────────────
def setup_boards(boards):
    global channel_states
    for board_id, info in boards.items():
        print(f"Setup {board_id} ({info['type']})...", flush=True)
        
        if info["type"] == "hybrid":
            info.setdefault("channel_modes", ["T"] * 6)
            for ch in range(1, 3):
                publish_discovery_sensor(board_id, info, ch)
            for relay_num in range(1, 7):
                channel_states[f"{board_id}_relay_{relay_num}"] = False
                publish_discovery_relay(board_id, info, relay_num)
                mqtt_client.subscribe(f"tecnonautica/{board_id}/rele{relay_num}/set")
            for fb_num in range(1, info.get("feedback", 6) + 1):
                channel_states[f"{board_id}_fb_{fb_num}"] = False
                publish_discovery_feedback(board_id, info, fb_num)
                
        elif info["type"] == "switch":
            for ch in range(1, info["channels"] + 1):
                channel_states[f"{board_id}_{ch}"] = False
                publish_discovery_switch(board_id, info, ch)
                mqtt_client.subscribe(f"tecnonautica/{board_id}/canale{ch}/set")
            fb_count = info.get("feedback", info["channels"])
            for fb_num in range(1, fb_count + 1):
                channel_states[f"{board_id}_fb_{fb_num}"] = False
                publish_discovery_feedback(board_id, info, fb_num)
                
        elif info["type"] == "light":
            for ch in range(1, info["channels"] + 1):
                channel_states[f"{board_id}_{ch}"] = False
                publish_discovery_light(board_id, info, ch)
                mqtt_client.subscribe(f"tecnonautica/{board_id}/canale{ch}/set")
            fb_count = info.get("feedback", info["channels"])
            for fb_num in range(1, fb_count + 1):
                channel_states[f"{board_id}_fb_{fb_num}"] = False
                publish_discovery_feedback(board_id, info, fb_num)
                
        elif info["type"] == "status":
            for ch in range(1, info["channels"] + 1):
                channel_states[f"{board_id}_spia_{ch}"] = False
                publish_discovery_status(board_id, info, ch)
                
        elif info["type"] == "alarm":
            for ch in range(1, info["channels"] + 1):
                publish_discovery_alarm(board_id, info, ch)
            switch_count = info.get("switches", 4)
            for sw_num in range(1, switch_count + 1):
                channel_states[f"{board_id}_switch_{sw_num}"] = False
                publish_discovery_switch_alarm(board_id, info, sw_num)
                mqtt_client.subscribe(f"tecnonautica/{board_id}/switch{sw_num}/set")
            fb_count = info.get("feedback", 4)
            for fb_num in range(1, fb_count + 1):
                channel_states[f"{board_id}_fb_{fb_num}"] = False
                publish_discovery_feedback(board_id, info, fb_num)
            publish_discovery_alarm_controls(board_id, info)
            mqtt_client.subscribe(f"tecnonautica/{board_id}/cmd")

def do_scan():
    global detected_boards, enable_commands_at
    print("Avvio scansione su richiesta...", flush=True)
    enable_commands_at = time.time() + 9999
    found = scan_bus()
    if not found:
        found["T2_00"] = {
            "machine": "T2", "address": "00",
            "channels": 6, "type": "switch",
            "name": "TN218 addr=00", "model": "TN218",
        }
    detected_boards = found
    save_boards(found)
    setup_boards(found)
    enable_commands_at = time.time() + 5
    mqtt_client.publish(SCAN_TOPIC + "/result", f"Trovate {len(found)} schede", retain=False)

# ─────────────────────────────────────────
# THREAD TX
# ─────────────────────────────────────────
def tx_thread():
    """Trasmette prima i comandi (cmd_queue), poi le query (tx_queue)."""
    TX_INTERFRAME = 0.03
    TX_POST_QUERY = 0.05
    while running:
        frame = None
        source = None
        try:
            # Priorità assoluta ai comandi da HA
            frame = cmd_queue.get(timeout=0.02)
            source = "CMD"
        except queue.Empty:
            try:
                frame = tx_queue.get(timeout=0.1)
                source = "HB"
            except queue.Empty:
                continue

        if scanning:
            if source == "CMD":
                cmd_queue.task_done()
            else:
                tx_queue.task_done()
            continue

        try:
            ser.write(frame.encode('ascii'))
            print(f"TX [{source}]: {frame}", flush=True)
            if source == "CMD":
                last_command_time = time.time()
                cmd_queue.task_done()
            else:
                tx_queue.task_done()

            is_query = len(frame) > 1 and frame[1] == "Q"
            time.sleep(TX_POST_QUERY if is_query else TX_INTERFRAME)
        except Exception as e:
            print(f"TX errore: {e}", flush=True)

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
                        print(f"RX: {frame}", flush=True)
                        parse_frame(frame)
                    else:
                        break
        except Exception as e:
            if running:
                print(f"RX errore: {e}", flush=True)
            time.sleep(0.01)

# ─────────────────────────────────────────
# PARSING FRAME
# ─────────────────────────────────────────
def parse_frame(msg):
    if time.time() - last_command_time < 0.1:
        return

    # Risposta ST — switch/light/hybrid relè + TN223 spie + TN234 switch
    if "ST" in msg:
        for board_id, info in detected_boards.items():
            if info["type"] not in ["switch", "light", "hybrid", "status", "alarm"]:
                continue
            mm = info["machine"]
            aa = info["address"]
            if mm in msg and f"{mm}{aa}" in msg:
                try:
                    idx = msg.find("ST") + 2
                    stati = ""
                    for c in msg[idx:]:
                        if c in "01":
                            stati += c
                        elif c in "K*]":
                            break
                    
                    if info["type"] == "hybrid" and len(stati) >= 6:
                        for i in range(6):
                            key = f"{board_id}_relay_{i+1}"
                            nuovo_stato = (stati[i] == "1")
                            if nuovo_stato != channel_states.get(key, False):
                                channel_states[key] = nuovo_stato
                                publish_relay_state(board_id, i+1, nuovo_stato)
                                
                    elif info["type"] == "status" and len(stati) >= info["channels"]:
                        for i in range(info["channels"]):
                            key = f"{board_id}_spia_{i+1}"
                            nuovo_stato = (stati[i] == "1")
                            if nuovo_stato != channel_states.get(key, False):
                                channel_states[key] = nuovo_stato
                                publish_spia_state(board_id, i+1, nuovo_stato)
                                
                    elif info["type"] == "alarm":
                        switch_count = info.get("switches", 4)
                        if len(stati) >= switch_count:
                            for i in range(switch_count):
                                key = f"{board_id}_switch_{i+1}"
                                nuovo_stato = (stati[i] == "1")
                                if nuovo_stato != channel_states.get(key, False):
                                    channel_states[key] = nuovo_stato
                                    publish_switch_alarm_state(board_id, i+1, nuovo_stato)
                                    
                    elif len(stati) >= info["channels"]:
                        for i in range(info["channels"]):
                            key = f"{board_id}_{i+1}"
                            nuovo_stato = (stati[i] == "1")
                            if nuovo_stato != channel_states.get(key, False):
                                channel_states[key] = nuovo_stato
                                publish_state(board_id, i+1, nuovo_stato)
                                
                except Exception as e:
                    print(f"  Parse ST errore: {e}", flush=True)
                break

    # Risposta FB — feedback/LED di stato per tutte le schede
    if "FB" in msg:
        for board_id, info in detected_boards.items():
            if info["type"] not in ["switch", "light", "hybrid", "alarm"]:
                continue
            mm = info["machine"]
            aa = info["address"]
            if mm in msg and f"{mm}{aa}" in msg:
                try:
                    idx = msg.find("FB") + 2
                    stati = ""
                    for c in msg[idx:]:
                        if c in "01":
                            stati += c
                        elif c in "K*]":
                            break
                    
                    fb_count = info.get("feedback", 0)
                    if fb_count > 0 and len(stati) >= fb_count:
                        for i in range(fb_count):
                            key = f"{board_id}_fb_{i+1}"
                            nuovo_stato = (stati[i] == "1")
                            if nuovo_stato != channel_states.get(key, False):
                                channel_states[key] = nuovo_stato
                                publish_feedback_state(board_id, i+1, nuovo_stato)
                                
                except Exception as e:
                    print(f"  Parse FB errore: {e}", flush=True)
                break

    # Risposta AS — allarmi TN234
    if "AS" in msg:
        for board_id, info in detected_boards.items():
            if info["type"] != "alarm":
                continue
            if info["machine"] in msg and f"{info['machine']}{info['address']}" in msg:
                try:
                    idx = msg.find("AS") + 2
                    stati = msg[idx:idx+info["channels"]]
                    for i, s in enumerate(stati):
                        if s in ["D", "A", "C", "R", "N"]:
                            publish_alarm_state(board_id, i+1, s)
                except Exception as e:
                    print(f"  Parse AS errore: {e}", flush=True)
                break

    # Risposta LS — luci TN234
    if "LS" in msg:
        for board_id, info in detected_boards.items():
            if info["type"] != "alarm":
                continue
            if info["machine"] in msg and f"{info['machine']}{info['address']}" in msg:
                try:
                    idx = msg.find("LS") + 2
                    f_state = msg[idx] if idx < len(msg) else "0"
                    n_state = msg[idx+1] if idx+1 < len(msg) else "0"
                    mqtt_client.publish(f"tecnonautica/{board_id}/anchor/state",
                                       "ON" if f_state == "1" else "OFF", retain=True)
                    mqtt_client.publish(f"tecnonautica/{board_id}/navlights/state",
                                       "ON" if n_state == "1" else "OFF", retain=True)
                except Exception as e:
                    print(f"  Parse LS errore: {e}", flush=True)
                break

    # Risposta CO — configurazione canali TN267
    if "CO" in msg:
        for board_id, info in detected_boards.items():
            if info["type"] != "hybrid":
                continue
            if info["machine"] in msg and f"{info['machine']}{info['address']}" in msg:
                try:
                    idx = msg.find("CO") + 2
                    modes = ""
                    for c in msg[idx:]:
                        if c in "BT":
                            modes += c
                        elif c in "K*]":
                            break
                    if len(modes) == 6:
                        new_modes = list(modes)
                        if info.get("channel_modes") != new_modes:
                            info["channel_modes"] = new_modes
                            print(f"  {board_id} channel_modes = {modes}", flush=True)
                except Exception as e:
                    print(f"  Parse CO errore: {e}", flush=True)
                break

    # Risposta ME — sensori analogici TN267
    if "ME" in msg:
        for board_id, info in detected_boards.items():
            if info["type"] != "hybrid":
                continue
            mm = info["machine"]
            aa = info["address"]
            if mm in msg and f"{mm}{aa}" in msg:
                try:
                    for prefix_a in ["A+", "A-"]:
                        if prefix_a in msg:
                            idx = msg.find(prefix_a)
                            raw = msg[idx:idx+7]
                            val = ''.join(c for c in raw if c in '0123456789+-.')
                            val = val[:6]
                            if val:
                                publish_sensor_value(board_id, 1, val)
                            break
                    for prefix_b in ["B+", "B-"]:
                        if prefix_b in msg:
                            idx = msg.find(prefix_b)
                            raw = msg[idx:idx+10]
                            val = ''.join(c for c in raw if c in '0123456789+-.')
                            val = val[:7]
                            if val:
                                publish_sensor_value(board_id, 2, val)
                            break
                except Exception as e:
                    print(f"  Parse ME errore: {e}", flush=True)
                break

# ─────────────────────────────────────────
# THREAD HEARTBEAT
# ─────────────────────────────────────────
def heartbeat_thread():
    ping_counter = 0
    mqtt_ready.wait()
    time.sleep(2)
    print("Heartbeat avviato.", flush=True)
    while running:
        time.sleep(0.2)
        if scanning:
            continue
        if not tx_queue.empty():
            continue
        nn = f"{ping_counter:02d}"
        body = f"P{nn}KK"
        cs = checksum(body)
        tx_queue.put(f"[{body}*{cs}]")
        ping_counter = (ping_counter + 1) % 100
        time.sleep(0.2)
        for board_id, info in detected_boards.items():
            if info["type"] in ["switch", "light"]:
                tx_queue.put(build_frame("Q", info["machine"], info["address"], "ST"))
                tx_queue.put(build_frame("Q", info["machine"], info["address"], "FB"))
            elif info["type"] == "hybrid":
                tx_queue.put(build_frame("Q", info["machine"], info["address"], "ME"))
                tx_queue.put(build_frame("Q", info["machine"], info["address"], "ST"))
                tx_queue.put(build_frame("Q", info["machine"], info["address"], "FB"))
                tx_queue.put(build_frame("Q", info["machine"], info["address"], "CO"))
            elif info["type"] == "status":
                tx_queue.put(build_frame("Q", info["machine"], info["address"], "ST"))
            elif info["type"] == "alarm":
                tx_queue.put(build_frame("Q", info["machine"], info["address"], "AS"))
                tx_queue.put(build_frame("Q", info["machine"], info["address"], "LS"))
                tx_queue.put(build_frame("Q", info["machine"], info["address"], "ST"))
                tx_queue.put(build_frame("Q", info["machine"], info["address"], "FB"))
        # Pausa 0.5 s invece di 5 s
        for _ in range(5):
            if not running:
                break
            time.sleep(0.1)

# ─────────────────────────────────────────
# MQTT
# ─────────────────────────────────────────
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connesso a MQTT!", flush=True)
        mqtt_client.subscribe(SCAN_TOPIC)
        mqtt_ready.set()
    else:
        print(f"Errore MQTT: {rc}", flush=True)

def on_message(client, userdata, msg):
    global last_command_time
    topic   = msg.topic
    payload = msg.payload.decode()
    if not payload:
        return

    if topic == SCAN_TOPIC and payload == "SCAN":
        print("Scansione richiesta da HA!", flush=True)
        threading.Thread(target=do_scan, daemon=True).start()
        return

    if time.time() < enable_commands_at:
        print(f"  Ignoro durante init: {topic}", flush=True)
        return

    print(f"MQTT RX: {topic} = {payload}", flush=True)
    try:
        parts    = topic.split("/")
        board_id = parts[1]
        info     = detected_boards.get(board_id)
        if not info:
            return
        mm = info["machine"]
        aa = info["address"]

        # Comando TN234
        if len(parts) > 2 and parts[2] == "cmd":
            last_command_time = time.time()
            tx_queue.put(build_frame("S", mm, aa, payload))
            return

        # Comando relè TN267
        if info["type"] == "hybrid" and len(parts) > 2 and "rele" in parts[2]:
            relay_num = int(parts[2].replace("rele", ""))
            key = f"{board_id}_relay_{relay_num}"
            stato_attuale = channel_states.get(key, False)
            vuole_on = (payload == "ON")
            ch_modes = info.get("channel_modes", ["T"] * 6)
            mode = ch_modes[relay_num - 1] if relay_num - 1 < len(ch_modes) else "T"

            if vuole_on != stato_attuale:
                channel_states[key] = vuole_on
                publish_relay_state(board_id, relay_num, vuole_on)
                last_command_time = time.time()

                if mode == "B":
                    burst_key = f"{board_id}_relay_{relay_num}"
                    if vuole_on:
                        stop_event = threading.Event()
                        burst_active[burst_key] = stop_event
                        threading.Thread(
                            target=burst_loop,
                            args=(board_id, relay_num, mm, aa, stop_event),
                            daemon=True
                        ).start()
                        print(f"Burst start {board_id}/rele{relay_num}", flush=True)
                    else:
                        if burst_key in burst_active:
                            burst_active[burst_key].set()
                            del burst_active[burst_key]
                else:
                    tx_queue.put(build_frame("S", mm, aa, f"P{relay_num}"))
            else:
                print(f"  {board_id}/rele{relay_num} già {payload}", flush=True)
            return

        # Comando switch luci TN234
        if info["type"] == "alarm" and len(parts) > 2 and "switch" in parts[2]:
            sw_num = int(parts[2].replace("switch", ""))
            key = f"{board_id}_switch_{sw_num}"
            stato_attuale = channel_states.get(key, False)
            vuole_on = (payload == "ON")
            
            if vuole_on != stato_attuale:
                channel_states[key] = vuole_on
                publish_switch_alarm_state(board_id, sw_num, vuole_on)
                last_command_time = time.time()
                tx_queue.put(build_frame("S", mm, aa, f"P{sw_num}"))
            else:
                print(f"  {board_id}/switch{sw_num} già {payload}", flush=True)
            return

        # Comando canale switch/light con supporto burst
        ch = int(parts[2].replace("canale", ""))
        key = f"{board_id}_{ch}"
        stato_attuale = channel_states.get(key, False)
        vuole_on = (payload == "ON")
        ch_modes = info.get("channel_modes", ["T"] * info["channels"])
        mode = ch_modes[ch-1] if ch <= len(ch_modes) else "T"

        if vuole_on != stato_attuale:
            channel_states[key] = vuole_on
            publish_state(board_id, ch, vuole_on)
            last_command_time = time.time()

            if mode == "B":
                burst_key = f"{board_id}_{ch}"
                if vuole_on:
                    stop_event = threading.Event()
                    burst_active[burst_key] = stop_event
                    threading.Thread(
                        target=burst_loop,
                        args=(board_id, ch, mm, aa, stop_event),
                        daemon=True
                    ).start()
                    print(f"Burst start {board_id}/canale{ch}", flush=True)
                else:
                    if burst_key in burst_active:
                        burst_active[burst_key].set()
                        del burst_active[burst_key]
            else:
                tx_queue.put(build_frame("S", mm, aa, f"P{ch}"))
        else:
            print(f"  {board_id}/canale{ch} già {payload}", flush=True)

    except Exception as e:
        print(f"Errore comando: {e}", flush=True)

mqtt_client = mqtt.Client()
if MQTT_USER:
    mqtt_client.username_pw_set(MQTT_USER, MQTT_PASS)
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

print(f"Connessione MQTT {MQTT_HOST}:{MQTT_PORT}...", flush=True)
mqtt_client.connect(MQTT_HOST, MQTT_PORT, 60)
mqtt_client.loop_start()

mqtt_ready.wait(timeout=30)
publish_scan_button()

boards = load_boards()
if boards:
    detected_boards = boards
    setup_boards(boards)
else:
    print("Nessuna scheda salvata, avvio scansione...", flush=True)
    detected_boards = scan_bus()
    if not detected_boards:
        detected_boards["T2_00"] = {
            "machine": "T2", "address": "00",
            "channels": 6, "type": "switch",
            "name": "TN218 addr=00", "model": "TN218",
        }
    save_boards(detected_boards)
    setup_boards(detected_boards)

enable_commands_at = time.time() + 5
print("Comandi abilitati tra 5 secondi...", flush=True)

threading.Thread(target=tx_thread,        daemon=True).start()
threading.Thread(target=rx_thread,        daemon=True).start()
threading.Thread(target=heartbeat_thread, daemon=True).start()

print("Tecnonautica bridge avviato!", flush=True)

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    running = False
    mqtt_client.loop_stop()
    mqtt_client.disconnect()
    ser.close()
mqtt_ready.wait(timeout=30)
publish_scan_button()
