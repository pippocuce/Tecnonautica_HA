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
    "T2": {"name": "TN218",  "type": "switch",   "channels": 6,  "has_kb": True},
    "T1": {"name": "TN222",  "type": "switch",   "channels": 10, "has_kb": True},
    "PM": {"name": "TN267",  "type": "hybrid",   "channels": 8,  "has_kb": True, "sensors": 2, "relays": 6},
    "AL": {"name": "TN234",  "type": "alarm",    "channels": 16, "has_kb": False},
    "SP": {"name": "TN223",  "type": "warning",  "channels": 10, "has_kb": False},
    "SL": {"name": "TN224",  "type": "light",    "channels": 6,  "has_kb": True},
}

detected_boards    = {}
running            = True
channel_states     = {}
keyboard_states    = {}
sensor_values      = {}
last_command_time  = 0
enable_commands_at = time.time() + 9999
tx_queue           = queue.Queue()
mqtt_ready         = threading.Event()

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
    ser.reset_input_buffer()
    ser.write(frame.encode('ascii'))
    buffer = ""
    deadline = time.time() + timeout
    while time.time() < deadline:
        data = ser.read(64)
        if data:
            buffer += data.decode('ascii', errors='replace')
            if '[' in buffer and ']' in buffer:
                start = buffer.find('[')
                end   = buffer.find(']', start)
                if end != -1:
                    return buffer[start:end+1]
    return None

def board_display_name(board_id, board_info):
    return BOARD_NAMES.get(board_id, board_info["name"])

def read_relay_config(mm, aa, board_id):
    """Legge la configurazione CO (TOGGLE/BURST) di ogni relè TN267"""
    frame = build_frame("Q", mm, aa, "CO")
    resp = send_direct(frame, timeout=0.5)
    
    config = {}
    if resp and "CO" in resp:
        try:
            idx = resp.find("CO") + 2
            stati = resp[idx:idx+6]  # 6 caratteri per 6 relè
            for i, s in enumerate(stati):
                relay_num = i + 1
                mode = "burst" if s == "B" else "toggle" if s == "T" else "unknown"
                config[relay_num] = mode
                print(f"    Relè {relay_num}: {mode.upper()}", flush=True)
        except Exception as e:
            print(f"    Errore lettura CO: {e}", flush=True)
    
    return config

def publish_relay_state(board_id, relay_num, stato):
    """Pubblica stato relè (simile a switch)"""
    topic = f"tecnonautica/{board_id}/relè{relay_num}/state"
    payload = "ON" if stato else "OFF"
    mqtt_client.publish(topic, payload, retain=True)
    print(f"HA <- {board_id}/relè{relay_num} = {payload}", flush=True)

def publish_button_state(board_id, btn_num, stato):
    """Pubblica stato pulsante (pressed/released)"""
    topic = f"tecnonautica/{board_id}/pulsante{btn_num}/state"
    payload = "pressed" if stato else "released"
    mqtt_client.publish(topic, payload, retain=True)
    print(f"HA <- {board_id}/pulsante{btn_num} = {payload}", flush=True)

def publish_state(board_id, ch_num, stato):
    topic = f"tecnonautica/{board_id}/canale{ch_num}/state"
    payload = "ON" if stato else "OFF"
    mqtt_client.publish(topic, payload, retain=True)
    print(f"HA <- {board_id}/canale{ch_num} = {payload}", flush=True)

def publish_alarm_state(board_id, ch_num, stato):
    topic = f"tecnonautica/{board_id}/allarme{ch_num}/state"
    payload = "ON" if stato in ["A", "C"] else "OFF"
    mqtt_client.publish(topic, payload, retain=True)
    mqtt_client.publish(f"tecnonautica/{board_id}/allarme{ch_num}/raw", stato, retain=True)

def publish_sensor_value(board_id, ch_num, value):
    topic = f"tecnonautica/{board_id}/sensore{ch_num}/state"
    mqtt_client.publish(topic, str(value), retain=True)
    print(f"HA <- {board_id}/sensore{ch_num} = {value}", flush=True)

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
    print("\n=== SCANSIONE BUS TECNONAUTICA ===", flush=True)
    found = {}
    for mm, info in MACHINE_TYPES.items():
        for addr in range(33):
            aa = f"{addr:02d}"
            frame = build_frame("Q", mm, aa, "ID")
            resp = send_direct(frame, timeout=0.3)
            if resp and "ID" in resp and mm in resp:
                board_id = f"{mm}_{aa}"
                board_data = {
                    "machine":  mm,
                    "address":  aa,
                    "type":     info["type"],
                    "name":     f"{info['name']} addr={aa}",
                    "model":    info["name"],
                }
                
                # Se è una TN267 (hybrid), leggi la configurazione dei relè
                if mm == "PM":
                    print(f"  Lettura configurazione relè TN267 {aa}...", flush=True)
                    relay_config = read_relay_config(mm, aa, board_id)
                    board_data["relay_config"] = relay_config
                
                found[board_id] = board_data
                print(f"  ✓ Trovata {info['name']} indirizzo {aa}", flush=True)
            time.sleep(0.05)
    if not found:
        print("  Nessuna scheda trovata!", flush=True)
    print("=== FINE SCANSIONE ===\n", flush=True)
    return found

# ─────────────────────────────────────────
# DISCOVERY MQTT - TN267 (HYBRID)
# ─────────────────────────────────────────
def publish_discovery_relay(board_id, board_info, relay_num):
    """TN267: relè come switch o button dipende dalla config"""
    relay_config = board_info.get("relay_config", {})
    relay_mode = relay_config.get(relay_num, "toggle")
    
    uid  = f"tecnonautica_{board_id}_relay{relay_num}"
    name = f"{board_display_name(board_id, board_info)} Relè {relay_num}"
    
    # Se è in modalità BURST (momentaneo), usa un BUTTON, altrimenti SWITCH
    if relay_mode == "burst":
        # Modalità momentanea: BUTTON (pulsante)
        payload = json.dumps({
            "name": name, "unique_id": uid,
            "command_topic": f"tecnonautica/{board_id}/relè{relay_num}/set",
            "payload_press": "PULSE",
            "device": {
                "identifiers": [f"tecnonautica_{board_id}"],
                "name": board_display_name(board_id, board_info),
                "model": board_info["model"],
                "manufacturer": "Tecnonautica"
            }
        })
        mqtt_client.publish(f"homeassistant/button/{uid}/config", payload, retain=True)
        print(f"    💨 Relè {relay_num} configurato come BUTTON (momentaneo)", flush=True)
    
    else:
        # Modalità toggle/switch: SWITCH (interruttore)
        payload = json.dumps({
            "name": name, "unique_id": uid,
            "state_topic":   f"tecnonautica/{board_id}/relè{relay_num}/state",
            "command_topic": f"tecnonautica/{board_id}/relè{relay_num}/set",
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
        print(f"    🔘 Relè {relay_num} configurato come SWITCH (toggle)", flush=True)

def publish_discovery_button(board_id, board_info, btn_num):
    """TN267: pulsanti come binary sensor"""
    uid  = f"tecnonautica_{board_id}_btn{btn_num}"
    name = f"{board_display_name(board_id, board_info)} Pulsante {btn_num}"
    payload = json.dumps({
        "name": name, "unique_id": uid,
        "state_topic": f"tecnonautica/{board_id}/pulsante{btn_num}/state",
        "payload_on": "pressed", "payload_off": "released",
        "device_class": "switch",
        "device": {
            "identifiers": [f"tecnonautica_{board_id}"],
            "name": board_display_name(board_id, board_info),
            "model": board_info["model"],
            "manufacturer": "Tecnonautica"
        }
    })
    mqtt_client.publish(f"homeassistant/binary_sensor/{uid}/config", payload, retain=True)

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

def publish_discovery_alarm(board_id, board_info, ch):
    uid  = f"tecnonautica_{board_id}_allarme{ch}"
    name = f"{board_display_name(board_id, board_info)} Allarme {ch}"
    payload = json.dumps({
        "name": name, "unique_id": uid,
        "state_topic":  f"tecnonautica/{board_id}/allarme{ch}/state",
        "payload_on":   "ON",
        "payload_off":  "OFF",
        "device_class": "safety",
        "device": {
            "identifiers": [f"tecnonautica_{board_id}"],
            "name": board_display_name(board_id, board_info),
            "model": board_info["model"],
            "manufacturer": "Tecnonautica"
        }
    })
    mqtt_client.publish(f"homeassistant/binary_sensor/{uid}/config", payload, retain=True)

def publish_discovery_alarm_controls(board_id, board_info):
    base_name = board_display_name(board_id, board_info)

    uid = f"tecnonautica_{board_id}_clear"
    payload = json.dumps({
        "name": f"{base_name} Azzera Allarmi",
        "unique_id": uid,
        "command_topic": f"tecnonautica/{board_id}/cmd",
        "payload_press": "CL",
        "device": {
            "identifiers": [f"tecnonautica_{board_id}"],
            "name": base_name,
            "model": board_info["model"],
            "manufacturer": "Tecnonautica"
        }
    })
    mqtt_client.publish(f"homeassistant/button/{uid}/config", payload, retain=True)

    uid = f"tecnonautica_{board_id}_anchor"
    payload = json.dumps({
        "name": f"{base_name} Luce Ancoraggio",
        "unique_id": uid,
        "state_topic":   f"tecnonautica/{board_id}/anchor/state",
        "command_topic": f"tecnonautica/{board_id}/cmd",
        "payload_on":  "FD",
        "payload_off": "FD",
        "retain": True,
        "device": {
            "identifiers": [f"tecnonautica_{board_id}"],
            "name": base_name,
            "model": board_info["model"],
            "manufacturer": "Tecnonautica"
        }
    })
    mqtt_client.publish(f"homeassistant/switch/{uid}/config", payload, retain=True)

    uid = f"tecnonautica_{board_id}_navlights"
    payload = json.dumps({
        "name": f"{base_name} Luci Navigazione",
        "unique_id": uid,
        "state_topic":   f"tecnonautica/{board_id}/navlights/state",
        "command_topic": f"tecnonautica/{board_id}/cmd",
        "payload_on":  "NA",
        "payload_off": "NA",
        "retain": True,
        "device": {
            "identifiers": [f"tecnonautica_{board_id}"],
            "name": base_name,
            "model": board_info["model"],
            "manufacturer": "Tecnonautica"
        }
    })
    mqtt_client.publish(f"homeassistant/switch/{uid}/config", payload, retain=True)

    uid = f"tecnonautica_{board_id}_smoke"
    payload = json.dumps({
        "name": f"{base_name} Reset Fumo",
        "unique_id": uid,
        "command_topic": f"tecnonautica/{board_id}/cmd",
        "payload_press": "SM",
        "device": {
            "identifiers": [f"tecnonautica_{board_id}"],
            "name": base_name,
            "model": board_info["model"],
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

def setup_boards(boards):
    global channel_states, keyboard_states, sensor_values
    for board_id, info in boards.items():
        board_type = info.get("type", "unknown")
        print(f"Setup {board_id} ({board_type})...", flush=True)
        
        # TN267 - HYBRID (sensori + relè + pulsanti)
        if board_type == "hybrid":
            # 2 sensori analogici
            for ch in range(1, 3):
                sensor_key = f"{board_id}_sensor_{ch}"
                sensor_values[sensor_key] = "0"
                publish_discovery_sensor(board_id, info, ch)
            
            # 6 relè (controllabili con config dinamica)
            for relay_num in range(1, 7):
                relay_key = f"{board_id}_relay_{relay_num}"
                channel_states[relay_key] = False
                publish_discovery_relay(board_id, info, relay_num)
                mqtt_client.subscribe(f"tecnonautica/{board_id}/relè{relay_num}/set")
            
            # 6 pulsanti (input)
            for btn_num in range(1, 7):
                btn_key = f"{board_id}_button_{btn_num}"
                keyboard_states[btn_key] = False
                publish_discovery_button(board_id, info, btn_num)
        
        # TN222 / TN218 - SWITCH
        elif board_type == "switch":
            num_channels = 10 if info["machine"] == "T1" else 6
            for ch in range(1, num_channels + 1):
                key = f"{board_id}_{ch}"
                channel_states[key] = False
                publish_discovery_switch(board_id, info, ch)
                mqtt_client.subscribe(f"tecnonautica/{board_id}/canale{ch}/set")
        
        # TN224 - LIGHT
        elif board_type == "light":
            for ch in range(1, 7):
                key = f"{board_id}_{ch}"
                channel_states[key] = False
                publish_discovery_light(board_id, info, ch)
                mqtt_client.subscribe(f"tecnonautica/{board_id}/canale{ch}/set")
        
        # TN234 - ALARM
        elif board_type == "alarm":
            for ch in range(1, 17):
                key = f"{board_id}_{ch}"
                channel_states[key] = "D"
                publish_discovery_alarm(board_id, info, ch)
            publish_discovery_alarm_controls(board_id, info)
            mqtt_client.subscribe(f"tecnonautica/{board_id}/cmd")
        
        # TN223 - WARNING LIGHTS
        elif board_type == "warning":
            for ch in range(1, 11):
                key = f"{board_id}_{ch}"
                channel_states[key] = False
                publish_discovery_switch(board_id, info, ch)
                mqtt_client.subscribe(f"tecnonautica/{board_id}/canale{ch}/set")

def do_scan():
    global detected_boards, enable_commands_at
    print("Avvio scansione su richiesta...", flush=True)
    enable_commands_at = time.time() + 9999
    found = scan_bus()
    if not found:
        found["T2_00"] = {
            "machine": "T2", "address": "00",
            "type": "switch",
            "name": "TN218 addr=00", "model": "TN218",
        }
    detected_boards = found
    save_boards(found)
    setup_boards(found)
    enable_commands_at = time.time() + 5
    mqtt_client.publish(SCAN_TOPIC + "/result", f"Trovate {len(found)} schede", retain=False)

# ─────────────────────────────────────────
# Thread TX
# ─────────────────────────────────────────
def tx_thread():
    while running:
        try:
            frame = tx_queue.get(timeout=0.1)
            ser.write(frame.encode('ascii'))
            print(f"TX: {frame}", flush=True)
            tx_queue.task_done()
            time.sleep(0.05)
        except queue.Empty:
            pass
        except Exception as e:
            print(f"TX errore: {e}", flush=True)

# ─────────────────────────────────────────
# Thread RX - PARSER COMPLETO
# ─────────────────────────────────────────
def rx_thread():
    buffer = ""
    while running:
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

def parse_frame(msg):
    global last_command_time
    
    if time.time() - last_command_time < 1.0:
        return

    # ─────────────────────────────────────────
    # ST - Channel Status (Switch, Light, TN267 Relays)
    # ─────────────────────────────────────────
    if "ST" in msg:
        for board_id, info in detected_boards.items():
            board_type = info.get("type", "")
            if board_type not in ["switch", "light", "hybrid", "warning"]:
                continue
            
            mm = info["machine"]
            aa = info["address"]
            
            if mm not in msg or aa not in msg:
                continue
            
            try:
                idx = msg.find("ST") + 2
                stati = ""
                for c in msg[idx:]:
                    if c in "01":
                        stati += c
                    elif c in "K*]":
                        break
                
                # TN267: 6 relè
                if board_type == "hybrid" and len(stati) == 6:
                    for i, s in enumerate(stati):
                        key = f"{board_id}_relay_{i+1}"
                        nuovo_stato = (s == "1")
                        if nuovo_stato != channel_states.get(key, False):
                            channel_states[key] = nuovo_stato
                            publish_relay_state(board_id, i+1, nuovo_stato)
                
                # T1 (10 canali)
                elif info["machine"] == "T1" and len(stati) == 10:
                    for i, s in enumerate(stati):
                        key = f"{board_id}_{i+1}"
                        nuovo_stato = (s == "1")
                        if nuovo_stato != channel_states.get(key, False):
                            channel_states[key] = nuovo_stato
                            publish_state(board_id, i+1, nuovo_stato)
                
                # T2 (6 canali)
                elif info["machine"] == "T2" and len(stati) == 6:
                    for i, s in enumerate(stati):
                        key = f"{board_id}_{i+1}"
                        nuovo_stato = (s == "1")
                        if nuovo_stato != channel_states.get(key, False):
                            channel_states[key] = nuovo_stato
                            publish_state(board_id, i+1, nuovo_stato)
                
                # SL (6 luci)
                elif info["machine"] == "SL" and len(stati) == 6:
                    for i, s in enumerate(stati):
                        key = f"{board_id}_{i+1}"
                        nuovo_stato = (s == "1")
                        if nuovo_stato != channel_states.get(key, False):
                            channel_states[key] = nuovo_stato
                            publish_state(board_id, i+1, nuovo_stato)
                
                # SP (10 canali)
                elif info["machine"] == "SP" and len(stati) == 10:
                    for i, s in enumerate(stati):
                        key = f"{board_id}_{i+1}"
                        nuovo_stato = (s == "1")
                        if nuovo_stato != channel_states.get(key, False):
                            channel_states[key] = nuovo_stato
                            publish_state(board_id, i+1, nuovo_stato)
            
            except Exception as e:
                print(f"  Parse ST errore: {e}", flush=True)
            break

    # ─────────────────────────────────────────
    # KB - Keyboard Status (TN267 Buttons)
    # ─────────────────────────────────────────
    if "KB" in msg:
        for board_id, info in detected_boards.items():
            if info.get("type") != "hybrid":
                continue
            
            mm = info["machine"]
            aa = info["address"]
            
            if mm not in msg or aa not in msg:
                continue
            
            try:
                idx = msg.find("KB") + 2
                stati = msg[idx:idx+6]
                
                for i, s in enumerate(stati):
                    if s in "01":
                        btn_key = f"{board_id}_button_{i+1}"
                        nuovo_stato = (s == "1")
                        if nuovo_stato != keyboard_states.get(btn_key, False):
                            keyboard_states[btn_key] = nuovo_stato
                            publish_button_state(board_id, i+1, nuovo_stato)
            
            except Exception as e:
                print(f"  Parse KB errore: {e}", flush=True)
            break

        # ─────────────────────────────────────────
    # ME - Analog Sensors (TN267)
    # ─────────────────────────────────────────
    if "ME" in msg:
        for board_id, info in detected_boards.items():
            if info.get("type") != "hybrid":
                continue
            if info["machine"] in msg and info["address"] in msg:
                try:
                    if "A+" in msg or "A-" in msg:
                        idx = msg.find("A+") if "A+" in msg else msg.find("A-")
                        val = msg[idx+1:idx+7].strip()
                        publish_sensor_value(board_id, 1, val)
                    if "B+" in msg or "B-" in msg:
                        idx = msg.find("B+") if "B+" in msg else msg.find("B-")
                        val = msg[idx+1:idx+7].strip()
                        publish_sensor_value(board_id, 2, val)
                except Exception as e:
                    print(f"  Parse ME errore: {e}", flush=True)
                break

    # ─────────────────────────────────────────
    # AS - Alarm Status (TN234)
    # ─────────────────────────────────────────
    if "AS" in msg:
        for board_id, info in detected_boards.items():
            if info.get("type") != "alarm":
                continue
            
            mm = info["machine"]
            aa = info["address"]
            
            if mm not in msg or aa not in msg:
                continue
            
            try:
                idx = msg.find("AS") + 2
                stati = msg[idx:idx+16]
                
                for i, s in enumerate(stati):
                    if s in ["D", "A", "C", "R", "N"]:
                        publish_alarm_state(board_id, i+1, s)
            
            except Exception as e:
                print(f"  Parse AS errore: {e}", flush=True)
            break

    # ─────────────────────────────────────────
    # LS - Lights Status (TN234 Anchor/Nav)
    # ─────────────────────────────────────────
    if "LS" in msg:
        for board_id, info in detected_boards.items():
            if info.get("type") != "alarm":
                continue
            
            mm = info["machine"]
            aa = info["address"]
            
            if mm not in msg or aa not in msg:
                continue
            
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

# ─────────────────────────────────────────
# Thread Heartbeat
# ─────────────────────────────────────────
def heartbeat_thread():
    ping_counter = 0
    mqtt_ready.wait()
    time.sleep(2)
    print("Heartbeat avviato.", flush=True)
    
    while running:
        time.sleep(0.5)  # ← Veloce
        
        if time.time() - last_command_time < 1:  # ← Ridotto
            continue
        if not tx_queue.empty():
            continue
        
        # PING
        nn = f"{ping_counter:02d}"
        body = f"P{nn}KK"
        cs = checksum(body)
        tx_queue.put(f"[{body}*{cs}]")
        ping_counter = (ping_counter + 1) % 100
        time.sleep(0.1)  # ← Ridotto
        
        # QUERY su tutte le schede
        for board_id, info in detected_boards.items():
            mm = info["machine"]
            aa = info["address"]
            board_type = info.get("type", "")
            
            if board_type == "switch":
                tx_queue.put(build_frame("Q", mm, aa, "ST"))
            
            elif board_type == "light":
                tx_queue.put(build_frame("Q", mm, aa, "LS"))
            
            elif board_type == "hybrid":
                tx_queue.put(build_frame("Q", mm, aa, "ME"))
                tx_queue.put(build_frame("Q", mm, aa, "ST"))
                tx_queue.put(build_frame("Q", mm, aa, "KB"))
            
            elif board_type == "alarm":
                tx_queue.put(build_frame("Q", mm, aa, "AS"))
                tx_queue.put(build_frame("Q", mm, aa, "LS"))
            
            elif board_type == "warning":
                tx_queue.put(build_frame("Q", mm, aa, "ST"))
            
            time.sleep(0.05)  # ← Ridotto
        
        for _ in range(20):  # ← Ridotto a 2 secondi
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

    # Comando scansione
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
            print(f"  Scheda {board_id} non trovata!", flush=True)
            return
        
        mm = info["machine"]
        aa = info["address"]
        board_type = info.get("type", "")
        
        # ─────────────────────────────────────────
        # Comando TN234 (Allarmi)
        # ─────────────────────────────────────────
        if board_type == "alarm" and len(parts) > 2 and parts[2] == "cmd":
            last_command_time = time.time()
            tx_queue.put(build_frame("S", mm, aa, payload))
            return
        
        # ─────────────────────────────────────────
        # Comando TN267 Relè
        # ─────────────────────────────────────────
        if board_type == "hybrid" and len(parts) > 2 and "relè" in parts[2]:
            relay_num = int(parts[2].replace("relè", ""))
            relay_config = info.get("relay_config", {})
            relay_mode = relay_config.get(relay_num, "toggle")
            
            key = f"{board_id}_relay_{relay_num}"
            stato_attuale = channel_states.get(key, False)
            
            print(f"  🔌 Relè {relay_num} ({relay_mode}): {payload}", flush=True)
            
            last_command_time = time.time()
            frame = build_frame("S", mm, aa, f"P{relay_num}")
            print(f"  📤 TX: {frame}", flush=True)
            tx_queue.put(frame)
            
            # Se è TOGGLE, aggiorna lo stato locale
            if relay_mode == "toggle":
                vuole_on = (payload == "ON")
                if vuole_on != stato_attuale:
                    channel_states[key] = vuole_on
                    publish_relay_state(board_id, relay_num, vuole_on)
            
            # Se è BURST, non preoccuparti dello stato (è solo un impulso)
            elif relay_mode == "burst":
                print(f"    💨 Impulso momentaneo inviato", flush=True)
            
            return
        
        # ─────────────────────────────────────────
        # Comando Switch / Light / Warning
        # ─────────────────────────────────────────
        if len(parts) > 2 and "canale" in parts[2]:
            ch = int(parts[2].replace("canale", ""))
            key = f"{board_id}_{ch}"
            stato_attuale = channel_states.get(key, False)
            vuole_on = (payload == "ON")
            
            if vuole_on != stato_attuale:
                channel_states[key] = vuole_on
                publish_state(board_id, ch, vuole_on)
                last_command_time = time.time()
                tx_queue.put(build_frame("S", mm, aa, f"P{ch}"))
            else:
                print(f"  {board_id}/canale{ch} già {payload}", flush=True)
    
    except Exception as e:
        print(f"Errore comando MQTT: {e}", flush=True)

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

# Carica schede salvate o scansiona
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
            "type": "switch",
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
