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
    "T2": {"name": "TN218",  "channels": 6,  "type": "switch"},
    "T1": {"name": "TN222",  "channels": 10, "type": "switch"},
    "PM": {"name": "TN267",  "channels": 8,  "type": "hybrid"},  # ← 2 sensori + 6 relè
    "AL": {"name": "TN234",  "channels": 16, "type": "alarm"},
    "SP": {"name": "TN223",  "channels": 10, "type": "switch"},
    "SL": {"name": "TN224",  "channels": 6,  "type": "light"},
}

detected_boards    = {}
running            = True
channel_states     = {}
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

def publish_state(board_id, ch_num, stato):
    topic = f"tecnonautica/{board_id}/canale{ch_num}/state"
    payload = "ON" if stato else "OFF"
    mqtt_client.publish(topic, payload, retain=True)
    print(f"HA <- {board_id}/canale{ch_num} = {payload}", flush=True)

def publish_relay_state(board_id, relay_num, stato):
    """Pubblica stato relè TN267"""
    topic = f"tecnonautica/{board_id}/relè{relay_num}/state"
    payload = "ON" if stato else "OFF"
    mqtt_client.publish(topic, payload, retain=True)
    print(f"HA <- {board_id}/relè{relay_num} = {payload}", flush=True)

def publish_alarm_state(board_id, ch_num, stato):
    topic = f"tecnonautica/{board_id}/allarme{ch_num}/state"
    # D=idle, A=allarme, C=cleared, R=recorded
    payload = "ON" if stato in ["A", "C"] else "OFF"
    mqtt_client.publish(topic, payload, retain=True)
    # Pubblica anche stato grezzo
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
                found[board_id] = {
                    "machine":  mm,
                    "address":  aa,
                    "channels": info["channels"],
                    "type":     info["type"],
                    "name":     f"{info['name']} addr={aa}",
                    "model":    info["name"],
                }
                print(f"  ✓ Trovata {info['name']} indirizzo {aa}", flush=True)
            time.sleep(0.05)
    if not found:
        print("  Nessuna scheda trovata!", flush=True)
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
    """Pubblica discovery per relè TN267"""
    uid  = f"tecnonautica_{board_id}_relay{relay_num}"
    name = f"{board_display_name(board_id, board_info)} Relè {relay_num}"
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
    # Sensore binario per stato allarme
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
    """Pubblica controlli speciali per TN234: clear allarmi, luce ancoraggio, luci navigazione"""
    base_name = board_display_name(board_id, board_info)

    # Pulsante clear allarmi
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

    # Switch luce ancoraggio
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

    # Switch luci navigazione
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

    # Pulsante reset rilevatori fumo
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
    global channel_states
    for board_id, info in boards.items():
        print(f"Setup {board_id} ({info['type']})...", flush=True)
        
        # TN267 HYBRID: 2 sensori + 6 relè
        if info["type"] == "hybrid":
            # 2 sensori
            for ch in range(1, 3):
                channel_states[f"{board_id}_sensor_{ch}"] = "0"
                publish_discovery_sensor(board_id, info, ch)
            # 6 relè
            for relay_num in range(1, 7):
                channel_states[f"{board_id}_relay_{relay_num}"] = False
                publish_discovery_relay(board_id, info, relay_num)
                mqtt_client.subscribe(f"tecnonautica/{board_id}/relè{relay_num}/set")
        else:
            # Tutti gli altri tipi
            for ch in range(1, info["channels"] + 1):
                channel_states[f"{board_id}_{ch}"] = False
                if info["type"] == "switch":
                    publish_discovery_switch(board_id, info, ch)
                    mqtt_client.subscribe(f"tecnonautica/{board_id}/canale{ch}/set")
                elif info["type"] == "light":
                    publish_discovery_light(board_id, info, ch)
                    mqtt_client.subscribe(f"tecnonautica/{board_id}/canale{ch}/set")
                elif info["type"] == "sensor":
                    publish_discovery_sensor(board_id, info, ch)
                elif info["type"] == "alarm":
                    publish_discovery_alarm(board_id, info, ch)
        
        if info["type"] == "alarm":
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
# Thread RX
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
    if time.time() - last_command_time < 1.0:
        return

       # Risposta ME — sensori analogici TN267
    if "ME" in msg:
        for board_id, info in detected_boards.items():
            if info["type"] != "hybrid":
                continue
            if info["machine"] in msg and info["address"] in msg:
                try:
                    # Formato: A+xxxxxB+yyyyy
                    # Estrai valore A (sensore 1)
                    for prefix_a in ["A+", "A-"]:
                        if prefix_a in msg:
                            idx = msg.find(prefix_a)
                            val = msg[idx:idx+7].strip()
                            publish_sensor_value(board_id, 1, val)
                            break
                    # Estrai valore B (sensore 2)
                    for prefix_b in ["B+", "B-"]:
                        if prefix_b in msg:
                            idx = msg.find(prefix_b)
                            val = msg[idx:idx+7].strip()
                            publish_sensor_value(board_id, 2, val)
                            break
                except Exception as e:
                    print(f"  Parse ME errore: {e}", flush=True)
                break
                    
                    # TN267 relè (6 canali)
                    if info["type"] == "hybrid" and len(stati) == 6:
                        for i, s in enumerate(stati):
                            key = f"{board_id}_relay_{i+1}"
                            nuovo_stato = (s == "1")
                            if nuovo_stato != channel_states.get(key, False):
                                channel_states[key] = nuovo_stato
                                publish_relay_state(board_id, i+1, nuovo_stato)
                    # Switch/Light normali
                    elif len(stati) == info["channels"]:
                        for i, s in enumerate(stati):
                            key = f"{board_id}_{i+1}"
                            nuovo_stato = (s == "1")
                            if nuovo_stato != channel_states.get(key, False):
                                channel_states[key] = nuovo_stato
                                publish_state(board_id, i+1, nuovo_stato)
                except Exception as e:
                    print(f"  Parse ST errore: {e}", flush=True)
                break

    # Risposta AS — allarmi TN234
    if "AS" in msg:
        for board_id, info in detected_boards.items():
            if info["type"] != "alarm":
                continue
            if info["machine"] in msg and info["address"] in msg:
                try:
                    idx = msg.find("AS") + 2
                    stati = msg[idx:idx+info["channels"]]
                    for i, s in enumerate(stati):
                        if s in ["D", "A", "C", "R"]:
                            publish_alarm_state(board_id, i+1, s)
                except Exception as e:
                    print(f"  Parse AS errore: {e}", flush=True)
                break

    # Risposta LS (luci) — TN234 anchor/nav lights
    if "LS" in msg:
        for board_id, info in detected_boards.items():
            if info["type"] != "alarm":
                continue
            if info["machine"] in msg and info["address"] in msg:
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

    # Risposta ME — sensori analogici TN267
    if "ME" in msg:
        for board_id, info in detected_boards.items():
            if info["type"] != "hybrid":
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
# Thread Heartbeat
# ─────────────────────────────────────────
def heartbeat_thread():
    ping_counter = 0
    mqtt_ready.wait()
    time.sleep(2)
    print("Heartbeat avviato.", flush=True)
    while running:
        time.sleep(1)
        if time.time() - last_command_time < 2:
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
            elif info["type"] == "hybrid":
                # TN267: query sensori e relè
                tx_queue.put(build_frame("Q", info["machine"], info["address"], "ME"))
                tx_queue.put(build_frame("Q", info["machine"], info["address"], "ST"))
            elif info["type"] == "sensor":
                tx_queue.put(build_frame("Q", info["machine"], info["address"], "ME"))
            elif info["type"] == "alarm":
                tx_queue.put(build_frame("Q", info["machine"], info["address"], "AS"))
                tx_queue.put(build_frame("Q", info["machine"], info["address"], "LS"))
            time.sleep(0.1)
        for _ in range(50):
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
            return
        mm = info["machine"]
        aa = info["address"]

        # Comando TN234
        if len(parts) > 2 and parts[2] == "cmd":
            last_command_time = time.time()
            tx_queue.put(build_frame("S", mm, aa, payload))
            return

        # Comando relè TN267
        if info["type"] == "hybrid" and len(parts) > 2 and "relè" in parts[2]:
            relay_num = int(parts[2].replace("relè", ""))
            key = f"{board_id}_relay_{relay_num}"
            stato_attuale = channel_states.get(key, False)
            vuole_on = (payload == "ON")
            if vuole_on != stato_attuale:
                channel_states[key] = vuole_on
                publish_relay_state(board_id, relay_num, vuole_on)
                last_command_time = time.time()
                tx_queue.put(build_frame("S", mm, aa, f"P{relay_num}"))
            else:
                print(f"  {board_id}/relè{relay_num} già {payload}", flush=True)
            return

        # Comando canale switch/light
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
            "channels": 6, "type": "switch",
            "name": "TN218 addr=00", "model": "TN218",
        }
    save_boards(detected_boards)
    setup_boards(detected_boards)

# Abilita comandi dopo 5 secondi
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
