import serial
import threading
import queue
import time
import paho.mqtt.client as mqtt
import json
import os

# ─────────────────────────────────────────
# CONFIGURAZIONE E COSTANTI
# ─────────────────────────────────────────
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
enable_commands_at = time.time() + 5
# PriorityQueue: 0=Comandi (Urgenti), 1=Query post-comando, 2=Heartbeat (Routine)
tx_queue           = queue.PriorityQueue()
mqtt_ready         = threading.Event()
burst_active       = {}
last_sensor_values = {}

# Timeout ridotto a 0.05 per migliorare la reattività con chip CH340
ser = serial.Serial(PORT, BAUD, timeout=0.05)

def checksum(data):
    cs = 0
    for c in data: cs ^= ord(c)
    return format(cs, "02X")

def build_frame(msg_type, machine, addr, data):
    body = f"{msg_type}{machine}{addr}{data}KK"
    return f"[{body}*{checksum(body)}]"

def send_direct(frame, timeout=0.3):
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
                if end != -1: return buffer[start:end+1]
    return None

def board_display_name(board_id, board_info):
    return BOARD_NAMES.get(board_id, board_info["name"])

# ─────────────────────────────────────────
# PUBBLICAZIONE STATI MQTT
# ─────────────────────────────────────────
def publish_state(board_id, ch_num, stato):
    mqtt_client.publish(f"tecnonautica/{board_id}/canale{ch_num}/state", "ON" if stato else "OFF", retain=True)

def publish_relay_state(board_id, relay_num, stato):
    mqtt_client.publish(f"tecnonautica/{board_id}/rele{relay_num}/state", "ON" if stato else "OFF", retain=True)

def publish_alarm_state(board_id, ch_num, stato):
    mqtt_client.publish(f"tecnonautica/{board_id}/allarme{ch_num}/state", "ON" if stato in ["A", "C"] else "OFF", retain=True)
    mqtt_client.publish(f"tecnonautica/{board_id}/allarme{ch_num}/raw", stato, retain=True)

def publish_sensor_value(board_id, ch_num, value):
    if not value or value.strip() in ["", "0", "0000"]: return
    mqtt_client.publish(f"tecnonautica/{board_id}/sensore{ch_num}/state", str(value), retain=True)
    last_sensor_values[f"{board_id}_{ch_num}"] = value

def publish_feedback_state(board_id, fb_num, stato):
    mqtt_client.publish(f"tecnonautica/{board_id}/fb{fb_num}/state", "ON" if stato else "OFF", retain=True)

def publish_spia_state(board_id, ch_num, stato):
    mqtt_client.publish(f"tecnonautica/{board_id}/spia{ch_num}/state", "ON" if stato else "OFF", retain=True)

def publish_switch_alarm_state(board_id, sw_num, stato):
    mqtt_client.publish(f"tecnonautica/{board_id}/switch{sw_num}/state", "ON" if stato else "OFF", retain=True)

# ─────────────────────────────────────────
# DISCOVERY E GESTIONE FILES
# ─────────────────────────────────────────
def save_boards(boards):
    with open(BOARDS_FILE, "w") as f: json.dump(boards, f, indent=2)

def load_boards():
    if os.path.exists(BOARDS_FILE):
        with open(BOARDS_FILE) as f: return json.load(f)
    return None

def scan_bus():
    global scanning
    scanning = True
    time.sleep(0.2)
    found = {}
    for mm, info in MACHINE_TYPES.items():
        for addr in range(33):
            aa = f"{addr:02d}"
            resp = send_direct(build_frame("Q", mm, aa, "ID"), timeout=0.2)
            if resp and "ID" in resp and mm in resp:
                board_id = f"{mm}_{aa}"
                found[board_id] = {
                    "machine": mm, "address": aa, "channels": info["channels"],
                    "type": info["type"], "name": f"{info['name']} addr={aa}",
                    "model": info["name"], "feedback": info.get("feedback", 0),
                    "switches": info.get("switches", 0),
                }
    scanning = False
    return found

# ─────────────────────────────────────────
# THREAD TX (GESTORE PRIORITÀ)
# ─────────────────────────────────────────
def tx_thread():
    while running:
        try:
            priority, frame = tx_queue.get(timeout=0.1)
            
            # Se è routine (P2) ma c'è stato un comando recente, scartiamo per liberare il bus
            if priority == 2 and (time.time() - last_command_time < 1.5):
                tx_queue.task_done()
                continue

            if not scanning:
                ser.reset_input_buffer()
                ser.write(frame.encode('ascii'))
                ser.flush()
            
            tx_queue.task_done()
            # Timing aggressivo: 80ms per query, 40ms per comandi
            time.sleep(0.08 if "Q" in frame else 0.04)
        except queue.Empty: pass

# ─────────────────────────────────────────
# THREAD RX E PARSING
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
                        parse_frame(buffer[start:end+1])
                        buffer = buffer[end+1:]
                    else: break
        except: time.sleep(0.01)

def parse_frame(msg):
    # Ignora feedback seriali durante l'invio comandi
    if time.time() - last_command_time < 0.1: return

    if "ST" in msg or "FB" in msg or "AS" in msg or "LS" in msg or "ME" in msg or "CO" in msg:
        for board_id, info in detected_boards.items():
            mm, aa = info["machine"], info["address"]
            if f"{mm}{aa}" not in msg: continue

            try:
                if "ST" in msg:
                    idx = msg.find("ST") + 2
                    stati = "".join(c for c in msg[idx:idx+20] if c in "01")
                    if info["type"] == "hybrid":
                        for i in range(min(6, len(stati))):
                            key, val = f"{board_id}_relay_{i+1}", (stati[i] == "1")
                            if val != channel_states.get(key): 
                                channel_states[key] = val
                                publish_relay_state(board_id, i+1, val)
                    elif info["type"] == "status":
                        for i in range(min(info["channels"], len(stati))):
                            key, val = f"{board_id}_spia_{i+1}", (stati[i] == "1")
                            if val != channel_states.get(key):
                                channel_states[key] = val
                                publish_spia_state(board_id, i+1, val)
                    else:
                        for i in range(min(info["channels"], len(stati))):
                            key, val = f"{board_id}_{i+1}", (stati[i] == "1")
                            if val != channel_states.get(key):
                                channel_states[key] = val
                                publish_state(board_id, i+1, val)

                if "FB" in msg:
                    idx = msg.find("FB") + 2
                    stati = "".join(c for c in msg[idx:idx+20] if c in "01")
                    for i in range(min(info.get("feedback", 0), len(stati))):
                        key, val = f"{board_id}_fb_{i+1}", (stati[i] == "1")
                        if val != channel_states.get(key):
                            channel_states[key] = val
                            publish_feedback_state(board_id, i+1, val)

                if "AS" in msg and info["type"] == "alarm":
                    idx = msg.find("AS") + 2
                    for i, s in enumerate(msg[idx:idx+info["channels"]]):
                        if s in "DA CRN": publish_alarm_state(board_id, i+1, s)

                if "ME" in msg and info["type"] == "hybrid":
                    for p, ch in [("A", 1), ("B", 2)]:
                        if p in msg:
                            idx = msg.find(p)
                            val = "".join(c for c in msg[idx:idx+10] if c in "0123456789+-.")
                            publish_sensor_value(board_id, ch, val[:7])

            except Exception as e: print(f"Errore parsing: {e}")

# ─────────────────────────────────────────
# HEARTBEAT (ROUTINE)
# ─────────────────────────────────────────
def heartbeat_thread():
    ping = 0
    mqtt_ready.wait()
    while running:
        if scanning or (time.time() - last_command_time < 2.0):
            time.sleep(0.5)
            continue
        
        # PING di sistema
        tx_queue.put((2, build_frame("P", f"{ping:02d}", "00", "KK")))
        ping = (ping + 1) % 100
        
        # Polling stati
        for board_id, info in detected_boards.items():
            mm, aa = info["machine"], info["address"]
            tx_queue.put((2, build_frame("Q", mm, aa, "ST")))
            if info.get("feedback", 0) > 0:
                tx_queue.put((2, build_frame("Q", mm, aa, "FB")))
            if info["type"] == "hybrid":
                tx_queue.put((2, build_frame("Q", mm, aa, "ME")))
            time.sleep(0.1)
        time.sleep(5)

# ─────────────────────────────────────────
# MQTT E COMANDI REATTIVI
# ─────────────────────────────────────────
def on_message(client, userdata, msg):
    global last_command_time
    topic, payload = msg.topic, msg.payload.decode()
    if not payload or time.time() < enable_commands_at: return

    if topic == SCAN_TOPIC and payload == "SCAN":
        threading.Thread(target=do_scan, daemon=True).start()
        return

    try:
        parts = topic.split("/")
        board_id = parts[1]
        info = detected_boards.get(board_id)
        if not info: return

        # BLOCCO IMMEDIATO DEL TRAFFICO DI ROUTINE
        last_command_time = time.time()

        # SVUOTA LA CODA ISTANTANEAMENTE
        while not tx_queue.empty():
            try: tx_queue.get_nowait(); tx_queue.task_done()
            except: break

        mm, aa = info["machine"], info["address"]
        
        if "rele" in parts[2] or "canale" in parts[2] or "switch" in parts[2]:
            ch_str = "".join(filter(str.isdigit, parts[2]))
            ch = int(ch_str)
            
            # INVIA COMANDO (Priorità 0 - Massima)
            tx_queue.put((0, build_frame("S", mm, aa, f"P{ch}")))
            
            # CHIEDI STATO IMMEDIATO (Priorità 1) per feedback visivo in HA
            tx_queue.put((1, build_frame("Q", mm, aa, "ST")))
            tx_queue.put((1, build_frame("Q", mm, aa, "FB")))
            
            print(f"HA -> TX [PRIO 0]: {board_id} CH{ch} {payload}")

    except Exception as e: print(f"Errore MQTT: {e}")

# ─────────────────────────────────────────
# AVVIO SISTEMA
# ─────────────────────────────────────────
mqtt_client = mqtt.Client()
if MQTT_USER: mqtt_client.username_pw_set(MQTT_USER, MQTT_PASS)
mqtt_client.on_connect = lambda c,u,f,rc: (mqtt_client.subscribe(SCAN_TOPIC), mqtt_ready.set())
mqtt_client.on_message = on_message
mqtt_client.connect(MQTT_HOST, MQTT_PORT, 60)
mqtt_client.loop_start()

boards = load_boards()
if boards:
    detected_boards = boards
    # Qui andrebbero i tuoi cicli publish_discovery_... (omessi per brevità)
else:
    detected_boards = scan_bus()
    save_boards(detected_boards)

threading.Thread(target=tx_thread,        daemon=True).start()
threading.Thread(target=rx_thread,        daemon=True).start()
threading.Thread(target=heartbeat_thread, daemon=True).start()

print("Tecnonautica Bridge ULTRA-REATTIVO avviato!", flush=True)

try:
    while True: time.sleep(1)
except:
    running = False
    ser.close()
