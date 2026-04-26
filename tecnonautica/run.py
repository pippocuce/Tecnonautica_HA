import serial
import threading
import queue
import time
import re
import json
import os
import logging
import paho.mqtt.client as mqtt

# ─────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("tecnonautica")

# ─────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────
with open("/data/options.json") as f:
    config = json.load(f)

PORT        = config.get("serial_port", "/dev/ttyUSB0")
BAUD        = 19200
MQTT_HOST   = config.get("mqtt_host", "core-mosquitto")
MQTT_PORT   = config.get("mqtt_port", 1883)
MQTT_USER   = config.get("mqtt_user", "")
MQTT_PASS   = config.get("mqtt_pass", "")
BOARD_NAMES = config.get("board_names", {})
SCAN_TIMEOUT = float(config.get("scan_timeout", 0.2))   # tempo per indirizzo durante scansione
SCAN_DELAY   = float(config.get("scan_delay", 0.03))    # pausa fra probe consecutivi

BOARDS_FILE = "/data/boards.json"
SCAN_TOPIC  = "tecnonautica/scan"

MACHINE_TYPES = {
    "T2": {"name": "TN218",  "channels": 6,  "type": "switch"},
    "T1": {"name": "TN222",  "channels": 10, "type": "switch"},
    "PM": {"name": "TN267",  "channels": 8,  "type": "hybrid"},
    "AL": {"name": "TN234",  "channels": 16, "type": "alarm"},
    "SP": {"name": "TN223",  "channels": 10, "type": "switch"},
    "SL": {"name": "TN224",  "channels": 6,  "type": "light"},
}

# ─────────────────────────────────────────
# STATO GLOBALE
# ─────────────────────────────────────────
detected_boards     = {}
running             = True
scanning            = False
channel_states      = {}
last_command_time   = 0.0
enable_commands_at  = time.time() + 9999
tx_queue            = queue.Queue(maxsize=200)
mqtt_ready          = threading.Event()
burst_active        = {}
state_lock          = threading.Lock()        # protegge detected_boards / channel_states
serial_lock         = threading.Lock()        # protegge accessi diretti alla porta
published_discovery = set()                   # uid già pubblicati (per cleanup su rescan)

# Finestra (s) per filtrare l'eco dei nostri stessi comandi
COMMAND_ECHO_WINDOW = 0.25
# Soglia oltre la quale heartbeat smette di accodare
TX_QUEUE_HIGH = 50

# ─────────────────────────────────────────
# PORTA SERIALE (con riapertura automatica)
# ─────────────────────────────────────────
ser = None

def open_serial():
    global ser
    while running:
        try:
            log.info(f"Apertura porta {PORT}…")
            ser = serial.Serial(PORT, BAUD, timeout=0.1)
            log.info("Porta aperta.")
            return
        except Exception as e:
            log.error(f"Apertura porta fallita: {e}. Riprovo fra 3 s…")
            time.sleep(3)

open_serial()

# ─────────────────────────────────────────
# UTILITÀ FRAME
# ─────────────────────────────────────────
def checksum(data):
    cs = 0
    for c in data:
        cs ^= ord(c)
    return format(cs, "02X")

def build_frame(msg_type, machine, addr, data):
    body = f"{msg_type}{machine}{addr}{data}KK"
    return f"[{body}*{checksum(body)}]"

# Estrae (mm, aa) da un frame ricevuto del tipo "[X mm aa CMD ... KK*cc]"
# La maggior parte delle risposte ha 1 char di prefisso dopo '['.
FRAME_RE = re.compile(r"\[.([A-Z0-9]{2})(\d{2})([A-Z]{2})")

def frame_match(msg, mm, aa):
    """True se il frame proviene davvero dalla scheda (mm, aa).
       Confronto posizionale: niente più 'in msg' che dà falsi positivi."""
    m = FRAME_RE.search(msg)
    return bool(m and m.group(1) == mm and m.group(2) == aa)

def frame_cmd(msg):
    """Ritorna il codice comando (ST, AS, LS, ME, ID, …) o None."""
    m = FRAME_RE.search(msg)
    return m.group(3) if m else None

# ─────────────────────────────────────────
# I/O SERIALE
# ─────────────────────────────────────────
def serial_write(data):
    try:
        with serial_lock:
            ser.write(data)
        return True
    except Exception as e:
        log.error(f"Scrittura seriale fallita: {e}. Riapro porta…")
        try:
            ser.close()
        except Exception:
            pass
        open_serial()
        return False

def serial_read(n):
    try:
        with serial_lock:
            return ser.read(n)
    except Exception as e:
        if running:
            log.error(f"Lettura seriale fallita: {e}. Riapro porta…")
        try:
            ser.close()
        except Exception:
            pass
        open_serial()
        return b""

def send_direct(frame, timeout=0.3):
    """Invio sincrono per la scansione."""
    try:
        with serial_lock:
            ser.reset_input_buffer()
            ser.write(frame.encode("ascii"))
    except Exception as e:
        log.error(f"send_direct fallito: {e}")
        open_serial()
        return None

    buffer = ""
    deadline = time.time() + timeout
    while time.time() < deadline:
        chunk = serial_read(64)
        if chunk:
            buffer += chunk.decode("ascii", errors="replace")
            if "[" in buffer and "]" in buffer:
                start = buffer.find("[")
                end   = buffer.find("]", start)
                if end != -1:
                    return buffer[start:end+1]
    return None

# ─────────────────────────────────────────
# PUBBLICAZIONE STATO
# ─────────────────────────────────────────
def board_display_name(board_id, board_info):
    return BOARD_NAMES.get(board_id, board_info["name"])

def publish_state(board_id, ch_num, stato):
    payload = "ON" if stato else "OFF"
    mqtt_client.publish(f"tecnonautica/{board_id}/canale{ch_num}/state", payload, retain=True)
    log.info(f"HA <- {board_id}/canale{ch_num} = {payload}")

def publish_relay_state(board_id, relay_num, stato):
    payload = "ON" if stato else "OFF"
    mqtt_client.publish(f"tecnonautica/{board_id}/rele{relay_num}/state", payload, retain=True)
    log.info(f"HA <- {board_id}/rele{relay_num} = {payload}")

def publish_alarm_state(board_id, ch_num, stato):
    payload = "ON" if stato in ["A", "C"] else "OFF"
    mqtt_client.publish(f"tecnonautica/{board_id}/allarme{ch_num}/state", payload, retain=True)
    mqtt_client.publish(f"tecnonautica/{board_id}/allarme{ch_num}/raw", stato, retain=True)

def publish_sensor_value(board_id, ch_num, value):
    mqtt_client.publish(f"tecnonautica/{board_id}/sensore{ch_num}/state", str(value), retain=True)
    log.info(f"HA <- {board_id}/sensore{ch_num} = {value}")

# ─────────────────────────────────────────
# BURST
# ─────────────────────────────────────────
def burst_loop(board_id, ch, mm, aa, stop_event):
    frame = build_frame("S", mm, aa, f"P{ch}")
    while not stop_event.is_set():
        try:
            tx_queue.put(frame, timeout=1.0)
        except queue.Full:
            log.warning("TX queue piena, salto frame burst")
        stop_event.wait(0.5)
    try:
        tx_queue.put(build_frame("S", mm, aa, f"R{ch}"), timeout=1.0)
    except queue.Full:
        pass
    log.info(f"Burst stop {board_id}/canale{ch}")

# ─────────────────────────────────────────
# PERSISTENZA
# ─────────────────────────────────────────
def save_boards(boards):
    try:
        with open(BOARDS_FILE, "w") as f:
            json.dump(boards, f, indent=2)
        log.info(f"Schede salvate in {BOARDS_FILE}")
    except Exception as e:
        log.error(f"Salvataggio schede fallito: {e}")

def load_boards():
    if os.path.exists(BOARDS_FILE):
        try:
            with open(BOARDS_FILE) as f:
                boards = json.load(f)
            log.info(f"Schede caricate: {list(boards.keys())}")
            return boards
        except Exception as e:
            log.error(f"Caricamento schede fallito: {e}")
    return None

# ─────────────────────────────────────────
# DISCOVERY BUS
# ─────────────────────────────────────────
def scan_bus():
    global scanning
    scanning = True
    time.sleep(0.3)
    log.info("=== SCANSIONE BUS TECNONAUTICA ===")
    found = {}
    try:
        for mm, info in MACHINE_TYPES.items():
            for addr in range(33):
                if not running:
                    break
                aa = f"{addr:02d}"
                resp = send_direct(build_frame("Q", mm, aa, "ID"), timeout=SCAN_TIMEOUT)
                if resp and frame_match(resp, mm, aa) and frame_cmd(resp) == "ID":
                    board_id = f"{mm}_{aa}"
                    found[board_id] = {
                        "machine":  mm,
                        "address":  aa,
                        "channels": info["channels"],
                        "type":     info["type"],
                        "name":     f"{info['name']} addr={aa}",
                        "model":    info["name"],
                    }
                    log.info(f"  ✓ Trovata {info['name']} indirizzo {aa}")
                time.sleep(SCAN_DELAY)
    finally:
        scanning = False
        log.info("=== FINE SCANSIONE ===")
    if not found:
        log.warning("Nessuna scheda trovata!")
    return found

# ─────────────────────────────────────────
# DISCOVERY MQTT
# ─────────────────────────────────────────
def _device_block(board_id, board_info):
    return {
        "identifiers":  [f"tecnonautica_{board_id}"],
        "name":         board_display_name(board_id, board_info),
        "model":        board_info["model"],
        "manufacturer": "Tecnonautica",
    }

def _publish_disc(component, uid, payload):
    mqtt_client.publish(f"homeassistant/{component}/{uid}/config",
                        json.dumps(payload), retain=True)
    published_discovery.add((component, uid))

def publish_discovery_switch(board_id, board_info, ch):
    uid = f"tecnonautica_{board_id}_ch{ch}"
    _publish_disc("switch", uid, {
        "name": f"{board_display_name(board_id, board_info)} Canale {ch}",
        "unique_id": uid,
        "state_topic":   f"tecnonautica/{board_id}/canale{ch}/state",
        "command_topic": f"tecnonautica/{board_id}/canale{ch}/set",
        "payload_on": "ON", "payload_off": "OFF",
        "retain": True, "optimistic": False,
        "device": _device_block(board_id, board_info),
    })

def publish_discovery_relay(board_id, board_info, relay_num):
    uid = f"tecnonautica_{board_id}_relay{relay_num}"
    _publish_disc("switch", uid, {
        "name": f"{board_display_name(board_id, board_info)} Rele {relay_num}",
        "unique_id": uid,
        "state_topic":   f"tecnonautica/{board_id}/rele{relay_num}/state",
        "command_topic": f"tecnonautica/{board_id}/rele{relay_num}/set",
        "payload_on": "ON", "payload_off": "OFF",
        "retain": True, "optimistic": False,
        "device": _device_block(board_id, board_info),
    })

def publish_discovery_light(board_id, board_info, ch):
    uid = f"tecnonautica_{board_id}_luce{ch}"
    _publish_disc("light", uid, {
        "name": f"{board_display_name(board_id, board_info)} Luce {ch}",
        "unique_id": uid,
        "state_topic":   f"tecnonautica/{board_id}/canale{ch}/state",
        "command_topic": f"tecnonautica/{board_id}/canale{ch}/set",
        "payload_on": "ON", "payload_off": "OFF",
        "retain": True, "optimistic": False,
        "device": _device_block(board_id, board_info),
    })

def publish_discovery_sensor(board_id, board_info, ch):
    uid = f"tecnonautica_{board_id}_sensore{ch}"
    _publish_disc("sensor", uid, {
        "name": f"{board_display_name(board_id, board_info)} Sensore {ch}",
        "unique_id": uid,
        "state_topic": f"tecnonautica/{board_id}/sensore{ch}/state",
        "device": _device_block(board_id, board_info),
    })

def publish_discovery_alarm(board_id, board_info, ch):
    uid = f"tecnonautica_{board_id}_allarme{ch}"
    _publish_disc("binary_sensor", uid, {
        "name": f"{board_display_name(board_id, board_info)} Allarme {ch}",
        "unique_id": uid,
        "state_topic":  f"tecnonautica/{board_id}/allarme{ch}/state",
        "payload_on":   "ON", "payload_off": "OFF",
        "device_class": "safety",
        "device": _device_block(board_id, board_info),
    })

def publish_discovery_alarm_controls(board_id, board_info):
    base_name = board_display_name(board_id, board_info)
    for uid_suffix, name_suffix, cmd in [
        ("clear", "Azzera Allarmi", "CL"),
        ("smoke", "Reset Fumo",     "SM"),
    ]:
        uid = f"tecnonautica_{board_id}_{uid_suffix}"
        _publish_disc("button", uid, {
            "name": f"{base_name} {name_suffix}",
            "unique_id": uid,
            "command_topic": f"tecnonautica/{board_id}/cmd",
            "payload_press": cmd,
            "device": _device_block(board_id, board_info),
        })

    for uid_suffix, name_suffix, cmd, state_suffix in [
        ("anchor",    "Luce Ancoraggio",  "FD", "anchor"),
        ("navlights", "Luci Navigazione", "NA", "navlights"),
    ]:
        uid = f"tecnonautica_{board_id}_{uid_suffix}"
        _publish_disc("switch", uid, {
            "name": f"{base_name} {name_suffix}",
            "unique_id": uid,
            "state_topic":   f"tecnonautica/{board_id}/{state_suffix}/state",
            "command_topic": f"tecnonautica/{board_id}/cmd",
            "payload_on": cmd, "payload_off": cmd,
            "retain": True,
            "device": _device_block(board_id, board_info),
        })

def publish_scan_button():
    uid = "tecnonautica_scan_button"
    _publish_disc("button", uid, {
        "name": "Scansiona Bus",
        "unique_id": uid,
        "command_topic": SCAN_TOPIC,
        "payload_press": "SCAN",
        "device": {
            "identifiers": ["tecnonautica_gateway"],
            "name": "Tecnonautica Gateway",
            "model": "RS485 Bridge",
            "manufacturer": "Tecnonautica",
        },
    })
    log.info("Pulsante scansione pubblicato")

def cleanup_stale_discovery(active_uids):
    """Rimuove i discovery di entità scomparse dopo una rescansione."""
    stale = [(c, u) for (c, u) in published_discovery if u not in active_uids]
    for component, uid in stale:
        mqtt_client.publish(f"homeassistant/{component}/{uid}/config", "", retain=True)
        published_discovery.discard((component, uid))
        log.info(f"Discovery rimosso: {component}/{uid}")

# ─────────────────────────────────────────
# SETUP SCHEDE
# ─────────────────────────────────────────
def setup_boards(boards):
    active_uids = {"tecnonautica_scan_button"}
    with state_lock:
        for board_id, info in boards.items():
            log.info(f"Setup {board_id} ({info['type']})…")
            if info["type"] == "hybrid":
                for ch in range(1, 3):
                    publish_discovery_sensor(board_id, info, ch)
                    active_uids.add(f"tecnonautica_{board_id}_sensore{ch}")
                for relay_num in range(1, 7):
                    channel_states.setdefault(f"{board_id}_relay_{relay_num}", False)
                    publish_discovery_relay(board_id, info, relay_num)
                    active_uids.add(f"tecnonautica_{board_id}_relay{relay_num}")
                    mqtt_client.subscribe(f"tecnonautica/{board_id}/rele{relay_num}/set")
            else:
                for ch in range(1, info["channels"] + 1):
                    channel_states.setdefault(f"{board_id}_{ch}", False)
                    if info["type"] == "switch":
                        publish_discovery_switch(board_id, info, ch)
                        active_uids.add(f"tecnonautica_{board_id}_ch{ch}")
                        mqtt_client.subscribe(f"tecnonautica/{board_id}/canale{ch}/set")
                    elif info["type"] == "light":
                        publish_discovery_light(board_id, info, ch)
                        active_uids.add(f"tecnonautica_{board_id}_luce{ch}")
                        mqtt_client.subscribe(f"tecnonautica/{board_id}/canale{ch}/set")
                    elif info["type"] == "sensor":
                        publish_discovery_sensor(board_id, info, ch)
                        active_uids.add(f"tecnonautica_{board_id}_sensore{ch}")
                    elif info["type"] == "alarm":
                        publish_discovery_alarm(board_id, info, ch)
                        active_uids.add(f"tecnonautica_{board_id}_allarme{ch}")
                if info["type"] == "alarm":
                    publish_discovery_alarm_controls(board_id, info)
                    for suf in ("clear", "smoke", "anchor", "navlights"):
                        active_uids.add(f"tecnonautica_{board_id}_{suf}")
                    mqtt_client.subscribe(f"tecnonautica/{board_id}/cmd")
    cleanup_stale_discovery(active_uids)

def do_scan():
    global detected_boards, enable_commands_at
    log.info("Avvio scansione su richiesta…")
    enable_commands_at = time.time() + 9999
    try:
        found = scan_bus()
        if not found:
            found["T2_00"] = {
                "machine": "T2", "address": "00",
                "channels": 6, "type": "switch",
                "name": "TN218 addr=00", "model": "TN218",
            }
        with state_lock:
            # Conserva eventuali channel_modes già presenti
            for bid, info in found.items():
                if bid in detected_boards and "channel_modes" in detected_boards[bid]:
                    info["channel_modes"] = detected_boards[bid]["channel_modes"]
            detected_boards = found
        save_boards(found)
        setup_boards(found)
        mqtt_client.publish(SCAN_TOPIC + "/result", f"Trovate {len(found)} schede", retain=False)
    except Exception as e:
        log.exception(f"Scansione fallita: {e}")
    finally:
        # Riabilita comandi ANCHE in caso di errore
        enable_commands_at = time.time() + 5

# ─────────────────────────────────────────
# THREAD TX
# ─────────────────────────────────────────
def tx_thread():
    while running:
        try:
            frame = tx_queue.get(timeout=0.1)
            if not scanning:
                if serial_write(frame.encode("ascii")):
                    log.debug(f"TX: {frame}")
            tx_queue.task_done()
            time.sleep(0.05)
        except queue.Empty:
            pass
        except Exception as e:
            log.error(f"TX errore: {e}")

# ─────────────────────────────────────────
# THREAD RX
# ─────────────────────────────────────────
def rx_thread():
    buffer = ""
    while running:
        if scanning:
            time.sleep(0.1)
            continue
        chunk = serial_read(64)
        if chunk:
            buffer += chunk.decode("ascii", errors="replace")
            while "[" in buffer and "]" in buffer:
                start = buffer.find("[")
                end   = buffer.find("]", start)
                if end == -1:
                    break
                frame  = buffer[start:end+1]
                buffer = buffer[end+1:]
                log.debug(f"RX: {frame}")
                try:
                    parse_frame(frame)
                except Exception as e:
                    log.exception(f"parse_frame errore: {e}")

def parse_frame(msg):
    cmd = frame_cmd(msg)
    if cmd is None:
        return

    # Filtra solo l'eco di un comando S* appena inviato (finestra breve).
    # ST/AS/LS/ME sono risposte legittime e non vanno scartate.
    if cmd not in ("ST", "AS", "LS", "ME", "ID"):
        if time.time() - last_command_time < COMMAND_ECHO_WINDOW:
            return

    with state_lock:
        boards_snapshot = list(detected_boards.items())

    # ── ST: switch / light / hybrid (relè) ──
    if cmd == "ST":
        for board_id, info in boards_snapshot:
            if info["type"] not in ("switch", "light", "hybrid"):
                continue
            if not frame_match(msg, info["machine"], info["address"]):
                continue
            try:
                idx = msg.find("ST") + 2
                stati = ""
                for c in msg[idx:]:
                    if c in "01":
                        stati += c
                    elif c in "K*]":
                        break
                if info["type"] == "hybrid" and len(stati) == 6:
                    for i, s in enumerate(stati):
                        key = f"{board_id}_relay_{i+1}"
                        nuovo = (s == "1")
                        with state_lock:
                            cambiato = (nuovo != channel_states.get(key, False))
                            if cambiato:
                                channel_states[key] = nuovo
                        if cambiato:
                            publish_relay_state(board_id, i+1, nuovo)
                elif len(stati) == info["channels"]:
                    for i, s in enumerate(stati):
                        key = f"{board_id}_{i+1}"
                        nuovo = (s == "1")
                        with state_lock:
                            cambiato = (nuovo != channel_states.get(key, False))
                            if cambiato:
                                channel_states[key] = nuovo
                        if cambiato:
                            publish_state(board_id, i+1, nuovo)
            except Exception as e:
                log.error(f"Parse ST errore: {e}")
            return

    # ── AS: allarmi TN234 ──
    if cmd == "AS":
        for board_id, info in boards_snapshot:
            if info["type"] != "alarm":
                continue
            if not frame_match(msg, info["machine"], info["address"]):
                continue
            try:
                idx = msg.find("AS") + 2
                stati = msg[idx:idx + info["channels"]]
                for i, s in enumerate(stati):
                    if s in ("D", "A", "C", "R"):
                        publish_alarm_state(board_id, i+1, s)
            except Exception as e:
                log.error(f"Parse AS errore: {e}")
            return

    # ── LS: luci ancoraggio/navigazione TN234 ──
    if cmd == "LS":
        for board_id, info in boards_snapshot:
            if info["type"] != "alarm":
                continue
            if not frame_match(msg, info["machine"], info["address"]):
                continue
            try:
                idx = msg.find("LS") + 2
                f_state = msg[idx]   if idx     < len(msg) else "0"
                n_state = msg[idx+1] if idx+1   < len(msg) else "0"
                mqtt_client.publish(f"tecnonautica/{board_id}/anchor/state",
                                    "ON" if f_state == "1" else "OFF", retain=True)
                mqtt_client.publish(f"tecnonautica/{board_id}/navlights/state",
                                    "ON" if n_state == "1" else "OFF", retain=True)
            except Exception as e:
                log.error(f"Parse LS errore: {e}")
            return

    # ── ME: sensori analogici TN267 ──
    if cmd == "ME":
        for board_id, info in boards_snapshot:
            if info["type"] != "hybrid":
                continue
            if not frame_match(msg, info["machine"], info["address"]):
                continue
            try:
                me_re = re.compile(r"([AB])([+\-])(\d+)")
                values = {"A": None, "B": None}
                for m in me_re.finditer(msg):
                    ch_letter, sign, digits = m.group(1), m.group(2), m.group(3)
                    if values[ch_letter] is None:
                        values[ch_letter] = f"{sign}{digits}"
                if values["A"] is not None:
                    publish_sensor_value(board_id, 1, values["A"])
                if values["B"] is not None:
                    publish_sensor_value(board_id, 2, values["B"])
            except Exception as e:
                log.error(f"Parse ME errore: {e}")
            return

# ─────────────────────────────────────────
# THREAD HEARTBEAT
# ─────────────────────────────────────────
def heartbeat_thread():
    ping_counter = 0
    mqtt_ready.wait()
    time.sleep(2)
    log.info("Heartbeat avviato.")
    while running:
        time.sleep(1)
        if scanning:
            continue
        if time.time() - last_command_time < 2:
            continue
        if not tx_queue.empty():
            continue
        if tx_queue.qsize() > TX_QUEUE_HIGH:
            log.warning("TX queue troppo piena, salto heartbeat")
            time.sleep(2)
            continue

        nn   = f"{ping_counter:02d}"
        body = f"P{nn}KK"
        try:
            tx_queue.put(f"[{body}*{checksum(body)}]", timeout=0.5)
        except queue.Full:
            pass
        ping_counter = (ping_counter + 1) % 100
        time.sleep(0.2)

        with state_lock:
            boards_snapshot = list(detected_boards.items())

        for board_id, info in boards_snapshot:
            try:
                if info["type"] in ("switch", "light"):
                    tx_queue.put(build_frame("Q", info["machine"], info["address"], "ST"), timeout=0.5)
                elif info["type"] == "hybrid":
                    tx_queue.put(build_frame("Q", info["machine"], info["address"], "ME"), timeout=0.5)
                    tx_queue.put(build_frame("Q", info["machine"], info["address"], "ST"), timeout=0.5)
                elif info["type"] == "sensor":
                    tx_queue.put(build_frame("Q", info["machine"], info["address"], "ME"), timeout=0.5)
                elif info["type"] == "alarm":
                    tx_queue.put(build_frame("Q", info["machine"], info["address"], "AS"), timeout=0.5)
                    tx_queue.put(build_frame("Q", info["machine"], info["address"], "LS"), timeout=0.5)
            except queue.Full:
                log.warning("TX queue piena durante heartbeat")
                break
            time.sleep(0.1)

        for _ in range(50):
            if not running:
                break
            time.sleep(0.1)

# ─────────────────────────────────────────
# MQTT
# ─────────────────────────────────────────
def on_connect(client, userdata, flags, rc, *args):
    if rc == 0:
        log.info("Connesso a MQTT.")
        client.subscribe(SCAN_TOPIC)
        # Re-subscribe per ogni scheda nota (utile dopo riconnessioni)
        with state_lock:
            for board_id, info in detected_boards.items():
                if info["type"] == "hybrid":
                    for r in range(1, 7):
                        client.subscribe(f"tecnonautica/{board_id}/rele{r}/set")
                elif info["type"] in ("switch", "light"):
                    for ch in range(1, info["channels"] + 1):
                        client.subscribe(f"tecnonautica/{board_id}/canale{ch}/set")
                elif info["type"] == "alarm":
                    client.subscribe(f"tecnonautica/{board_id}/cmd")
        mqtt_ready.set()
    else:
        log.error(f"Errore connessione MQTT: {rc}")

def on_disconnect(client, userdata, rc, *args):
    log.warning(f"MQTT disconnesso (rc={rc}). loop_start gestirà la riconnessione.")

def on_message(client, userdata, msg):
    global last_command_time
    topic   = msg.topic
    payload = msg.payload.decode(errors="replace")
    if not payload:
        return

    if topic == SCAN_TOPIC and payload == "SCAN":
        log.info("Scansione richiesta da HA")
        threading.Thread(target=do_scan, daemon=True).start()
        return

    if time.time() < enable_commands_at:
        log.info(f"  Ignoro durante init: {topic}")
        return

    log.info(f"MQTT RX: {topic} = {payload}")
    try:
        parts    = topic.split("/")
        board_id = parts[1]
        with state_lock:
            info = detected_boards.get(board_id)
        if not info:
            return
        mm = info["machine"]
        aa = info["address"]

        # Comando generico TN234
        if len(parts) > 2 and parts[2] == "cmd":
            last_command_time = time.time()
            tx_queue.put(build_frame("S", mm, aa, payload), timeout=1.0)
            return

        # Comando relè TN267
        if info["type"] == "hybrid" and len(parts) > 2 and "rele" in parts[2]:
            relay_num = int(parts[2].replace("rele", ""))
            key = f"{board_id}_relay_{relay_num}"
            with state_lock:
                stato_attuale = channel_states.get(key, False)
            vuole_on = (payload == "ON")
            if vuole_on != stato_attuale:
                with state_lock:
                    channel_states[key] = vuole_on
                publish_relay_state(board_id, relay_num, vuole_on)
                last_command_time = time.time()
                tx_queue.put(build_frame("S", mm, aa, f"P{relay_num}"), timeout=1.0)
            else:
                log.info(f"  {board_id}/rele{relay_num} già {payload}")
            return

        # Comando canale switch / light (con eventuale burst mode)
        ch  = int(parts[2].replace("canale", ""))
        key = f"{board_id}_{ch}"
        with state_lock:
            stato_attuale = channel_states.get(key, False)
        vuole_on = (payload == "ON")
        ch_modes = info.get("channel_modes", ["T"] * info["channels"])
        mode = ch_modes[ch-1] if ch <= len(ch_modes) else "T"

        if vuole_on != stato_attuale:
            with state_lock:
                channel_states[key] = vuole_on
            publish_state(board_id, ch, vuole_on)
            last_command_time = time.time()

            if mode == "B":
                burst_key = f"{board_id}_{ch}"
                if vuole_on:
                    if burst_key not in burst_active:
                        stop_event = threading.Event()
                        burst_active[burst_key] = stop_event
                        threading.Thread(
                            target=burst_loop,
                            args=(board_id, ch, mm, aa, stop_event),
                            daemon=True,
                        ).start()
                        log.info(f"Burst start {board_id}/canale{ch}")
                else:
                    ev = burst_active.pop(burst_key, None)
                    if ev:
                        ev.set()
            else:
                tx_queue.put(build_frame("S", mm, aa, f"P{ch}"), timeout=1.0)
        else:
            log.info(f"  {board_id}/canale{ch} già {payload}")

    except queue.Full:
        log.error("TX queue piena, comando perso")
    except Exception as e:
        log.exception(f"Errore comando: {e}")

# Compatibilità paho-mqtt v1 e v2
try:
    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
except AttributeError:
    mqtt_client = mqtt.Client()

if MQTT_USER:
    mqtt_client.username_pw_set(MQTT_USER, MQTT_PASS)
mqtt_client.on_connect    = on_connect
mqtt_client.on_disconnect = on_disconnect
mqtt_client.on_message    = on_message
mqtt_client.reconnect_delay_set(min_delay=1, max_delay=30)

log.info(f"Connessione MQTT {MQTT_HOST}:{MQTT_PORT}…")
while running:
    try:
        mqtt_client.connect(MQTT_HOST, MQTT_PORT, 60)
        break
    except Exception as e:
        log.error(f"Connessione MQTT fallita: {e}. Riprovo fra 3 s…")
        time.sleep(3)

mqtt_client.loop_start()
mqtt_ready.wait(timeout=30)
publish_scan_button()

# ─────────────────────────────────────────
# AVVIO
# ─────────────────────────────────────────
boards = load_boards()
if boards:
    detected_boards = boards
    setup_boards(boards)
else:
    log.info("Nessuna scheda salvata, avvio scansione…")
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
log.info("Comandi abilitati tra 5 secondi…")

threading.Thread(target=tx_thread,        daemon=True).start()
threading.Thread(target=rx_thread,        daemon=True).start()
threading.Thread(target=heartbeat_thread, daemon=True).start()

log.info("Tecnonautica bridge avviato.")

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    log.info("Arresto…")
    running = False
    try:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
    except Exception:
        pass
    try:
        if ser:
            ser.close()
    except Exception:
        pass
