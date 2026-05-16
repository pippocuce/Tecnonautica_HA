```python
import serial
import time
import threading
import queue
import re
import json
import signal
import sys

import paho.mqtt.client as mqtt

# ============================================================
# CONFIG
# ============================================================

SERIAL_PORT = "/dev/ttyUSB0"
BAUDRATE = 19200
SERIAL_TIMEOUT = 0.05

MQTT_HOST = "core-mosquitto"
MQTT_PORT = 1883
MQTT_USER = "prova"
MQTT_PASSWORD = "prova"

MQTT_BASE = "tecnonautica"

TX_DELAY = 0.20

# ============================================================
# SCHEDE
# ============================================================

BOARDS = {

    "PM_01": {
        "machine": "PM",
        "address": "01",
        "type": "pm",
        "relays": 6,
        "feedbacks": 6,
        "sensors": 2
    },

    "PM_02": {
        "machine": "PM",
        "address": "02",
        "type": "pm",
        "relays": 6,
        "feedbacks": 6,
        "sensors": 2
    },

    "SP_03": {
        "machine": "SP",
        "address": "03",
        "type": "sp",
        "channels": 10
    },

    "T1_04": {
        "machine": "T1",
        "address": "04",
        "type": "t1",
        "relays": 10,
        "feedbacks": 10
    },

    "SP_06": {
        "machine": "SP",
        "address": "06",
        "type": "sp",
        "channels": 10
    },

    "AL_07": {
        "machine": "AL",
        "address": "07",
        "type": "al",
        "alarms": 16
    },

    "T1_09": {
        "machine": "T1",
        "address": "09",
        "type": "t1",
        "relays": 10,
        "feedbacks": 10
    }
}

# ============================================================
# GLOBALS
# ============================================================

running = True

tx_queue = queue.Queue()

last_states = {}
last_seen = {}

serial_lock = threading.Lock()

mqtt_ready = threading.Event()

# ============================================================
# SERIAL
# ============================================================

ser = serial.Serial(
    SERIAL_PORT,
    BAUDRATE,
    timeout=SERIAL_TIMEOUT
)

# ============================================================
# CHECKSUM
# ============================================================

def checksum(data):

    c = 0

    for ch in data:
        c ^= ord(ch)

    return f"{c:02X}"

# ============================================================
# BUILD FRAME
# ============================================================

def build_frame(msg_type, mm, aa, payload):

    body = f"{msg_type}{mm}{aa}{payload}KK"

    cs = checksum(body)

    return f"[{body}*{cs}]"

# ============================================================
# MQTT
# ============================================================

mqtt_client = mqtt.Client(
    mqtt.CallbackAPIVersion.VERSION2
)

mqtt_client.username_pw_set(
    MQTT_USER,
    MQTT_PASSWORD
)

# ============================================================
# MQTT PUBLISH
# ============================================================

def mqtt_publish(topic, payload, retain=True):

    mqtt_client.publish(
        topic,
        payload,
        qos=0,
        retain=retain
    )

# ============================================================
# MQTT DISCOVERY
# ============================================================

def publish_discovery():

    for board_id, info in BOARDS.items():

        # RELAY
        if info["type"] in ["pm", "t1"]:

            channels = info["relays"]

            for ch in range(1, channels + 1):

                uid = f"{board_id}_relay_{ch}"

                topic = f"homeassistant/switch/{uid}/config"

                payload = {
                    "name": f"{board_id} Relay {ch}",
                    "uniq_id": uid,
                    "cmd_t": f"{MQTT_BASE}/{board_id}/relay{ch}/set",
                    "stat_t": f"{MQTT_BASE}/{board_id}/relay{ch}/state"
                }

                mqtt_publish(
                    topic,
                    json.dumps(payload)
                )

        # FEEDBACK
        if info["type"] in ["pm", "t1"]:

            channels = info["feedbacks"]

            for ch in range(1, channels + 1):

                uid = f"{board_id}_feedback_{ch}"

                topic = f"homeassistant/binary_sensor/{uid}/config"

                payload = {
                    "name": f"{board_id} Feedback {ch}",
                    "uniq_id": uid,
                    "stat_t": f"{MQTT_BASE}/{board_id}/feedback{ch}/state",
                    "pl_on": "ON",
                    "pl_off": "OFF"
                }

                mqtt_publish(
                    topic,
                    json.dumps(payload)
                )

        # STATUS
        if info["type"] == "sp":

            channels = info["channels"]

            for ch in range(1, channels + 1):

                uid = f"{board_id}_status_{ch}"

                topic = f"homeassistant/binary_sensor/{uid}/config"

                payload = {
                    "name": f"{board_id} Status {ch}",
                    "uniq_id": uid,
                    "stat_t": f"{MQTT_BASE}/{board_id}/status{ch}/state",
                    "pl_on": "ON",
                    "pl_off": "OFF"
                }

                mqtt_publish(
                    topic,
                    json.dumps(payload)
                )

        # SENSORI ANALOGICI
        if info["type"] == "pm":

            for s in range(1, 3):

                uid = f"{board_id}_sensor_{s}"

                topic = f"homeassistant/sensor/{uid}/config"

                payload = {
                    "name": f"{board_id} Sensor {s}",
                    "uniq_id": uid,
                    "stat_t": f"{MQTT_BASE}/{board_id}/sensor{s}/state"
                }

                mqtt_publish(
                    topic,
                    json.dumps(payload)
                )

        # ALLARMI
        if info["type"] == "al":

            for a in range(1, 17):

                uid = f"{board_id}_alarm_{a}"

                topic = f"homeassistant/binary_sensor/{uid}/config"

                payload = {
                    "name": f"{board_id} Alarm {a}",
                    "uniq_id": uid,
                    "stat_t": f"{MQTT_BASE}/{board_id}/alarm{a}/state",
                    "pl_on": "ON",
                    "pl_off": "OFF"
                }

                mqtt_publish(
                    topic,
                    json.dumps(payload)
                )

# ============================================================
# MQTT CONNECT
# ============================================================

def on_connect(client, userdata, flags, reason_code, properties):

    print("MQTT connected", flush=True)

    mqtt_ready.set()

    client.subscribe(f"{MQTT_BASE}/+/+/set")

    publish_discovery()

# ============================================================
# MQTT MESSAGE
# ============================================================

def on_message(client, userdata, msg):

    topic = msg.topic

    payload = msg.payload.decode().strip().upper()

    print(f"MQTT RX: {topic} = {payload}", flush=True)

    parts = topic.split("/")

    if len(parts) < 4:
        return

    board_id = parts[1]
    entity = parts[2]

    if board_id not in BOARDS:
        return

    info = BOARDS[board_id]

    mm = info["machine"]
    aa = info["address"]

    m = re.match(r"relay(\d+)", entity)

    if m:

        ch = int(m.group(1))

        if ch == 10:
            key = "0"
        else:
            key = str(ch)

        if payload == "ON":

            tx_queue.put(
                build_frame("S", mm, aa, f"P{key}")
            )

        elif payload == "OFF":

            tx_queue.put(
                build_frame("S", mm, aa, f"R{key}")
            )

        # refresh stato
        tx_queue.put(
            build_frame("Q", mm, aa, "ST")
        )

# ============================================================
# MQTT SETUP
# ============================================================

mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

mqtt_client.connect(
    MQTT_HOST,
    MQTT_PORT,
    60
)

mqtt_client.loop_start()

# ============================================================
# TX THREAD
# ============================================================

def tx_thread():

    while running:

        try:

            frame = tx_queue.get(timeout=0.1)

            with serial_lock:

                ser.write(
                    frame.encode("ascii")
                )

            print(f"TX: {frame}", flush=True)

            tx_queue.task_done()

            # IMPORTANTISSIMO:
            # protocollo richiede >180ms
            time.sleep(TX_DELAY)

        except queue.Empty:
            pass

        except Exception as e:

            print(f"TX error: {e}", flush=True)

            time.sleep(1)

# ============================================================
# PARSE ST
# ============================================================

def parse_st(board_id, states, prefix):

    for i, state in enumerate(states):

        ch = i + 1

        value = "ON" if state == "1" else "OFF"

        topic = f"{MQTT_BASE}/{board_id}/{prefix}{ch}/state"

        old = last_states.get(topic)

        if old != value:

            last_states[topic] = value

            mqtt_publish(topic, value)

            print(
                f"HA <- {board_id}/{prefix}{ch} = {value}",
                flush=True
            )

# ============================================================
# PARSE ME
# ============================================================

def parse_me(board_id, frame):

    match_a = re.search(
        r"A([+-]\d{5})",
        frame
    )

    match_b = re.search(
        r"B([+-]\d{5})",
        frame
    )

    if match_a:

        mqtt_publish(
            f"{MQTT_BASE}/{board_id}/sensor1/state",
            match_a.group(1)
        )

    if match_b:

        mqtt_publish(
            f"{MQTT_BASE}/{board_id}/sensor2/state",
            match_b.group(1)
        )

# ============================================================
# PARSE ALARMS
# ============================================================

def parse_alarms(board_id, alarms):

    for i, a in enumerate(alarms):

        ch = i + 1

        active = a in ["A", "C"]

        value = "ON" if active else "OFF"

        topic = f"{MQTT_BASE}/{board_id}/alarm{ch}/state"

        old = last_states.get(topic)

        if old != value:

            last_states[topic] = value

            mqtt_publish(topic, value)

            print(
                f"HA <- {board_id}/alarm{ch} = {value}",
                flush=True
            )

# ============================================================
# PARSE FRAME
# ============================================================

def parse_frame(frame):

    print(f"RX: {frame}", flush=True)

    for board_id, info in BOARDS.items():

        mm = info["machine"]
        aa = info["address"]

        sig = f"{mm}{aa}"

        if sig not in frame:
            continue

        last_seen[board_id] = time.time()

        # PM/T1 ST
        m = re.search(r"ST([01]{6,10})", frame)

        if m:

            if info["type"] in ["pm", "t1"]:

                parse_st(
                    board_id,
                    m.group(1),
                    "relay"
                )

            elif info["type"] == "sp":

                parse_st(
                    board_id,
                    m.group(1),
                    "status"
                )

        # FEEDBACK
        m = re.search(r"FB([01]{6,10})", frame)

        if m:

            parse_st(
                board_id,
                m.group(1),
                "feedback"
            )

        # ANALOG
        if "ME" in frame:

            parse_me(
                board_id,
                frame
            )

        # ALARMS
        m = re.search(r"AS([DACRN]{16})", frame)

        if m:

            parse_alarms(
                board_id,
                m.group(1)
            )

        return

# ============================================================
# RX THREAD
# ============================================================

def rx_thread():

    buffer = ""

    while running:

        try:

            data = ser.read(256)

            if not data:
                continue

            chunk = data.decode(
                "ascii",
                errors="ignore"
            )

            buffer += chunk

            if len(buffer) > 4096:
                buffer = buffer[-1024:]

            while True:

                start = buffer.find("[")

                if start == -1:

                    buffer = ""

                    break

                end = buffer.find("]", start)

                if end == -1:

                    buffer = buffer[start:]

                    break

                frame = buffer[start:end + 1]

                buffer = buffer[end + 1:]

                # recovery frame concatenati
                if frame.count("[") > 1:

                    last = frame.rfind("[")

                    buffer = frame[last:] + buffer

                    continue

                if "*" not in frame:
                    continue

                parse_frame(frame)

        except Exception as e:

            print(f"RX error: {e}", flush=True)

            time.sleep(1)

# ============================================================
# HEARTBEAT
# ============================================================

def heartbeat_thread():

    mqtt_ready.wait()

    time.sleep(2)

    print("Heartbeat started", flush=True)

    cycle = 0

    poll_list = []

    while running:

        try:

            poll_list.clear()

            for board_id, info in BOARDS.items():

                mm = info["machine"]
                aa = info["address"]

                # stato principale
                poll_list.append(
                    build_frame(
                        "Q",
                        mm,
                        aa,
                        "ST"
                    )
                )

                # feedback
                if info["type"] in ["pm", "t1"]:

                    if cycle % 3 == 0:

                        poll_list.append(
                            build_frame(
                                "Q",
                                mm,
                                aa,
                                "FB"
                            )
                        )

                # analogici
                if info["type"] == "pm":

                    if cycle % 5 == 0:

                        poll_list.append(
                            build_frame(
                                "Q",
                                mm,
                                aa,
                                "ME"
                            )
                        )

                # allarmi
                if info["type"] == "al":

                    poll_list.append(
                        build_frame(
                            "Q",
                            mm,
                            aa,
                            "AS"
                        )
                    )

                    if cycle % 5 == 0:

                        poll_list.append(
                            build_frame(
                                "Q",
                                mm,
                                aa,
                                "LS"
                            )
                        )

            for frame in poll_list:

                tx_queue.put(frame)

            cycle += 1

            # IMPORTANTISSIMO:
            # tutte le schede <5 sec
            time.sleep(3)

        except Exception as e:

            print(f"Heartbeat error: {e}", flush=True)

            time.sleep(2)

# ============================================================
# WATCHDOG
# ============================================================

def watchdog_thread():

    while running:

        now = time.time()

        for board_id in BOARDS:

            last = last_seen.get(board_id)

            if last is None:
                continue

            delta = now - last

            if delta > 10:

                print(
                    f"WARNING: {board_id} offline",
                    flush=True
                )

        time.sleep(5)

# ============================================================
# SHUTDOWN
# ============================================================

def shutdown(sig=None, frame=None):

    global running

    print("Shutdown...", flush=True)

    running = False

    try:
        mqtt_client.loop_stop()
    except:
        pass

    try:
        mqtt_client.disconnect()
    except:
        pass

    try:
        ser.close()
    except:
        pass

    sys.exit(0)

# ============================================================
# SIGNALS
# ============================================================

signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

# ============================================================
# START THREADS
# ============================================================

threading.Thread(
    target=tx_thread,
    daemon=True
).start()

threading.Thread(
    target=rx_thread,
    daemon=True
).start()

threading.Thread(
    target=heartbeat_thread,
    daemon=True
).start()

threading.Thread(
    target=watchdog_thread,
    daemon=True
).start()

print("Bridge Tecnonautica avviato", flush=True)

# ============================================================
# MAIN LOOP
# ============================================================

while running:

    time.sleep(1)
```
