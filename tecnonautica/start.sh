#!/bin/sh
echo "Aggiunta root al gruppo audio..."
addgroup root audio 2>/dev/null || true

# Risolvi il percorso by-id se necessario
PORT=$(python3 -c "
import json
with open('/data/options.json') as f:
    c = json.load(f)
print(c.get('serial_port', '/dev/ttyUSB1'))
")
echo "Porta configurata: $PORT"

# Se è un percorso by-id, trova il device reale
if echo "$PORT" | grep -q "by-id"; then
    REAL=$(readlink -f "$PORT" 2>/dev/null)
    if [ -n "$REAL" ]; then
        echo "Porta reale: $REAL"
    fi
fi

echo "Avvio script Python..."
exec python3 -u /run.py
