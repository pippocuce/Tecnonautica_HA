ARG BUILD_FROM
FROM $BUILD_FROM

RUN apk add --no-cache python3 py3-pip && \
    pip3 install pyserial paho-mqtt --break-system-packages

COPY run.py /run.py
COPY start.sh /start.sh
RUN chmod +x /start.sh

CMD ["/start.sh"]
