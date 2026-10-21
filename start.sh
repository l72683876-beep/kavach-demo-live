#!/bin/sh
kavach -seats 20000 -addr 127.0.0.1:8080 -api-keys appkey:aurora-app -hmac-secret appsecret -hold-ttl 60 -ip-rate 0 -waitlist-cap 100000 -workers 2 &
sleep 1
exec python3 -m gunicorn -w 2 --threads 32 -b 0.0.0.0:"${PORT:-9000}" app:app
