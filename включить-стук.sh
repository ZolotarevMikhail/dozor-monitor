#!/bin/bash
# ВКЛЮЧИТЬ СЛУЖБУ СТУК-ПРИЁМНИКА НА VPS (одноразовый шаг, рука Михаила)
# Агенту включение служб перехватывает защита — поэтому команда здесь:
# запускать в Терминале через Claude Code:  ! bash ".../включить-стук.sh"
set -a; . /Users/nadezda/claudecode/.env; set +a
WIFI=$(ipconfig getifaddr en0)
ssh -b "$WIFI" -o BatchMode=yes -o ConnectTimeout=15 "$VPS_USER@$VPS_HOST" '
systemctl daemon-reload && systemctl enable --now stuk.service && sleep 1
echo "служба stuk.service: $(systemctl is-active stuk.service)"
echo "порт 8618 слушает:"; ss -tlnp | grep 8618
echo "пробный стук:"; curl -s http://127.0.0.1:8618/ping/probnyi-token; echo'