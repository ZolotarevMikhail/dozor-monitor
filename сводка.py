#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Утренняя ИИ-сводка «Дозора» — этап 6 плана (ИИ-сводка, УТП).

Раз в сутки, утром:
1. Забирает с VPS по SSH записи проверок за последние 24 часа (по пути
   файла, без повторного мониторинга).
2. Собирает точные факты: сколько проверено, переходы упал/восстановился,
   средняя скорость, текущий статус каждого сайта.
3. Просит Ollama (модель kimi-k3:cloud, как в NewsAgent) написать
   короткий дайджест по-человечески. Ollama не отвечает — приходит
   механическая сводка фактов с честной пометкой.
4. Шлёт сводку в Telegram и письмом (обычно имитация, см. README).
5. Срабатывает «раз в день» — обёртка launchd будит каждые 15 минут,
   метка-файл делает фактический прогон единственным в сутки.

Запуск вручную:  python3 сводка.py
Данные сервера (VPS_USER/VPS_HOST) — в /Users/nadezda/claudecode/.env.
"""
import base64
import json
import os
import subprocess
import sys
import urllib.request
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_ENV = "/Users/nadezda/claudecode/.env"  # там VPS_USER и VPS_HOST
GUARD_FILE = os.path.join(BASE_DIR, "метка-сводки.txt")
MON_MOD = os.path.join(BASE_DIR, "monitor.py")

SSH_TIMEOUT = 20
OLLAMA_URL = "http://localhost:11434/api/chat"
OLLAMA_TIMEOUT = 240
MODEL = "kimi-k3:cloud"  # проверенная русскоязычная модель (NewsAgent)

import importlib.util  # noqa: E402

spec = importlib.util.spec_from_file_location("monitor", MON_MOD)
monitor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(monitor)


def load_root_env():
    """VPS_USER/VPS_HOST из корневого .env (секреты в .env, не в коде)."""
    creds = {}
    with open(ROOT_ENV, "r", encoding="utf-8") as f:
        for row in f:
            row = row.strip()
            if row.startswith(("VPS_USER=", "VPS_HOST=")):
                key, value = row.split("=", 1)
                creds[key] = value.strip()
    return creds.get("VPS_USER", ""), creds.get("VPS_HOST", "")


def wifi_ip():
    """Адрес Wi-Fi-карты Mac (обход VPN-туннеля при SSH, см. память)."""
    return subprocess.run(["ipconfig", "getifaddr", "en0"],
                          capture_output=True, text=True).stdout.strip()


def send_telegram_via_server(env, text):
    """Пересылка сводки в Telegram через VPS (прямой путь с Mac бывает
    закрыт VPN-туннелем; сервер видит Telegram сам). Секреты в тексте
    не печатаются — payload вклеен в скрипт в base64.
    Возвращает True при успехе."""
    user, host = load_root_env()
    payload = base64.b64encode(json.dumps([
        env.get("BOT_TOKEN", ""), env.get("ADMIN_CHAT_ID", ""), text,
    ], ensure_ascii=False).encode("utf-8")).decode("ascii")
    remote = (
        "import json, urllib.request, urllib.parse, base64\n"
        "token, chat_id, text = json.loads(base64.b64decode(\n"
        f"    '{payload}'))\n"
        "req = urllib.request.Request("
        "'https://api.telegram.org/bot' + token + '/sendMessage',"
        " urllib.parse.urlencode({'chat_id': chat_id,"
        " 'text': text}).encode())\n"
        "with urllib.request.urlopen(req, timeout=15) as resp:\n"
        "    resp.read()\n"
    )
    try:
        result = subprocess.run(
            ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10",
             "-b", wifi_ip(), f"{user}@{host}", "python3 -"],
            input=remote, capture_output=True, text=True, timeout=60)
        return result.returncode == 0
    except Exception:
        return False


def fetch_day_from_server():
    """Забирает с VPS записи за 24 ч. Возвращает словарь фактов."""
    user, host = load_root_env()
    wifi = wifi_ip()
    remote_script = '''
import json, sqlite3
from datetime import datetime, timedelta
conn = sqlite3.connect("/root/monitorilka/monitor.db")
cutoff = (datetime.now() - timedelta(hours=24))\\
         .strftime("%Y-%m-%d %H:%M:%S")
rows = list(conn.execute(
    "SELECT ts, name, ok, latency_ms, status_code, error FROM checks"
    " WHERE ts >= ? ORDER BY id", (cutoff,)))
print(json.dumps(rows, ensure_ascii=False))
'''
    result = subprocess.run(
        ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10",
         "-b", wifi, f"{user}@{host}", "python3 - ", ],
        input=remote_script, capture_output=True, text=True,
        timeout=SSH_TIMEOUT)
    if result.returncode != 0:
        raise RuntimeError("SSH не подключился: " + result.stderr[:200])
    rows = json.loads(result.stdout.strip())
    return collect_facts(rows)


def collect_facts(rows):
    """Из сырых строк БД делает факты: счётчики, события, статусы."""
    sites, order, prev_ok = {}, [], {}
    for ts, name, ok, latency, code, error in rows:
        if name not in sites:
            sites[name] = {"events": [], "ok_ms": [], "bad": 0,
                           "total": 0, "last_ok": None, "last_ts": None}
            order.append(name)
            prev_ok[name] = None
        item = sites[name]
        item["total"] += 1
        item["last_ok"], item["last_ts"] = bool(ok), ts
        if ok:
            if latency:
                item["ok_ms"].append(latency)
        else:
            item["bad"] += 1
        if prev_ok[name] is not None and prev_ok[name] != bool(ok):
            word = "восстановился" if ok else "упал"
            item["events"].append(f"{word} — {ts}")
        prev_ok[name] = bool(ok)
    facts = {"total": len(rows), "sites": []}
    for name in order:
        item = sites[name]
        avg = (sum(item["ok_ms"]) // len(item["ok_ms"])
               if item["ok_ms"] else None)
        facts["sites"].append({
            "имя": name,
            "проверок_за_24ч": item["total"],
            "неудачных": item["bad"],
            "средняя_скорость_мс": avg,
            "сейчас": "работает" if item["last_ok"] else "НЕ работает",
            "последняя_проверка": item["last_ts"],
            "события": item["events"],
        })
    facts["событий_падения"] = sum(
        1 for s in facts["sites"] for e in s["события"] if "упал" in e)
    return facts


def ask_ollama(facts):
    """Промпт с фактами -> текст дайджеста от Ollama. None при сбое."""
    prompt = (
        "Ты пишет утренний дайджест мониторинга сайтов для владельца.\n"
        "Правила: по-русски; 3–7 предложений; только факты ниже, ничего\n"
        "не выдумывать и не приукрашивать; цифры можно округлять до\n"
        "целых, но не менять; если падений не было — так и скажи прямо,\n"
        "без слов «как видите», «в целом», «надеюсь».\n"
        "Начни с итога: сколько проверок, сколько падений/восстановлений.\n\n"
        "Факты за последние 24 часа:\n" + json.dumps(
            facts, ensure_ascii=False, indent=1)
    )
    body = json.dumps({
        "model": MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "stream": False,
    }).encode("utf-8")
    try:
        request = urllib.request.Request(
            OLLAMA_URL, data=body,
            headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(request, timeout=OLLAMA_TIMEOUT) as r:
            answer = json.load(r)
        return answer["message"]["content"].strip()
    except Exception:
        return None


def mechanical_digest(facts):
    """Запасной текст из точных фактов, если Ollama не отвечает."""
    rows = [
        f"{s['имя']}: проверок {s['проверок_за_24ч']},"
        f" неудачных {s['неудачных']}, сейчас {s['сейчас']}"
        + (f", средняя скорость {s['средняя_скорость_мс']} мс"
           if s["средняя_скорость_мс"] else "")
        + ("" if not s["события"] else "; события: "
           + "; ".join(s["события"]))
        for s in facts["sites"]
    ]
    return (f"Сводка за 24 часа — ИИ недоступен, механический свод:\n"
            f"проверок всего {facts['total']}, событий падения:"
            f" {facts['событий_падения']}.\n" + "\n".join(rows))


def already_sent_today():
    """Метка «сводка дня уже ушла» (launchd будит часто, шлём раз)."""
    today = datetime.now().strftime("%Y-%m-%d")
    try:
        return open(GUARD_FILE, encoding="utf-8").read().strip() == today
    except OSError:
        return False


def mark_sent_today():
    with open(GUARD_FILE, "w", encoding="utf-8") as f:
        f.write(datetime.now().strftime("%Y-%m-%d"))


def main():
    if already_sent_today():
        print("Сводка за этот день уже отправлялась — тихий выход")
        return 0
    try:
        facts = fetch_day_from_server()
    except Exception as exc:
        print("⚠️ данные с сервера не забрались:", exc)
        return 1
    text = ask_ollama(facts)
    if text:
        head = "🤖 Утренняя сводка (ИИ)\n"
    else:
        text = mechanical_digest(facts)
        head = "📋 Утренняя сводка (ИИ не отвечал — механическое свод)\n"
    # единые каналы отправки из monitor.py: Telegram + email (имитация)
    env = monitor.load_env()
    ok, err = monitor.send_telegram(
        env.get("BOT_TOKEN", ""), env.get("ADMIN_CHAT_ID", ""), head + text)
    if not ok:
        # прямой путь упал (обычно VPN-туннель на Mac) — через сервер
        if send_telegram_via_server(env, head + text):
            ok, err = True, ""
            print("Telegram: ушло (пересылка через сервер)")
        else:
            print("Telegram: ⚠️ не ушёл и через сервер:", err)
    else:
        print("Telegram: ушло")
    ok_email, err_email = monitor.send_email(env, head + text)
    if err_email in ("", "имитация: письмо записано в письма.txt"):
        print("Письмо:", err_email or "отправлено")
    else:
        print("⚠️ письмо:", err_email)
    mark_sent_today()
    return 0


if __name__ == "__main__":
    sys.exit(main())
