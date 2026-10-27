#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Приёмник «стук-сторожа»: ловит пинги роботов и пишет время в базу.

Робот клиента (бот, выгрузка, скрипт) после каждого успешного прогона
делает простой GET-запрос на адрес:

    http://<адрес сервера>:8618/ping/<токен>

Токен — случайный секрет, по нему приёмник понимает, какой робот
стучится; «стук»-объект с этим токеном задан в config.json Дозора.
Если робот перестал стучаться, monitor.py замечает это на следующей
проверке (check_ping) и присылает алерт как при обычном падении.

URL сделан чисто латинским не случайно: curl и многие клиенты не умеют
отправлять русские буквы в адресе запроса. Токен генерируется один раз
(например, python3 -c "import uuid; print(uuid.uuid4().hex)").

Запуск: python3 стук-приёмник.py [порт]   (по умолчанию 8618)
Список роботов читается из config.json один раз при старте — после
изменения списка приёмник надо перезапустить.
"""
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from monitor import db, load_config, now_ts

DEFAULT_PORT = 8618
PING_PREFIX = "/ping/"


def allowed_pings():
    """Словарь разрешённых стуков: токен -> имя робота."""
    out = {}
    for site in load_config()["sites"]:
        if "стук" in site and site["стук"].get("токен"):
            out[site["стук"]["токен"]] = site["name"]
    return out


def make_handler(allowed):
    """Собирает класс-обработчик с доступом к списку токенов.

    allowed — словарь {токен: имя робота}; адрес запроса строго латиница.
    """
    class Handler(BaseHTTPRequestHandler):
        """Отвечает только на /ping/<токен>; остальное — 404."""

        def send_text(self, code, text):
            body = text.encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            parts = [c for c in self.path.split("/") if c]
            token = parts[1] if len(parts) == 2 \
                and parts[0] == PING_PREFIX.strip("/") else None
            if not token or token not in allowed:
                self.send_text(404, "не знаю такой адрес")
                return
            conn = db()
            conn.execute("INSERT INTO pings (ts, name) VALUES (?, ?)",
                         (now_ts(), allowed[token]))
            conn.commit()
            conn.close()
            self.send_text(200, f"стук принят: {allowed[token]}")

        def log_message(self, fmt, *args):
            # короткая строка в журнал: время, адрес, запрос
            print(now_ts(), self.address_string(), fmt % args)

    return Handler


def main():
    """Запуск приёмника на всех адресах (порт задаётся аргументом)."""
    port = int(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_PORT
    allowed = allowed_pings()
    print(f"Приёмник стука запущен на порту {port};"
          f" роботов в списке: {len(allowed)}")
    server = ThreadingHTTPServer(("0.0.0.0", port), make_handler(allowed))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("Приёмник остановлен")


if __name__ == "__main__":
    main()
