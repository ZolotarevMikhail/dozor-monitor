#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Дозор — робот-проверяльщик сайтов и сторож роботов.

Что делает за один прогон (check):
1. Проверяет сайты из config.json (HTTP-код + время ответа + слово в теле)
   и сторожевые точки «стук»: там мониторится не сайт, а чужой робот —
   бот, выгрузка, скрипт; они после успешного прогона стучатся на
   приёмник (стук-приёмник.py), а здесь смотрим, не замолчал ли робот.
2. Пишет результат в историю SQLite (monitor.db).
3. При смене статуса шлёт алерт в Telegram: «упал» после 2 подряд
   неудачных проверок, «восстановился» — после 2 подряд удачных.
4. Перерисовывает страницу статусов (статусы.html) — открывается
   двойным кликом в браузере.

Другие команды:
    python3 monitor.py страница  — только перерисовать статусы.html
    python3 monitor.py история   — последние 20 записей базы

Секреты (токен бота и id чата) лежат в .env рядом со скриптом.
Зависимость одна: certifi — актуальный набор корневых сертификатов
(стандартный Python на Mac не видит системные, из-за этого без certifi
любой https-запрос падает с «CERTIFICATE_VERIFY_FAILED»).
"""
import fcntl  # замок прогона: доступен и на Mac, и на Linux-сервере
import json
import os
import smtplib
import sqlite3
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from email.mime.text import MIMEText

import certifi

# --- пути: всё лежит рядом со скриптом ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(BASE_DIR, "config.json")
DB_FILE = os.path.join(BASE_DIR, "monitor.db")
PAGE_FILE = os.path.join(BASE_DIR, "статусы.html")
ENV_FILE = os.path.join(BASE_DIR, ".env")

TEST_URL = "https://api.telegram.org"  # для проверки токена алертов

# количество попыток проверки сайта и пауза между ними, секунд
TRIES = 2
RETRY_PAUSE = 5
TIMEOUT = 15
# количество последних проверок, показанных на странице статусов
DOTS = 12
# анти-дрожь: «упал» шлётся только после N подряд неудачных проверок,
# «восстановился» — после N подряд удачных (провал сети на миг — не алерт)
FAIL_CONFIRM = 2
RECOVER_CONFIRM = 2
# сколько сайтов проверяем одновременно (больше — быстрее весь прогон)
WORKERS = 8
# записи истории старше этого срока удаляются раз в сутки (база не жиреет)
RETENTION_DAYS = 90
# если последняя проверка старше такого числа минут, страница помечает
# данные устаревшими (робот мог умереть — видно сразу)
STALE_MINUTES = 30
# файл-замок: не даёт двум прогонам наложиться (крон запускается поверх)
RUN_LOCK_FILE = os.path.join(BASE_DIR, "прогон.lock")

# HTTPS-контекст с настоящим набором корневых сертификатов (certifi):
# один для всех https-запросов и smtp-соединений скрипта
SSL_CTX = ssl.create_default_context(cafile=certifi.where())


def load_env():
    """Читает .env простым способом: СТРОКА=ЗНАЧЕНИЕ, возвращает словарь."""
    env = {}
    if not os.path.isfile(ENV_FILE):
        return env
    with open(ENV_FILE, "r", encoding="utf-8") as f:
        for row in f:
            row = row.strip()
            if not row or row.startswith("#") or "=" not in row:
                continue
            key, value = row.split("=", 1)
            env[key.strip()] = value.strip().strip('"').strip("'")
    return env


def load_config():
    """Читает config.json целиком (sites, тихие_часы и другие поля)."""
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    if not cfg.get("sites"):
        raise SystemExit("config.json не содержит сайтов для проверки")
    return cfg


def check_site(site):
    """Проверяет один сайт. Возвращает словарь с результатом проверки.

    Словарь: ok, status_code, latency_ms, error, note.
    ok=False, если сеть/сервер не ответили, код не 2xx/3xx
    или заданное keyword-слово не найдено в теле страницы.
    """
    url = site["url"]
    keyword = site.get("keyword")
    result = {
        "ok": False,
        "status_code": None,
        "latency_ms": None,
        "error": None,
        "note": "",
    }
    for attempt in range(1, TRIES + 1):
        started = time.monotonic()
        try:
            request = urllib.request.Request(
                url,
                headers={"User-Agent": "monitor-ilka/1 (personal check)"},
            )
            with urllib.request.urlopen(request, timeout=TIMEOUT,
                                        context=SSL_CTX) as resp:
                body = resp.read(200000).decode("utf-8", "replace")
                result["status_code"] = resp.status
        except (urllib.error.HTTPError, urllib.error.URLError,
                TimeoutError, ConnectionError, OSError) as exc:
            reason = getattr(exc, "reason", None) or exc
            result["error"] = f"{type(exc).__name__}: {reason}"
            result["note"] = f"попытка {attempt}/{TRIES}"
        else:
            elapsed_ms = int((time.monotonic() - started) * 1000)
            result["latency_ms"] = elapsed_ms
            if result["status_code"] >= 400:
                result["error"] = f"код {result['status_code']}"
            elif keyword and keyword.lower() not in body.lower():
                result["error"] = "страница открылась, но в ней нет слова" \
                                  f" «{keyword}»"
            else:
                result["ok"] = True
            break
        if attempt < TRIES:
            time.sleep(RETRY_PAUSE)
    return result


def now_ts():
    """Текущее время в едином формате для базы и страницы."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def check_ping(conn, site):
    """Сторож-проверка «стук»: следит, что чужой робот стучится вовремя.

    В config.json у такого объекта вместо «url» есть поле «стук»:
        "стук": {"токен": "...", "не_тише_минут": 30}
    Робот после успешного прогона делает GET-запрос на приёмник
    (стук-приёмник.py), время стука пишется в таблицу pings этой же базы.
    Робот считается работающим, если последний стук был не позже
    «не_тише_минут» назад. Возвращает словарь как check_site, при этом
    latency_ms хранит минуты молчания (у стука нет «скорости ответа»).
    """
    limit_min = int(site["стук"]["не_тише_минут"])
    result = {
        "ok": False,
        "status_code": None,
        "latency_ms": None,
        "error": None,
        "note": f"ожидаю стук не реже 1 раза в {limit_min} мин",
    }
    row = conn.execute(
        "SELECT ts FROM pings WHERE name = ? ORDER BY id DESC LIMIT 1",
        (site["name"],)).fetchone()
    if not row:
        result["error"] = "стука ещё не было ни разу"
        return result
    last_ts = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S")
    silence_min = int((datetime.now() - last_ts).total_seconds() // 60)
    if silence_min > limit_min:
        result["error"] = f"робот молчит {silence_min} мин"
    else:
        result["ok"] = True
        result["latency_ms"] = silence_min
    return result


def db():
    """Открывает базу и создаёт таблицы, если их ещё нет.

    Три настройки надёжности, рецепт стандартный [проверено ресёрчем]:
    busy_timeout (ждать освобождения базы) ДО включения WAL, режим WAL
    (крон-робот и приёмник стука пишут в одну базу не мешая друг другу),
    и немедленная блокировка на запись (BEGIN IMMEDIATE сразу, а не в
    конце операции — иначе ловится ошибка «database is locked»).
    """
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA busy_timeout = 30000")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.isolation_level = "IMMEDIATE"
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS checks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            name TEXT NOT NULL,
            url TEXT NOT NULL,
            ok INTEGER NOT NULL,
            status_code INTEGER,
            latency_ms INTEGER,
            error TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS state (
            name TEXT PRIMARY KEY,
            was_ok INTEGER NOT NULL,
            since TEXT NOT NULL
        )
        """
    )
    # отложенные ночные алерты: копятся в тихие часы, уходят утром
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS postponed (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            text TEXT NOT NULL
        )
        """
    )
    # время «стука» чужих роботов: пишет стук-приёмник.py, читает
    # check_ping на стороне Дозора
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            name TEXT NOT NULL
        )
        """
    )
    # серии подряд идущих исходов: «упал» только после N подряд неудач
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS streaks (
            name TEXT PRIMARY KEY,
            fails INTEGER NOT NULL,
            goods INTEGER NOT NULL
        )
        """
    )
    # служебные пометки (день последней чистки истории и т.п.)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """
    )
    return conn


def cleanup_old(conn):
    """Раз в сутки стирает записи истории старше RETENTION_DAYS.

    Без чистки база бесконечно растёт (при 100 объектах — десятки тысяч
    строк в день). День последней чистки хранится в таблице meta, поэтому
    лишние удаления не делаются: на обычном прогоне проверка стоит дёшево.
    """
    today = now_ts()[:10]
    row = conn.execute(
        "SELECT value FROM meta WHERE key = 'чистка'").fetchone()
    if row and row[0] == today:
        return
    edge = (datetime.now() - timedelta(days=RETENTION_DAYS)
            ).strftime("%Y-%m-%d %H:%M:%S")
    for table in ("checks", "pings", "postponed"):
        conn.execute(f"DELETE FROM {table} WHERE ts < ?", (edge,))
    conn.execute(
        "INSERT OR REPLACE INTO meta (key, value) VALUES ('чистка', ?)",
        (today,))
    conn.commit()
    print("Чистка истории: всё старше", RETENTION_DAYS, "дней удалено")


def in_quiet_hours(cfg, ts):
    """Истина, если время ts попадает в тихие часы из config.json.

    Поле «тихие_часы» — список из двух строк, например
    ["23:00", "08:00"]; интервал может пересекать полночь.
    Поля нет — тихих часов нет, алерты всегда.
    """
    span = cfg.get("тихие_часы") or []
    if len(span) != 2:
        return False
    hh_mm = ts[11:16]
    start, end = span[0].strip(), span[1].strip()
    if start <= end:
        return start <= hh_mm < end
    return hh_mm >= start or hh_mm < end


def save_check(conn, site, result, ts, url=None):
    """Записывает одну проверку в историю (url — метка в истории)."""
    if url is None:
        url = site["url"]
    conn.execute(
        "INSERT INTO checks (ts, name, url, ok, status_code, latency_ms,"
        " error) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (ts, site["name"], url, int(result["ok"]),
         result["status_code"], result["latency_ms"], result["error"]),
    )


def read_state(conn):
    """Словарь прежних статусов: имя сайта -> was_ok (0/1)."""
    return {name: was_ok for name, was_ok in conn.execute(
        "SELECT name, was_ok FROM state")}


def read_streaks(conn):
    """Серии исходов: имя сайта -> [подряд неудач, подряд удачных]."""
    return {name: [fails, goods] for name, fails, goods in conn.execute(
        "SELECT name, fails, goods FROM streaks")}


def acquire_run_lock():
    """Открывает файл-замок прогона. Вернёт ручку или None (уже занят).

    Замок держится, пока открыта ручка; закрытие ручки снимает его даже
    при падении скрипта — «висящий» замок невозможен по устройству.
    """
    handle = open(RUN_LOCK_FILE, "w")
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        handle.close()
        return None
    return handle


def send_telegram(token, chat_id, text):
    """Отправляет алерт в Telegram. Возвращает (успех, текст ошибки)."""
    if not token or not chat_id:
        return False, "в .env нет BOT_TOKEN или ADMIN_CHAT_ID"
    api_url = TEST_URL + f"/bot{token}/sendMessage"
    payload = urllib.parse.urlencode(
        {"chat_id": chat_id, "text": text}).encode("utf-8")
    try:
        request = urllib.request.Request(
            api_url,
            data=payload,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        with urllib.request.urlopen(request, timeout=TIMEOUT,
                                    context=SSL_CTX) as resp:
            resp.read()
        return True, ""
    except (urllib.error.HTTPError, urllib.error.URLError,
            TimeoutError, ConnectionError, OSError) as exc:
        return False, str(exc)


def describe(site, result):
    """Короткий человекочитаемый итог одной проверки для алерта."""
    if result["ok"]:
        if "стук" in site:
            return (f"🟢 {site['name']} — стучался"
                    f" {result['latency_ms']} мин назад")
        return f"🟢 {site['name']} — работает, {result['latency_ms']} мс"
    detail = result["error"] or "без ответа"
    note = f" ({result['note']})" if result["note"] else ""
    return f"🔴 {site['name']} — НЕ работает: {detail}{note}"


SIMULATED_EMAIL_FILE = os.path.join(BASE_DIR, "письма.txt")


def send_email(env, text):
    """Отправляет сводку письмом. Три режима:

    1. В .env есть SMTP_HOST/SMTP_USER/SMTP_PASS/EMAIL_TO — настоящее
       письмо по SMTP (SMTP_SSL, порт по умолчанию 465).
    2. Есть только EMAIL_TO — ИМИТАЦИЯ: письмо с полными полями дописывается
       в письма.txt, почта Михаила не засоряется.
    3. Нет EMAIL_TO — канал выключен (это не ошибка).
    Возвращает (успех, пояснение).
    """
    host = env.get("SMTP_HOST", "")
    user = env.get("SMTP_USER", "")
    password = env.get("SMTP_PASS", "")
    to = env.get("EMAIL_TO", "")
    if not to:
        return True, "email-канал выключен (в .env нет EMAIL_TO)"
    message = MIMEText(text, "plain", "utf-8")
    message["From"] = user or "Дозор"
    message["To"] = to
    message["Subject"] = "Дозор: отчёт о сайтах"
    if not (host and user and password):
        with open(SIMULATED_EMAIL_FILE, "a", encoding="utf-8") as f:
            f.write("=" * 44 + "\n" + message.as_string() + "\n")
        return True, "имитация: письмо записано в письма.txt"
    port = int(env.get("SMTP_PORT", "465"))
    try:
        if port == 465:
            smtp = smtplib.SMTP_SSL(host, port, context=SSL_CTX,
                                    timeout=TIMEOUT)
        else:
            # например iCloud-почта (me.com): порт 587 и STARTTLS
            smtp = smtplib.SMTP(host, port, timeout=TIMEOUT)
            smtp.starttls(context=SSL_CTX)
        with smtp:
            smtp.login(user, password)
            smtp.sendmail(user, [to], message.as_string())
        return True, ""
    except (smtplib.SMTPException, TimeoutError, ConnectionError,
            OSError) as exc:
        return False, f"{type(exc).__name__}: {exc}"


def check_all(conn, sites):
    """Проверяет все объекты, возвращает результаты в порядке sites.

    Сайты идут ПАРАЛЛЕЛЬНО — пул из WORKERS потоков: один медленный сайт
    больше не задерживает весь прогон (стандартный приём всех чекеров).
    Стук-объекты проверяются в основном потоке: это чтение из базы,
    сеть не дёргается.
    """
    network = [site for site in sites if "стук" not in site]
    results = {}
    if network:
        with ThreadPoolExecutor(
                max_workers=min(WORKERS, len(network))) as pool:
            futures = {site["name"]: pool.submit(check_site, site)
                       for site in network}
            for name, future in futures.items():
                results[name] = future.result()
    out = []
    for site in sites:
        if "стук" in site:
            out.append(check_ping(conn, site))
        else:
            out.append(results[site["name"]])
    return out


def run_checks():
    """Полный прогон: проверки → база → уведомления (Telegram + письмо,
    с тихими часами) → страница статусов."""
    env = load_env()
    cfg = load_config()
    sites = cfg["sites"]
    conn = db()
    cleanup_old(conn)
    state = read_state(conn)
    streaks = read_streaks(conn)
    transitions = {}  # имя -> новый статус (пишется в state после цикла)
    ts = now_ts()
    quiet = in_quiet_hours(cfg, ts)
    fresh, lines = [], []  # новые сайты и строки сводки для уведомлений
    results = check_all(conn, sites)
    for site, result in zip(sites, results):
        is_ping = "стук" in site
        # у сторожевых точек в истории нет обычного адреса сайта;
        # токен стука — секрет, в историю и на страницу не пишется
        save_check(conn, site, result, ts,
                   url=site.get("url") or "стук (адрес: /ping/<токен>)")
        name = site["name"]
        was_ok = state.get(name)
        if was_ok is None:
            fresh.append((site, result))
            continue
        fails, goods = streaks.get(name, (0, 0))
        if result["ok"]:
            fails = 0
            goods += 1
        else:
            fails += 1
            goods = 0
        # анти-дрожь: алерт только после N ПОДРЯД подтверждающих исходов
        if result["ok"] and not was_ok and goods >= RECOVER_CONFIRM:
            if is_ping:
                lines.append(f"🟢 {name} — стук возобновился")
            else:
                lines.append(f"🟢 {name} — восстановился"
                             f" ({result['latency_ms']} мс)")
            transitions[name] = 1
        elif not result["ok"] and was_ok and fails >= FAIL_CONFIRM:
            lines.append(f"🔴 {name} — упал: {result['error']}")
            transitions[name] = 0
        streaks[name] = (fails, goods)
    for site, result in fresh:
        # новый объект сразу получает статус, иначе на каждом прогоне
        # считался бы «новым» и сводка стартовых оповещений бы повторялась
        transitions[site["name"]] = int(result["ok"])
    for name, (fails, goods) in streaks.items():
        conn.execute(
            "INSERT OR REPLACE INTO streaks (name, fails, goods)"
            " VALUES (?, ?, ?)", (name, fails, goods))
    for name, was_ok in transitions.items():
        conn.execute(
            "INSERT OR REPLACE INTO state (name, was_ok, since)"
            " VALUES (?, ?, ?)", (name, was_ok, ts))
    if fresh:
        good = sum(1 for _, r in fresh if r["ok"])
        lines.insert(0, f"Начал мониторить {len(fresh)} сайт(ов):"
                        f" 🟢 {good}, 🔴 {len(fresh) - good}")
        for site, result in fresh:
            lines.append(describe(site, result))
    conn.commit()
    # любые уведомления сначала копятся в базе (таблица postponed):
    # так сообщение не теряется, если Telegram/email временно не отвечают,
    # и ночью ничего не отправляется
    if lines:
        for row in lines:
            conn.execute("INSERT INTO postponed (ts, text) VALUES (?, ?)",
                         (ts, row))
        conn.commit()
    pending = list(conn.execute(
        "SELECT ts, text FROM postponed ORDER BY id"))
    if pending and not quiet:
        text = now_ts() + "\nУведомления:\n" + "\n".join(
            f"• {row_text} ({row_ts})" for row_ts, row_text in pending)
        ok_tg, err_tg = send_telegram(
            env.get("BOT_TOKEN", ""), env.get("ADMIN_CHAT_ID", ""), text)
        if ok_tg:
            print("Уведомления отправлены в Telegram")
        else:
            print("⚠️ уведомления в Telegram не отправились:", err_tg)
        ok_email, err_email = send_email(env, text)
        if ok_email:
            if env.get("SMTP_HOST"):
                print("Письмо отправлено:", env.get("EMAIL_TO", ""))
            elif env.get("EMAIL_TO"):
                print("Письмо сымитировано -> письма.txt"
                      f" (кому: {env.get('EMAIL_TO', '')})")
            # EMAIL_TO нет — email-канал выключен, это не ошибка
        else:
            print("⚠️ письмо не отправилось:", err_email)
        if ok_tg or ok_email:
            # отчитанное отправляем заново, пока хоть один канал не
            # сработал; иначе строки останутся для следующего прогона
            conn.execute("DELETE FROM postponed")
            conn.commit()
    elif pending and quiet:
        print(f"Тихие часы: копится {len(pending)} уведомлений")
    conn.close()
    build_page()
    print("Прогон завершён:", ts, "| сайтов:", len(sites),
          "| тихие часы:", "да" if quiet else "нет")


def build_page():
    """Перерисовывает статусы.html из базы. Работает и как cli 'страница'."""
    if not os.path.isfile(DB_FILE):
        raise SystemExit("базы ещё нет — сперва запусти: python3 monitor.py"
                         " check")
    conn = db()
    # крайняя проверка каждого сайта
    last = {}
    for row in conn.execute(
            "SELECT name, url, ts, ok, status_code, latency_ms, error"
            " FROM checks ORDER BY id"):
        last[row[0]] = row[1:]
    # метка «данные устарели»: если прогонов давно не было, значит сам
    # робот-проверяльщик не дышит — это видно сразу, а не по цифрам
    stale_html = ""
    if last:
        latest = max(row[1] for row in last.values())
        age_min = int((datetime.now() - datetime.strptime(
            latest, "%Y-%m-%d %H:%M:%S")).total_seconds() // 60)
        if age_min > STALE_MINUTES:
            stale_html = (
                '<div class="stale">⛔️ Данные устарели: последний прогон'
                f" был {age_min} мин назад — проверьте, жив ли"
                " робот-проверяльщик</div>")
    # последние DOTS проверок, история 90 дней и суточное время работы
    dots, days90, uptime = {}, {}, {}
    day_ago = (datetime.now() - timedelta(hours=24)
               ).strftime("%Y-%m-%d %H:%M:%S")
    for name in last:
        rows = list(conn.execute(
            "SELECT ok FROM checks WHERE name = ? ORDER BY id DESC"
            " LIMIT ?", (name, DOTS)))
        dots[name] = "".join("🟢" if r[0] else "🔴" for r in reversed(rows))
        total, good = conn.execute(
            "SELECT COUNT(*), SUM(ok) FROM checks WHERE name = ? AND"
            " ts >= ?", (name, day_ago)).fetchone()
        uptime[name] = "нет данных за 24 ч" if not total else (
            f"{good * 100 // total}% ({good} из {total} проверок)")
        # истории 90 дней: одна клетка = один день
        # (зелёный — всё работало; рыжий — было падение; красный —
        # вообще не работал; серый — проверок в этот день не было)
        since = (datetime.now() - timedelta(days=90)
                 ).strftime("%Y-%m-%d")
        days = {}
        for day, total_d, good_d in conn.execute(
                "SELECT substr(ts, 1, 10), COUNT(*), SUM(ok) FROM checks"
                " WHERE name = ? AND substr(ts, 1, 10) >= ?"
                " GROUP BY substr(ts, 1, 10)", (name, since)):
            days[day] = (total_d, good_d)
        days90[name] = days
    conn.close()
    cards = []
    for name, (url, ts_v, ok, code, latency, error) in last.items():
        status = "🟢 Работает" if ok else "🔴 Не работает"
        if ok:
            # у сторожа latency = минуты молчания; статус-кода нет
            detail = ("стук " + str(latency) + " мин назад" if code is None
                      else f"код {code}, {latency} мс")
        else:
            detail = error or "без ответа"
        cls = "ok" if ok else "bad"
        # настоящий адрес — ссылкой; метка стука («/стук/…») — текстом
        # (адрес стука настоящих роботов — секрет и в историю не пишется)
        url_html = (f'<a href="{url}">{url}</a>'
                    if str(url).startswith("http") else url)
        cards.append(CARD.format(
            name=name, cls=cls, url_html=url_html, status=status,
            detail=detail, ts=ts_v, dots=dots.get(name, ""),
            days=days90_html(days90[name], name),
            uptime=uptime.get(name, "")))
    html = (PAGE_HEADER
            .replace("@@CARDS@@", "\n".join(cards))
            .replace("@@STALE@@", stale_html)
            .replace("@@TS@@", now_ts())
            .replace("@@SITES@@", str(len(last))))
    with open(PAGE_FILE, "w", encoding="utf-8") as f:
        f.write(html)
    build_badge(last)
    print("Страница обновлена:", PAGE_FILE, "| бейдж тоже перерисован")


def days90_html(days, name):
    """HTML-полоса за 90 дней: по клетке на день, от старого к новому."""
    cells = []
    for offset in range(90, -1, -1):
        day = (datetime.now() - timedelta(days=offset)
               ).strftime("%Y-%m-%d")
        if day in days:
            total_d, good_d = days[day]
            if good_d == 0:
                cls = "no"
            elif good_d == total_d:
                cls = "yes"
            else:
                cls = "part"
            title = f"{name}, {day}: {good_d} из {total_d} проверок удачно"
        else:
            cls, title = "off", f"{name}, {day}: проверок не было"
        cells.append(f'<span class="{cls}" title="{title}"></span>')
    return "".join(cells)


BADGE_OK = "все ок"
# шаблон бейджа в стиле shields.io; собран склейкой коротких строк,
# чтобы не ловить E501 на неизменяемой разметке
BADGE_SVG = (
    '<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="20"'
    ' role="img" aria-label="Дозор: {value}">\n'
    '<linearGradient id="g" x2="0" y2="100%">'
    '<stop offset="0" stop-color="#bbb" stop-opacity=".1"/>'
    '<stop offset="1" stop-opacity=".1"/></linearGradient>\n'
    '<clipPath id="r"><rect width="{w}" height="20" rx="3" fill="#fff"/>'
    '</clipPath>\n'
    '<g clip-path="url(#r)"><rect width="104" height="20" fill="#555"/>'
    '<rect x="104" width="{vw}" height="20" fill="{color}"/>'
    '<rect width="{w}" height="20" fill="url(#g)"/></g>\n'
    '<g text-anchor="middle"'
    " font-family=\"DejaVu Sans,Verdana,sans-serif\""
    ' font-size="11" fill="#fff">\n'
    '<text x="52" y="14">Дозор</text>'
    '<text x="{vx}" y="14">{value}</text></g>\n'
    '</svg>'
)


def build_badge(last):
    """Рисует бейдж.svg: зелёный «все ок» или красный «упало: N».

    Плашку можно вписать в сайт/README — <img src="бейдж.svg"> она
    перерисовывается вместе с проверками.
    """
    fallen = sum(1 for row in last.values() if not row[2])
    if fallen:
        value, color = f"упало: {fallen}", "#c62828"
        width_v = 74
    else:
        value, color = BADGE_OK, "#0d7a4d"
        width_v = 54
    svg = BADGE_SVG.format(
        w=104 + width_v, vw=width_v, vx=104 + width_v // 2,
        value=value, color=color)
    with open(os.path.join(BASE_DIR, "бейдж.svg"), "w",
              encoding="utf-8") as f:
        f.write(svg)


def show_history(limit=20):
    """Печатает последние записи истории проверок."""
    if not os.path.isfile(DB_FILE):
        raise SystemExit("базы ещё нет — сперва запусти: python3 monitor.py"
                         " check")
    conn = sqlite3.connect(DB_FILE)
    rows = conn.execute(
        "SELECT ts, name, ok, status_code, latency_ms, error FROM checks"
        " ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
    conn.close()
    if not rows:
        print("История пуста")
        return
    for ts_v, name, ok, code, latency, error in rows:
        status = "🟢" if ok else "🔴"
        if ok and code is None:
            extra = f" стук {latency} мин назад"
        else:
            extra = f" код {code}, {latency} мс" if ok \
                else f" {error or ''}"
        print(f"{status} {ts_v} {name}{extra}")


CARD = """
  <div class="card {cls}">
    <div class="head"><span class="st">{status}</span><span class="ts">{ts}</span></div>  # noqa: E501
    <h2>{name}</h2>
    <div class="url">{url_html}</div>
    <div class="det">{detail}</div>
    <div class="dots">{dots}</div>
    <div class="days">{days}</div>
    <div class="up">{uptime}</div>
  </div>
"""

PAGE_HEADER = """<!doctype html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Статусы сайтов</title>
<style>
  :root { --bg:#f4f5f7; --card:#fff; --text:#1c2330; --muted:#5a6678;
          --ok:#0d7a4d; --bad:#c62828; --line:#e3e6ec; }
  @media (prefers-color-scheme: dark) { :root { --bg:#161a21;
        --card:#1e242e; --text:#e7ebf2; --muted:#98a2b3;
        --ok:#4ec98c; --bad:#ff7b7b; --line:#2c3441; } }
  * { box-sizing: border-box; margin: 0; }
  body { background: var(--bg); color: var(--text);
         font: 15px/1.45 system-ui, -apple-system, sans-serif; }
  .wrap { max-width: 1000px; margin: 0 auto; padding: 20px 16px; }
  h1 { font-size: 22px; margin-bottom: 4px; }
  .upl { color: var(--muted); font-size: 13px; margin-bottom: 16px; }
  .grid { display: grid; gap: 12px;
          grid-template-columns: repeat(auto-fill, minmax(290px, 1fr)); }
  .card { background: var(--card); border: 1px solid var(--line);
          border-radius: 12px; padding: 12px 14px; }
  .card.ok { border-left: 4px solid var(--ok); }
  .card.bad { border-left: 4px solid var(--bad); }
  .head { display: flex; justify-content: space-between; gap: 8px;
          font-size: 13px; }
  .st { font-weight: 600; }
  .ts { color: var(--muted); }
  h2 { font-size: 16px; margin: 6px 0 2px; }
  .url { font-size: 12px; word-break: break-all;
         color: var(--muted); margin-bottom: 6px; }
  .url a { color: inherit; }
  .det { font-size: 13px; margin-bottom: 8px; }
  .dots { letter-spacing: 1px; margin-bottom: 6px; }
  .days { display: flex; gap: 1px; margin-bottom: 6px; }
  .days span { flex: 1 1 auto; min-width: 0; height: 12px;
               border-radius: 1px; }
  .days .yes { background: var(--ok); }
  .days .part { background: #e8a13a; }
  .days .no { background: var(--bad); }
  .days .off { background: var(--line); opacity: .55; }
  .up { font-size: 12px; color: var(--muted); }
  .stale { color: #e8a13a; font-weight: 600; font-size: 14px;
           margin-bottom: 12px; }
  @media (max-width: 480px) { .grid { grid-template-columns: 1fr; } }
</style>
</head>
<body>
<div class="wrap">
  <h1>Статусы сайтов</h1>
  <div class="upl">Проверка: @@TS@@ · сайтов: @@SITES@@</div>
  @@STALE@@
  <div class="grid">@@CARDS@@
  </div>
</div>
</body>
</html>
"""


def main():
    """Разбор команды и запуск нужного действия."""
    cmd = sys.argv[1] if len(sys.argv) > 1 else "check"
    if cmd == "check":
        lock = acquire_run_lock()
        if lock is None:  # предыдущий прогон ещё не кончился
            print("Прогон уже идёт — этот запуск пропущен",
                  f"({RUN_LOCK_FILE})")
            return 0
        try:
            run_checks()
        finally:
            lock.close()  # закрытие ручки снимает замок
    elif cmd == "страница":
        build_page()
    elif cmd == "история":
        show_history()
    else:
        print("Не знаю команду:", cmd)
        print("Команды: check / страница / история")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
