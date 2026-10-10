"""بوت الخاص (DMs) - نسخة نظيفة السجلات لكشف سبب الجسر مباشرة."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import subprocess
import sys
import time
import traceback
from pathlib import Path
from typing import Any

# كتم سيل نصوص الشبكة من فيسبوك
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

HERE = Path(__file__).resolve().parent
CONFIG_PATH = HERE / "config.json"
BRIDGE_PATH = HERE.parent / "build" / "fbchat-bridge-e2ee"


def log(tag: str, message: str) -> None:
    line = f"[{time.strftime('%H:%M:%S')}] [{tag}] {message}"
    print(line, flush=True)


def load_config() -> dict[str, Any]:
    default_config = {
        "prefix": "/",
        "admins": ["100002717697815"],
        "cookies": "",
    }
    if not CONFIG_PATH.exists():
        return default_config
    try:
        with CONFIG_PATH.open("r", encoding="utf-8") as f:
            cfg = json.load(f)
            default_config.update(cfg)
    except Exception as e:
        log("config", f"تحذير: ({e}) - استخراج الكوكيز...")
        try:
            raw = CONFIG_PATH.read_text(encoding="utf-8", errors="ignore")
            m = re.search(r'"cookies"\s*:\s*"(.*?)"', raw)
            if m:
                default_config["cookies"] = m.group(1).replace("\n", " ").strip()
        except Exception:
            pass
    return default_config


def setup_bridge() -> Path | None:
    target_paths = [
        BRIDGE_PATH,
        HERE / "build" / "fbchat-bridge-e2ee",
        Path("build/fbchat-bridge-e2ee").resolve(),
    ]

    selected = None
    for p in target_paths:
        if p.exists() and p.is_file():
            selected = p
            break

    if not selected:
        log("bridge", "❌ لم يتم العثور على ملف الجسر!")
        return None

    try:
        selected.chmod(0o755)
        os.environ["FBCHAT_E2EE_BIN"] = str(selected)
        log("bridge", f"تم العثور على الجسر ومنحه الصلاحية: {selected}")

        # فحص خيارات الجسر
        res = subprocess.run([str(selected), "--help"], capture_output=True, text=True, timeout=5)
        help_text = (res.stdout or res.stderr or "").strip()
        if help_text:
            first_line = help_text.splitlines()[0]
            log("bridge-info", f"معلومات الجسر: {first_line}")
    except Exception as e:
        log("bridge-err", f"خطأ فحص الجسر: {e}")

    return selected


async def main() -> None:
    config = load_config()
    cookies = str(config.get("cookies") or "").strip()
    if not cookies:
        raise RuntimeError("لم يتم العثور على الكوكيز داخل config.json!")

    setup_bridge()

    log("boot", "جاري قراءة الجلسة وتسجيل الدخول...")
    from _core._session import dataGetHome
    from _messaging._listening_e2ee import listeningE2EEEvent

    dataFB = await dataGetHome(cookies)
    if not dataFB or not isinstance(dataFB, dict):
        raise RuntimeError("فشل استخراج الجلسة! حدث الكوكيز.")

    bot_id = str(dataFB.get("FacebookID") or "").strip()
    log("bot", f"تم تسجيل الدخول بنجاح! آيدي الحساب: {bot_id}")

    prefix = str(config.get("prefix") or "/")
    listener = listeningE2EEEvent(dataFB, debug_errors=True)
    events_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=1000)
    loop = asyncio.get_running_loop()

    def on_event(event: dict[str, Any]) -> None:
        if isinstance(event, dict) and event.get("type") in {"e2eeMessage", "message"}:
            loop.call_soon_threadsafe(events_queue.put_nowait, event)

    listener.on_message(on_event)

    log("bot", "بدء تشغيل مهمة اتصال الجسر و MQTT...")
    listener_task = asyncio.create_task(listener.connect_mqtt())

    # مراقبة مهمة الاتصال
    for sec in range(1, 25):
        await asyncio.sleep(1)
        if listener_task.done():
            exc = listener_task.exception()
            if exc:
                log("fatal-task", f"❌ توقفت مهمة الجسر بخطأ: {exc}")
                raise exc
            else:
                log("fatal-task", "❌ خرجت مهمة الجسر دون أخطاء برمجية ولكن انتهى عملها مبكراً!")
                raise RuntimeError("Listener task stopped unexpectedly")

        if getattr(listener, "_e2ee_ready", False) or getattr(listener, "connected", False):
            log("bot", f"✅ اتصل الجسر بنجاح بعد {sec} ثانية!")
            break
    else:
        log("warn", "انتهت المهلة، محاولة إجبار التحقق من الجسر...")

    log("bot", "البوت نشط الآن وجاهز لاستقبال رسائل الخاص (DMs) 24/7!")

    try:
        while True:
            event = await events_queue.get()
            data = event.get("data") or {}
            body = data.get("text") or ""
            chat_jid = data.get("chatJid")
            sender_id = str(data.get("senderId") or "")
            msg_id = str(data.get("id") or "")

            if not body or sender_id == bot_id or not chat_jid:
                continue

            log("recv", f"رسالة خاصة من {sender_id}: {body}")

            if not body.startswith(prefix):
                continue

            cmd_text = body[len(prefix) :].strip()
            parts = cmd_text.split(maxsplit=1)
            cmd = parts[0].lower()
            arg = parts[1] if len(parts) > 1 else ""

            if cmd == "ping":
                await listener.send_e2ee_message(
                    chat_jid, "🏓 pong! البوت شغال في الخاص ومشفر 🔒", reply_to_id=msg_id
                )
            elif cmd == "id":
                await listener.send_e2ee_message(
                    chat_jid, f"🆔 آيدي حسابك: {sender_id}", reply_to_id=msg_id
                )
            elif cmd == "echo":
                await listener.send_e2ee_message(
                    chat_jid, arg or "اكتب نصاً بعد الأمر", reply_to_id=msg_id
                )

    finally:
        listener.stop()
        await listener_task


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log("bot", "تم إيقاف البوت.")
    except Exception as e:
        log("exit", f"❌ تفاصيل التوقف: {e}")
        traceback.print_exc()
        time.sleep(60)
        sys.exit(1)
