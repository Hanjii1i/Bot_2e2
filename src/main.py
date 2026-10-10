"""بوت الخاص (DMs) مع دعم E2EE عبر Go Bridge."""

from __future__ import annotations

import asyncio
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any

import httpx

from _core._session import dataGetHome
from _messaging._bridge_actions import BridgeActions
from _messaging._listening_e2ee import listeningE2EEEvent

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


async def main() -> None:
    config = load_config()
    cookies = str(config.get("cookies") or "").strip()
    if not cookies:
        raise RuntimeError("لم يتم العثور على الكوكيز داخل config.json!")

    # تحديد مسار الجسر التنفيذي لضمان العثور عليه
    if BRIDGE_PATH.exists():
        os.environ["FBCHAT_E2EE_BIN"] = str(BRIDGE_PATH)
        log("bridge", f"تم العثور على جسر التشفير في: {BRIDGE_PATH}")
    else:
        log("bridge", f"تحذير: لم يتم العثور على {BRIDGE_PATH}، سيتم البحث في المسار الافتراضي.")

    log("boot", "جاري قراءة الجلسة وتسجيل الدخول...")
    dataFB = await dataGetHome(cookies)
    bot_id = str(dataFB.get("FacebookID") or "").strip()
    if not bot_id:
        raise RuntimeError("فشل استخراج جلسة Facebook! تأكد من الكوكيز.")

    log("bot", f"تم تسجيل الدخول بنجاح! آيدي الحساب: {bot_id}")

    prefix = str(config.get("prefix") or "/")
    listener = listeningE2EEEvent(dataFB, debug_errors=False)
    events_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=1000)
    loop = asyncio.get_running_loop()

    def on_event(event: dict[str, Any]) -> None:
        if isinstance(event, dict) and event.get("type") in {"e2eeMessage", "message"}:
            loop.call_soon_threadsafe(events_queue.put_nowait, event)

    listener.on_message(on_event)
    listener_task = asyncio.create_task(listener.connect_mqtt())

    log("bot", "جاري انتظار اتصال جسر التشفير (E2EE Handshake)...")
    ready = await asyncio.to_thread(listener.wait_until_connected, 90, require_e2ee=True)
    if not ready:
        raise RuntimeError("فشل بدء مستمع التشفير E2EE قبل انتهاء المهلة!")

    log("bot", "البوت نشط الآن وجاهز لاستقبال والرد على رسائل الخاص (DMs) 24/7!")

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
            elif cmd == "help":
                msg = (
                    "📖 أوامر الخاص:\n"
                    f"• {prefix}ping - فحص الاستجابة\n"
                    f"• {prefix}id - عرض الآيدي الخاص بك\n"
                    f"• {prefix}echo <نص> - تكرار النص"
                )
                await listener.send_e2ee_message(chat_jid, msg, reply_to_id=msg_id)
            elif cmd == "id":
                await listener.send_e2ee_message(
                    chat_jid, f"🆔 آيدي حسابك: {sender_id}", reply_to_id=msg_id
                )
            elif cmd == "echo":
                reply = arg if arg else f"الاستخدام: {prefix}echo <نص>"
                await listener.send_e2ee_message(chat_jid, reply, reply_to_id=msg_id)

    finally:
        listener.stop()
        await listener_task


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log("bot", "تم إيقاف البوت.")
    except Exception as e:
        log("fatal", f"❌ {e}")
        sys.exit(1)
