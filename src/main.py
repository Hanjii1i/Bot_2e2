"""بوت الخاص (DMs) مع فحص أخطاء جسر Go وحماية الجلسة."""

from __future__ import annotations

import asyncio
import json
import os
import re
import subprocess
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


def ensure_and_test_bridge() -> None:
    """منح الصلاحية وفحص الجسر لمعرفة سبب انطفائه الحقيقي."""
    target_paths = [
        BRIDGE_PATH,
        HERE / "build" / "fbchat-bridge-e2ee",
        Path("build/fbchat-bridge-e2ee").resolve(),
    ]

    selected_path = None
    for path in target_paths:
        if path.exists() and path.is_file():
            selected_path = path
            break

    if not selected_path:
        log("bridge", "تحذير: لم يتم العثور على ملف الجسر!")
        return

    try:
        selected_path.chmod(0o755)
        os.environ["FBCHAT_E2EE_BIN"] = str(selected_path)
        log("bridge", f"تم العثور على الجسر وتفعيل الصلاحيات: {selected_path}")

        # اختبار تشغيل الجسر مباشرة لكشف أي نقص في مكتبات لينكس
        test = subprocess.run(
            [str(selected_path), "--help"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        log("bridge", f"نتيجة اختبار الجسر: exit_code={test.returncode}")
        if test.stderr:
            log("bridge-err", f"{test.stderr.strip()[:200]}")
    except Exception as e:
        log("bridge", f"خطأ أثناء فحص الجسر التجريبي: {e}")


async def main() -> None:
    config = load_config()
    cookies = str(config.get("cookies") or "").strip()
    if not cookies:
        raise RuntimeError("لم يتم العثور على الكوكيز داخل config.json!")

    ensure_and_test_bridge()

    log("boot", "جاري قراءة الجلسة وتسجيل الدخول...")
    dataFB = await dataGetHome(cookies)

    if not dataFB or not isinstance(dataFB, dict):
        raise RuntimeError("فشل استخراج الجلسة! جدد الكوكيز بعد تأكيد الدخول.")

    bot_id = str(dataFB.get("FacebookID") or "").strip()
    if not bot_id:
        raise RuntimeError("فشل استخراج جلسة Facebook! آيدي الحساب مفقود.")

    log("bot", f"تم تسجيل الدخول بنجاح! آيدي الحساب: {bot_id}")

    prefix = str(config.get("prefix") or "/")
    
    # تفعيل debug_errors لرؤية ما يقوله الجسر بالضبط
    listener = listeningE2EEEvent(dataFB, debug_errors=True)
    events_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=1000)
    loop = asyncio.get_running_loop()

    def on_event(event: dict[str, Any]) -> None:
        if isinstance(event, dict) and event.get("type") in {"e2eeMessage", "message"}:
            loop.call_soon_threadsafe(events_queue.put_nowait, event)

    listener.on_message(on_event)
    listener_task = asyncio.create_task(listener.connect_mqtt())

    log("bot", "جاري انتظار اتصال جسر التشفير (E2EE Handshake)...")
    ready = await asyncio.to_thread(listener.wait_until_connected, 30, require_e2ee=True)
    if not ready:
        raise RuntimeError("الجسر لم يستجب للاتصال بخوادم التشفير.")

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
        # الانتظار 60 ثانية قبل الإغلاق لمنع الـ Crash Loop وحماية الكوكيز
        time.sleep(60)
        sys.exit(1)
