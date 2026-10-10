"""بوت الخاص (DMs) مع دعم E2EE والتجاوز السليم لفحص الإصدار."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import sys
import time
from pathlib import Path
from typing import Any

# كتم السجلات الزائدة للحفاظ على نظافة الشاشة
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

# تجاوز فحص الإصدار الشكلي فقط (دون المساس بأنبوب القراءة)
from _messaging._listening_e2ee import _BridgeProcess, listeningE2EEEvent
_BridgeProcess._validate_contract = lambda self: None

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
        log("bridge", f"تم تجهيز الجسر ومنحه الصلاحية: {selected}")
    except Exception as e:
        log("bridge-err", f"خطأ في الصلاحيات: {e}")

    return selected


async def main() -> None:
    config = load_config()
    cookies = str(config.get("cookies") or "").strip()
    if not cookies:
        raise RuntimeError("لم يتم العثور على الكوكيز داخل config.json!")

    setup_bridge()

    log("boot", "جاري قراءة الجلسة وتسجيل الدخول...")
    from _core._session import dataGetHome

    dataFB = await dataGetHome(cookies)
    if not dataFB or not isinstance(dataFB, dict):
        raise RuntimeError("فشل استخراج الجلسة! حدث الكوكيز.")

    bot_id = str(dataFB.get("FacebookID") or "").strip()
    log("bot", f"تم تسجيل الدخول بنجاح! آيدي الحساب: {bot_id}")

    prefix = str(config.get("prefix") or "/")
    listener = listeningE2EEEvent(dataFB, debug_errors=False)
    events_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=1000)
    loop = asyncio.get_running_loop()

    def on_event(event: dict[str, Any]) -> None:
        if not isinstance(event, dict):
            return
        etype = event.get("type")

        if etype == "ready":
            log("status", "✅ تم الاتصال بخوادم فيسبوك واستقبال إشارة Ready!")
        elif etype == "e2eeConnected":
            log("status", "🔐 تم تفعيل تشفير الخاص E2EE بنجاح تام!")

        if etype in {"e2eeMessage", "message"}:
            loop.call_soon_threadsafe(events_queue.put_nowait, event)

    listener.on_message(on_event)

    log("bot", "بدء تشغيل مهمة اتصال الجسر بخوادم فيسبوك...")
    listener_task = asyncio.create_task(listener.connect_mqtt())

    # انتظار إشارة ready (التي يستجيب لها الجسر في ثوانٍ معدودة)
    ready = await asyncio.to_thread(listener.wait_until_connected, 60, require_e2ee=False)
    if not ready:
        log("warn", "تأخر الرد الأولي، جاري المتابعة...")
    else:
        log("bot", "🟢 البوت متصل الآن ويعمل في الخاص (DMs) 24/7!")

    try:
        while True:
            if listener_task.done():
                exc = listener_task.exception()
                if exc:
                    log("fatal", f"توقفت مهمة الاتصال: {exc}")
                    raise exc

            try:
                event = await asyncio.wait_for(events_queue.get(), timeout=2.0)
            except asyncio.TimeoutError:
                continue

            data = event.get("data") or {}
            body = data.get("text") or data.get("body") or ""
            chat_jid = data.get("chatJid")
            thread_id = data.get("threadId") or data.get("replyToID")
            sender_id = str(data.get("senderId") or data.get("senderID") or data.get("userID") or "")
            msg_id = str(data.get("id") or data.get("messageID") or "")

            if not body or sender_id == bot_id:
                continue

            log("recv", f"📩 رسالة من {sender_id}: {body}")

            if not str(body).startswith(prefix):
                continue

            cmd_text = str(body)[len(prefix) :].strip()
            parts = cmd_text.split(maxsplit=1)
            cmd = parts[0].lower()
            arg = parts[1] if len(parts) > 1 else ""

            async def reply(text: str) -> None:
                if chat_jid:
                    await listener.send_e2ee_message(
                        str(chat_jid), text, reply_to_id=msg_id
                    )
                elif thread_id:
                    await listener.send_message(
                        int(thread_id), text, reply_to_id=msg_id
                    )

            if cmd == "ping":
                await reply("🏓 pong! البوت شغال في الخاص ومشفر 🔒")
            elif cmd == "help":
                msg = (
                    "📖 أوامر الخاص:\n"
                    f"• {prefix}ping - فحص الاستجابة\n"
                    f"• {prefix}id - عرض الآيدي الخاص بك\n"
                    f"• {prefix}echo <نص> - تكرار النص"
                )
                await reply(msg)
            elif cmd == "id":
                await reply(f"🆔 آيدي حسابك: {sender_id}")
            elif cmd == "echo":
                await reply(arg if arg else f"الاستخدام: {prefix}echo <نص>")

    finally:
        listener.stop()
        await listener_task


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log("bot", "تم إيقاف البوت.")
    except Exception as e:
        log("exit", f"❌ توقف بسبب: {e}")
        time.sleep(60)
        sys.exit(1)
