"""بوت الخاص (DMs) مع توجيه سجلات الجسر الحية لكشف حالة MQTT."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import sys
import threading
import time
from pathlib import Path
from typing import Any

# كتم طلبات HTTP العادية فقط
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

# تجاوز فحص الإصدار مع طباعة العقد
from _messaging._listening_e2ee import _BridgeProcess, listeningE2EEEvent

original_spawn = _BridgeProcess._spawn


def hooked_spawn(self) -> None:
    original_spawn(self)

    # قراءة وطباعة كل حرف يصدره الجسر فوراً في السجلات
    def stream_bridge_logs(pipe, label):
        if not pipe:
            return
        for line in iter(pipe.readline, ""):
            if not line:
                break
            text = line.strip()
            if text:
                print(f"[{label}] {text}", flush=True)

    if getattr(self, "_proc", None):
        threading.Thread(
            target=stream_bridge_logs,
            args=(self._proc.stderr, "BRIDGE-ERR"),
            daemon=True,
        ).start()
        threading.Thread(
            target=stream_bridge_logs,
            args=(self._proc.stdout, "BRIDGE-OUT"),
            daemon=True,
        ).start()


_BridgeProcess._spawn = hooked_spawn
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
        log("bridge", f"تم تجهيز الجسر: {selected}")
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
    listener = listeningE2EEEvent(dataFB, debug_errors=True)
    events_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=1000)
    loop = asyncio.get_running_loop()

    def on_event(event: dict[str, Any]) -> None:
        if isinstance(event, dict) and event.get("type") in {"e2eeMessage", "message"}:
            loop.call_soon_threadsafe(events_queue.put_nowait, event)

    listener.on_message(on_event)

    log("bot", "بدء تشغيل مهمة اتصال الجسر بخوادم فيسبوك...")
    listener_task = asyncio.create_task(listener.connect_mqtt())

    # انتظار الاتصال
    await asyncio.sleep(5)
    log("bot", "مراقبة استجابة الجسر عبر السجلات الحية...")

    while not listener_task.done():
        await asyncio.sleep(2)

    exc = listener_task.exception()
    if exc:
        raise exc


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log("bot", "تم إيقاف البوت.")
    except Exception as e:
        log("exit", f"❌ سبب التوقف: {e}")
        time.sleep(60)
        sys.exit(1)
