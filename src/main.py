"""Bot mẫu async-first cho fbchat-v2 مع دعم appstate.json التلقائي."""

from __future__ import annotations

import asyncio
import contextlib
import json
import sys
import time
import traceback
from collections import deque
from collections.abc import Awaitable, Callable
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx

from _core._permissions import create_private_json_file, set_private_file_permissions
from _core._session import dataGetHome
from _core._storage import FileSessionStorage
from _features._facebook import _search
from _messaging._bridge_actions import BridgeActions
from _messaging._listening_e2ee import listeningE2EEEvent

HERE = Path(__file__).resolve().parent
CONFIG_PATH = HERE / "config.json"
Handler = Callable[[dict[str, Any], str], Awaitable[None]]
DEFAULT_HTTP_TIMEOUT = 30.0
DEFAULT_E2EE_READY_TIMEOUT = 90.0
EVENT_QUEUE_MAXSIZE = 1000
RECENT_MESSAGE_IDS_MAXSIZE = 4096
MESSAGE_EVENT_TYPES = frozenset({"message", "e2eeMessage"})
CONTROL_EVENT_TYPES = frozenset(
    {
        "ready",
        "e2eeConnected",
        "reconnected",
        "disconnected",
        "closed",
        "error",
    }
)
FORWARDED_EVENT_TYPES = MESSAGE_EVENT_TYPES | CONTROL_EVENT_TYPES

_create_private_config = create_private_json_file
_set_private_file_permissions = set_private_file_permissions


def get_appstate_cookies() -> str | None:
    """البحث عن ملف appstate.json وتحويله تلقائياً إلى نص الكوكيز المطلوب."""
    possible_paths = [
        HERE / "appstate.json",
        HERE.parent / "appstate.json",
        Path("appstate.json").resolve(),
    ]
    for path in possible_paths:
        if path.exists() and path.is_file():
            try:
                with path.open("r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, list):
                    cookies = []
                    for item in data:
                        if isinstance(item, dict):
                            key = item.get("key") or item.get("name")
                            val = item.get("value")
                            if key and val:
                                cookies.append(f"{key}={val}")
                    if cookies:
                        cookie_str = "; ".join(cookies)
                        log("appstate", f"تم العثور على {path.name} وتحميل الكوكيز بنجاح!")
                        return cookie_str
            except Exception as e:
                log("appstate", f"فشل قراءة {path}: {e}")
    return None


def load_config() -> dict[str, Any]:
    """Đọc config và tạo template an toàn nếu chưa tồn tại."""
    if not CONFIG_PATH.exists():
        template = {
            "cookies": "",
            "prefix": "/",
            "admins": [],
            "log_message_content": False,
            "debug_errors": False,
        }
        _create_private_config(CONFIG_PATH, template)
        if get_appstate_cookies():
            return template
        raise RuntimeError(
            f"تم إنشاء {CONFIG_PATH}. ضع ملف appstate.json في المجلد أو اكتب الكوكيز ثم أعد التشغيل."
        )

    _set_private_file_permissions(CONFIG_PATH)
    with CONFIG_PATH.open("r", encoding="utf-8") as file_handle:
        config = json.load(file_handle)
    config.setdefault("prefix", "/")
    config.setdefault("admins", [])
    config.setdefault("log_message_content", False)
    config.setdefault("debug_errors", False)
    if not isinstance(config["prefix"], str) or not config["prefix"]:
        raise ValueError("config.prefix phải là chuỗi không rỗng.")
    if not isinstance(config["admins"], list):
        raise ValueError("config.admins phải là một danh sách ID.")
    if not isinstance(config["log_message_content"], bool):
        raise ValueError("config.log_message_content phải là true hoặc false.")
    if not isinstance(config["debug_errors"], bool):
        raise ValueError("config.debug_errors phải là true hoặc false.")
    return config


def is_valid_datafb(dataFB: object) -> bool:
    if not isinstance(dataFB, dict):
        return False
    facebook_id = str(dataFB.get("FacebookID") or "").strip()
    required = ("fb_dtsg", "jazoest", "sessionID", "clientRevision", "cookieFacebook")
    return facebook_id.isdigit() and all(
        str(dataFB.get(field) or "").strip() for field in required
    )


def log(tag: str, message: str) -> None:
    line = f"[{datetime.now():%H:%M:%S}] [{tag}] {message}"
    try:
        print(line)
    except UnicodeEncodeError:
        encoding = sys.stdout.encoding or "utf-8"
        safe_line = line.encode(encoding, errors="backslashreplace").decode(
            encoding, errors="replace"
        )
        print(safe_line)


class SimpleBot:
    def __init__(
        self,
        dataFB: dict[str, Any],
        *,
        prefix: str = "/",
        admins: list[Any] | None = None,
        log_message_content: bool = False,
        debug_errors: bool = False,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self.dataFB = dataFB
        self.prefix = prefix
        self.admins = {str(admin_id) for admin_id in admins or []}
        self.log_message_content = log_message_content
        self.debug_errors = debug_errors
        self.http_client = http_client
        self.listener = listeningE2EEEvent(dataFB, debug_errors=debug_errors)
        self._event_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(
            maxsize=EVENT_QUEUE_MAXSIZE
        )
        self._recent_message_ids: deque[str] = deque()
        self._recent_message_id_set: set[str] = set()
        self._dropped_message_events = 0
        self._last_bot_message: dict[str, tuple[str, str]] = {}
        self._handlers: dict[str, Handler] = {
            "ping": self._cmd_ping,
            "help": self._cmd_help,
            "id": self._cmd_id,
            "echo": self._cmd_echo,
            "search": self._cmd_search,
            "unsend": self._cmd_unsend,
        }

    async def run(self) -> None:
        log("bot", f"Đăng nhập thành công với UID = {self.dataFB.get('FacebookID')}")
        loop = asyncio.get_running_loop()
        self.listener.on_message(
            lambda event: self._forward_listener_event(loop, event)
        )
        listener_task = asyncio.create_task(
            self.listener.connect_mqtt(), name="fbchat-listener"
        )
        try:
            ready = await asyncio.to_thread(
                self.listener.wait_until_connected,
                DEFAULT_E2EE_READY_TIMEOUT,
                require_e2ee=False,
            )
            if not ready:
                raise RuntimeError("Listener chưa sẵn sàng trước timeout.")
            log("bot", "البوت جاهز ويعمل الآن في المجموعات! اضغط Ctrl+C للإيقاف.")
            while True:
                if listener_task.done():
                    listener_task.result()
                    raise RuntimeError("Listener قد توقف بشكل مفاجئ.")
                event = await self._get_event(timeout=1.0)
                message = self._message_from_event(event) if event else None
                if message is not None:
                    await self._dispatch(message)
        finally:
            await self._shutdown_listener(listener_task)

    def run_blocking(self) -> None:
        asyncio.run(self.run())

    async def _shutdown_listener(self, listener_task: asyncio.Task[None]) -> None:
        await asyncio.to_thread(self.listener.stop)
        try:
            await asyncio.wait_for(asyncio.shield(listener_task), timeout=5)
        except asyncio.TimeoutError:
            listener_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await listener_task
        except asyncio.CancelledError:
            if listener_task.cancelled():
                return
            if not listener_task.done():
                listener_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await listener_task
            raise
        except Exception:
            pass

    def _forward_listener_event(
        self, loop: asyncio.AbstractEventLoop, event: dict[str, Any]
    ) -> None:
        if not isinstance(event, dict):
            return
        if event.get("type") not in FORWARDED_EVENT_TYPES:
            return
        try:
            loop.call_soon_threadsafe(self._queue_event, event)
        except RuntimeError:
            if self.debug_errors:
                log("queue", "Bỏ qua event vì asyncio loop đã đóng.")

    def _queue_event(self, event: dict[str, Any]) -> None:
        event_type = event.get("type")
        if event_type not in MESSAGE_EVENT_TYPES:
            if event_type in CONTROL_EVENT_TYPES:
                self._message_from_event(event)
            return
        try:
            self._event_queue.put_nowait(event)
        except asyncio.QueueFull:
            with contextlib.suppress(asyncio.QueueEmpty):
                self._event_queue.get_nowait()
            self._event_queue.put_nowait(event)
            self._dropped_message_events += 1
            if self._dropped_message_events & (self._dropped_message_events - 1) == 0:
                log(
                    "queue",
                    "Queue message đầy; đã loại "
                    f"{self._dropped_message_events} event cũ.",
                )

    def _remember_message_id(self, message_id: str) -> bool:
        if message_id in self._recent_message_id_set:
            return False
        if len(self._recent_message_ids) >= RECENT_MESSAGE_IDS_MAXSIZE:
            oldest = self._recent_message_ids.popleft()
            self._recent_message_id_set.discard(oldest)
        self._recent_message_ids.append(message_id)
        self._recent_message_id_set.add(message_id)
        return True

    async def _get_event(self, timeout: float | None = None) -> dict[str, Any] | None:
        try:
            return await asyncio.wait_for(self._event_queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None

    def _message_from_event(self, event: dict[str, Any]) -> dict[str, Any] | None:
        event_type = event.get("type")
        raw_data = event.get("data")
        data = raw_data if isinstance(raw_data, dict) else {}

        if event_type in {
            "ready",
            "e2eeConnected",
            "reconnected",
            "disconnected",
            "closed",
        }:
            log("mqtt", str(event_type))
            return None
        if event_type == "error":
            if self.debug_errors:
                log("mqtt", f"Error: {data}")
            return None
        if event_type not in MESSAGE_EVENT_TYPES:
            return None

        chat_jid = data.get("chatJid")
        sender_jid = data.get("senderJid")
        message_type = "e2ee" if event_type == "e2eeMessage" else "thread"
        return {
            "body": data.get("text"),
            "timestamp": data.get("timestampMs", 0),
            "userID": data.get("senderId", 0),
            "messageID": data.get("id"),
            "replyToID": data.get("threadId", 0),
            "type": message_type,
            "chatJid": chat_jid,
            "senderJid": sender_jid,
            "attachments": data.get("attachments") or [],
            "raw": data,
        }

    async def _dispatch(self, message: dict[str, Any]) -> None:
        message_id = str(message.get("messageID") or "").strip()
        body = message.get("body")
        sender_id = str(message.get("userID") or "").strip()
        if not message_id or not sender_id or not isinstance(body, str) or not body:
            return

        if sender_id == str(self.dataFB.get("FacebookID")):
            return
        target = message.get("chatJid") or message.get("replyToID")
        body_for_log = (
            repr(body)
            if self.log_message_content
            else f"<redacted length={len(str(body))}>"
        )
        log("recv", f"[{message.get('type')}] {sender_id}@{target}: {body_for_log}")
        if not str(body).startswith(self.prefix):
            return

        command_line = str(body)[len(self.prefix) :].strip()
        if not command_line:
            return
        parts = command_line.split(maxsplit=1)
        command = parts[0].lower()
        argument = parts[1] if len(parts) > 1 else ""
        handler = self._handlers.get(command)
        if handler is None:
            return
        if not self._remember_message_id(message_id):
            return
        try:
            await handler(message, argument)
        except Exception as error:
            if self.debug_errors:
                log("err", f"Error /{command}: {error}")
                traceback.print_exc()

    async def _reply(self, message: dict[str, Any], content: str) -> None:
        chat_jid = message.get("chatJid")
        if chat_jid:
            result = await self.listener.send_e2ee_message(
                str(chat_jid),
                content,
                reply_to_id=str(message.get("messageID") or ""),
                reply_to_sender_jid=str(message.get("senderJid") or ""),
            )
            message_id = result.get("messageId") or result.get("id")
            if message_id:
                self._last_bot_message[str(chat_jid)] = (str(chat_jid), str(message_id))
                log("send", f"E2EE -> {chat_jid}: {content}")
            return

        thread_id = message.get("replyToID")
        if not thread_id:
            return
        result = await self.listener.send_message(
            int(thread_id),
            content,
            reply_to_id=str(message.get("messageID") or ""),
        )
        message_id = result.get("messageId") or result.get("id")
        if message_id:
            log("send", f"Group -> {thread_id}: {content}")

    async def _cmd_ping(self, message: dict[str, Any], argument: str) -> None:
        sent_ts = int(message.get("timestamp") or 0)
        latency = max(0, int(time.time() * 1000) - sent_ts) if sent_ts else None
        await self._reply(
            message, f"🏓 pong! ({latency} ms)" if latency is not None else "🏓 pong!"
        )

    async def _cmd_help(self, message: dict[str, Any], argument: str) -> None:
        prefix = self.prefix
        await self._reply(
            message,
            "📖 الأوامر المتاحة:\n"
            f"• {prefix}ping - فحص سرعة استجابة البوت\n"
            f"• {prefix}help - قائمة المساعدة\n"
            f"• {prefix}id - عرض الآيدي الخاص بك والمجموعة\n"
            f"• {prefix}echo <نص> - تكرار الكلام\n"
            f"• {prefix}search <اسم> - البحث عن حسابات فيسبوك",
        )

    async def _cmd_id(self, message: dict[str, Any], argument: str) -> None:
        await self._reply(
            message,
            f"🆔 نوع المحادثة: {message.get('type')}\n"
            f"آيدي المجموعة: {message.get('replyToID')}\n"
            f"آيدي حسابك: {message.get('userID')}\n"
            f"آيدي الرسالة: {message.get('messageID')}",
        )

    async def _cmd_echo(self, message: dict[str, Any], argument: str) -> None:
        await self._reply(
            message, argument or f"الاستخدام: {self.prefix}echo <نص>"
        )

    async def _cmd_search(self, message: dict[str, Any], argument: str) -> None:
        if not argument:
            await self._reply(message, f"الاستخدام: {self.prefix}search <اسم للبحث>")
            return
        result = await _search.func(self.dataFB, argument, client=self.http_client)
        users = result.get("searchResultsDict") if isinstance(result, dict) else None
        if not users:
            await self._reply(message, f"🔍 لم يتم العثور على نتائج لـ: {argument}")
            return
        lines = [f"🔍 نتائج البحث عن “{argument}”:"]
        lines.extend(
            f"{index}. {user.get('name')} - {user.get('id')}"
            for index, user in enumerate(users[:5], 1)
        )
        await self._reply(message, "\n".join(lines))

    async def _cmd_unsend(self, message: dict[str, Any], argument: str) -> None:
        sender_id = str(message.get("userID") or "")
        if self.admins and sender_id not in self.admins:
            await self._reply(message, "⛔ هذا الأمر خاص بالمطور فقط.")
            return
        chat_jid = str(message.get("chatJid") or "")
        if not chat_jid:
            await self._reply(message, "الحذف مدعوم في الرسائل المباشرة فقط.")
            return
        target = self._last_bot_message.get(chat_jid)
        if not target:
            return
        target_chat_jid, target_message_id = target
        if self.listener._bridge is not None:
            await BridgeActions(self.listener._bridge).unsend_e2ee_message(
                target_chat_jid, target_message_id
            )
            self._last_bot_message.pop(chat_jid, None)


async def main() -> None:
    config = load_config()
    log("boot", "جاري قراءة الجلسة وتسجيل الدخول...")

    # التحقق من وجود appstate.json أولاً
    appstate_cookies = get_appstate_cookies()
    if appstate_cookies:
        dataFB = await dataGetHome(appstate_cookies)
    else:
        log("boot", "لم يتم العثور على appstate.json، المحاولة عبر config.json...")
        dataFB = await dataGetHome(
            storage=FileSessionStorage(str(CONFIG_PATH), key="cookies")
        )

    if not is_valid_datafb(dataFB):
        raise RuntimeError(
            "فشل استخراج جلسة Facebook! تأكد من صحة ملف appstate.json أو تجديده."
        )

    async with httpx.AsyncClient(timeout=DEFAULT_HTTP_TIMEOUT) as http_client:
        bot = SimpleBot(
            dataFB,
            prefix=config["prefix"],
            admins=config["admins"],
            log_message_content=config["log_message_content"],
            debug_errors=config["debug_errors"],
            http_client=http_client,
        )
        await bot.run()


def main_blocking() -> None:
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log("bot", "تم إيقاف البوت بنجاح.")
    except (RuntimeError, ValueError, json.JSONDecodeError) as error:
        log("boot", f"❌ {error}")
        raise SystemExit(1) from error


if __name__ == "__main__":
    main_blocking()
