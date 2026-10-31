# -*- coding: utf-8 -*-
"""人设存档与切换插件。

把当前人设存成存档，随时切换回来。存档分三种范围：

- ``full``   整套人设：名字、人格/行为/回复风格、群聊通用提示词、私聊通用提示词、
              以及全部聊天流的专属提示词。
- ``group``  只存群聊通用提示词（``chat.reply_style.group_chat_prompt``）。
- ``stream`` 只存某个聊天流的专属提示词（``chat.reply_style.chat_prompts`` 里的一条）。

只动人设字段，聊天频率、模型、语音、黑白名单等配置一律不碰。
全部读写走 tomlkit，保留原文件的注释与排版。
"""

from __future__ import annotations

import asyncio
import copy
import io
import json
import re
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import tomllib
import tomlkit

from maibot_sdk import API, Field, MaiBotPlugin, PluginConfigBase

# 人设字段清单
BOT_KEYS = ("nickname", "alias_names")
PERSONALITY_KEYS = (
    "personality",
    "behavior_style",
    "reply_style",
    "multiple_reply_style",
    "multiple_probability",
)
REPLY_STYLE_KEYS = ("group_chat_prompt", "private_chat_prompts")

SCOPE_FULL = "full"
SCOPE_GROUP = "group"
SCOPE_STREAM = "stream"

SCOPE_LABELS = {
    SCOPE_FULL: "整套人设",
    SCOPE_GROUP: "群聊人设",
    SCOPE_STREAM: "聊天流人设",
}

FIELD_LABELS = {
    "nickname": "机器人名字",
    "alias_names": "别名",
    "personality": "人格设定",
    "behavior_style": "行为风格",
    "reply_style": "回复风格",
    "multiple_reply_style": "备用说话风格",
    "multiple_probability": "备用风格概率",
    "group_chat_prompt": "群聊通用提示词",
    "private_chat_prompts": "私聊通用提示词",
    "chat_prompts": "按聊天流定制",
}


class PluginSectionConfig(PluginConfigBase):
    """插件基础配置。"""

    __ui_label__ = "插件"
    __ui_icon__ = "package"
    __ui_order__ = 0

    enabled: bool = Field(default=True, description="是否启用插件")
    config_version: str = Field(default="1.1.0", description="配置版本")


class AutoRestoreSectionConfig(PluginConfigBase):
    """限时恢复。"""

    __ui_label__ = "限时恢复"
    __ui_icon__ = "clock"
    __ui_order__ = 1

    enabled: bool = Field(
        default=True,
        description="应用存档时允许填「多少分钟后自动恢复原人设」。关掉后只能手动切回，适合长期固定人设。",
    )
    default_minutes: int = Field(
        default=0,
        description="应用存档时不填时长就用这个值。0 表示不自动恢复（永久生效）。",
    )
    on_restart: str = Field(
        default="restore",
        description="插件重启后，发现限时已过期但还没恢复时的做法：restore 立即恢复原人设；keep 保持现状并在界面提示。",
    )


class PersonaSwitcherConfig(PluginConfigBase):
    """人设切换插件配置。"""

    plugin: PluginSectionConfig = Field(default_factory=PluginSectionConfig)
    auto_restore: AutoRestoreSectionConfig = Field(default_factory=AutoRestoreSectionConfig)


def _plain(value: Any) -> Any:
    """把 tomlkit 的值转成普通 Python 对象。"""
    if value is None:
        return None
    if hasattr(value, "unwrap"):
        value = value.unwrap()
    if isinstance(value, list):
        return [_plain(item) for item in value]
    if isinstance(value, dict):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _safe_name(name: str) -> str:
    """把用户输入的名字转成可安全作为目录名的形式。"""
    bad = '\\/:*?"<>|'
    cleaned = "".join("_" if char in bad else char for char in str(name or "")).strip().strip(".")
    return cleaned[:60] or "unnamed"


def _read_json(path: Path, default: Any = None) -> Any:
    """读 JSON，失败时返回默认值。"""
    if not path.is_file():
        return default
    try:
        return json.load(io.open(path, encoding="utf-8"))
    except (ValueError, OSError):
        return default


def _write_json(path: Path, payload: Any) -> None:
    """写 JSON（LF、无 BOM、保留中文）。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    with io.open(path, "w", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False, indent=2))


class PersonaSwitcherPlugin(MaiBotPlugin):
    """人设存档与切换。"""

    config_model = PersonaSwitcherConfig

    def __init__(self) -> None:
        super().__init__()
        self._plugin_dir: Path = Path(__file__).resolve().parent
        # 插件位于 <项目根>/plugins/<插件目录>/，上两级即项目根
        self._config_path: Path = (
            self._plugin_dir.parent.parent / "config" / "bot_config.toml"
        )
        self._store_dir: Path = self._plugin_dir / "archives"
        self._store_dir.mkdir(parents=True, exist_ok=True)
        # 生效状态（当前用的是哪份存档、什么时候到期）落在插件目录，
        # 不写宿主数据目录，卸载插件即一并消失。
        self._state_path: Path = self._plugin_dir / "active_state.json"
        self._restore_task: Optional[asyncio.Task] = None

    # ===== 生命周期 =====

    async def on_load(self) -> None:
        """加载时自检主配置是否可达，并恢复未过期的限时人设。"""
        self._store_dir.mkdir(parents=True, exist_ok=True)
        if not self._config_path.is_file():
            self._log(
                "warning",
                "找不到主配置 %s，人设读写会失败；插件目录结构可能不是 <项目根>/plugins/<插件>/",
                self._config_path,
            )
            return
        self._log(
            "info",
            "人设读写已就绪：%s（已有 %s 份存档）",
            self._config_path,
            len(self._list_archives()),
        )
        self._resume_pending_restore()

    async def on_unload(self) -> None:
        """卸载时取消挂起的恢复任务。状态文件保留，重启后继续。"""
        self._cancel_restore()

    async def on_config_update(
        self, scope: str, config_data: dict[str, Any], version: str
    ) -> None:
        """插件自身配置变更：只记一条日志，无需处理。"""
        self._log("debug", "插件配置已更新: scope=%s version=%s", scope, version)

    # ===== 日志 =====

    def _log(self, level: str, message: str, *args: Any) -> None:
        logger = getattr(self._ctx, "logger", None) if self._ctx is not None else None
        if logger is None:
            return
        getattr(logger, level, logger.info)(message, *args)

    # ===== 主配置读写 =====

    def _check_config(self) -> None:
        if not self._config_path.is_file():
            raise ValueError(f"找不到主配置文件：{self._config_path}")

    def _load_live(self) -> Any:
        self._check_config()
        with io.open(self._config_path, encoding="utf-8") as handle:
            return tomlkit.parse(handle.read())

    def _write_live(self, document: Any) -> None:
        text = self._normalize_spacing(tomlkit.dumps(document))
        tomllib.loads(text)  # 写盘前自检，坏内容不落盘
        with io.open(self._config_path, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(text)

    @staticmethod
    def _normalize_spacing(text: str) -> str:
        """确保每个顶层表头前都有一个空行。

        tomlkit 删除数组表末项时会把后一个表头前的空行一起吃掉，
        这里补回来，避免每次保存都在文件里留下无谓的格式差异。
        """
        header = re.compile(r"^\[\[?[A-Za-z0-9_.\-]+\]\]?\s*(?:#.*)?$")
        lines = text.split("\n")
        fixed: List[str] = []
        for index, line in enumerate(lines):
            if (
                index
                and header.match(line)
                and fixed
                and fixed[-1].strip()
            ):
                fixed.append("")
            fixed.append(line)
        return "\n".join(fixed)

    # ===== 人设提取与套用 =====

    def _extract(self, document: Any) -> Dict[str, Any]:
        """从配置文档取出全部人设字段。"""
        data: Dict[str, Any] = {}

        bot = document.get("bot", {})
        data["bot"] = {key: _plain(bot.get(key)) for key in BOT_KEYS if key in bot}

        personality = document.get("personality", {})
        data["personality"] = {
            key: _plain(personality.get(key))
            for key in PERSONALITY_KEYS
            if key in personality
        }

        reply_style = document.get("chat", {}).get("reply_style", {})
        data["reply_style"] = {
            key: _plain(reply_style.get(key))
            for key in REPLY_STYLE_KEYS
            if key in reply_style
        }
        prompts: List[Dict[str, Any]] = []
        for item in reply_style.get("chat_prompts", []):
            prompts.append(
                {
                    key: _plain(item.get(key))
                    for key in ("platform", "item_id", "rule_type", "prompt")
                    if key in item
                }
            )
        data["reply_style"]["chat_prompts"] = prompts
        return data

    def _apply(self, document: Any, data: Dict[str, Any]) -> None:
        """把存档内容写进配置文档（内存中操作）。"""
        bot_data = data.get("bot") or {}
        if bot_data:
            if "bot" not in document:
                document["bot"] = tomlkit.table()
            for key, value in bot_data.items():
                if _plain(document["bot"].get(key)) == value:
                    continue  # 值没变就不碰，避免 tomlkit 重排、丢注释
                document["bot"][key] = self._to_toml(value)

        personality_data = data.get("personality") or {}
        if personality_data:
            if "personality" not in document:
                document["personality"] = tomlkit.table()
            for key, value in personality_data.items():
                if _plain(document["personality"].get(key)) == value:
                    continue
                document["personality"][key] = self._to_toml(value)

        reply_data = data.get("reply_style") or {}
        if "chat" not in document or "reply_style" not in document["chat"]:
            raise ValueError("主配置里找不到 [chat.reply_style]，结构异常")
        reply_style = document["chat"]["reply_style"]
        for key in REPLY_STYLE_KEYS:
            if key in reply_data and _plain(reply_style.get(key)) != reply_data[key]:
                reply_style[key] = reply_data[key]

        if "chat_prompts" in reply_data:
            self._write_chat_prompts(reply_style, reply_data["chat_prompts"])

    @staticmethod
    def _to_toml(value: Any) -> Any:
        """把普通 Python 值转成 tomlkit 可写入的形式。"""
        if isinstance(value, list):
            array = tomlkit.array()
            array.extend(value)
            return array
        return value

    @staticmethod
    def _write_chat_prompts(reply_style: Any, prompts: List[Dict[str, Any]]) -> None:
        """写 chat_prompts。三元组能一一对上时只改文本，保留原格式与注释。"""
        existing = reply_style.get("chat_prompts", [])
        same_shape = len(existing) == len(prompts) and all(
            str(existing[index].get("platform")) == str(item.get("platform"))
            and str(existing[index].get("item_id")) == str(item.get("item_id"))
            and str(existing[index].get("rule_type")) == str(item.get("rule_type"))
            for index, item in enumerate(prompts)
        )
        if same_shape:
            for index, item in enumerate(prompts):
                if _plain(existing[index].get("prompt")) != item.get("prompt"):
                    existing[index]["prompt"] = item.get("prompt", "")
            return

        array_of_tables = tomlkit.aot()
        for item in prompts:
            table = tomlkit.table()
            table["platform"] = item.get("platform", "")
            table["item_id"] = str(item.get("item_id", ""))
            table["rule_type"] = item.get("rule_type", "")
            table["prompt"] = item.get("prompt", "")
            array_of_tables.append(table)
        reply_style["chat_prompts"] = array_of_tables

    # ===== 存档存储 =====

    def _archive_dir(self, name: str) -> Path:
        return self._store_dir / _safe_name(name)

    def _read_archive(self, name: str) -> Dict[str, Any]:
        payload = _read_json(self._archive_dir(name) / "persona.json")
        if not isinstance(payload, dict):
            raise ValueError(f"没有名为「{name}」的存档")
        return payload

    def _write_archive(self, name: str, payload: Dict[str, Any], note: str = "") -> None:
        directory = self._archive_dir(name)
        directory.mkdir(parents=True, exist_ok=True)
        _write_json(directory / "persona.json", payload)
        _write_json(
            directory / "meta.json",
            {
                "name": _safe_name(name),
                "saved_at": datetime.now().isoformat(timespec="seconds"),
                "note": note,
                "scope": payload.get("_scope", SCOPE_FULL),
                "target": payload.get("_target") or {},
                "summary": self._summarize(payload),
                "source": str(self._config_path),
            },
        )

    def _summarize(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """给存档生成一段摘要，用于列表展示。"""
        scope = payload.get("_scope", SCOPE_FULL)
        summary: Dict[str, Any] = {"scope": scope}
        if scope == SCOPE_STREAM:
            target = payload.get("_target") or {}
            prompt = self._stream_prompt_of(payload)
            summary["target_label"] = (
                f"{target.get('platform', '')} "
                f"{'群' if target.get('rule_type') == 'group' else '私聊'} "
                f"{target.get('item_id', '')}"
            )
            summary["chars"] = len(prompt)
            return summary

        if scope == SCOPE_GROUP:
            summary["chars"] = len(
                (payload.get("reply_style") or {}).get("group_chat_prompt") or ""
            )
            return summary

        personality = payload.get("personality") or {}
        summary["nickname"] = (payload.get("bot") or {}).get("nickname", "")
        summary["personality_chars"] = len(personality.get("personality") or "")
        summary["behavior_chars"] = len(personality.get("behavior_style") or "")
        summary["reply_chars"] = len(personality.get("reply_style") or "")
        reply_style = payload.get("reply_style") or {}
        summary["group_chars"] = len(reply_style.get("group_chat_prompt") or "")
        summary["private_chars"] = len(reply_style.get("private_chat_prompts") or "")
        prompts = reply_style.get("chat_prompts") or []
        summary["chat_prompt_count"] = len(prompts)
        summary["chat_prompt_chars"] = sum(len(item.get("prompt") or "") for item in prompts)
        summary["total_chars"] = (
            summary["personality_chars"]
            + summary["behavior_chars"]
            + summary["reply_chars"]
            + summary["group_chars"]
            + summary["private_chars"]
            + summary["chat_prompt_chars"]
        )
        return summary

    @staticmethod
    def _stream_prompt_of(payload: Dict[str, Any]) -> str:
        prompts = (payload.get("reply_style") or {}).get("chat_prompts") or []
        return (prompts[0].get("prompt") or "") if prompts else ""

    def _list_archives(self, scope: Optional[str] = None) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        if not self._store_dir.is_dir():
            return items
        for entry in sorted(self._store_dir.iterdir()):
            if not entry.is_dir():
                continue
            payload = _read_json(entry / "persona.json")
            if not isinstance(payload, dict):
                continue
            meta = _read_json(entry / "meta.json", {}) or {}
            item_scope = payload.get("_scope", SCOPE_FULL)
            if scope is not None and item_scope != scope:
                continue
            items.append(
                {
                    "name": entry.name,
                    "scope": item_scope,
                    "scope_label": SCOPE_LABELS.get(item_scope, item_scope),
                    "saved_at": meta.get("saved_at", ""),
                    "note": meta.get("note", ""),
                    "target": payload.get("_target") or {},
                    "summary": self._summarize(payload),
                }
            )
        items.sort(key=lambda item: item.get("saved_at", ""), reverse=True)
        return items

    # ===== 差异对比 =====

    def _diff(self, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        """对比存档与线上，返回逐字段差异。"""
        live = self._extract(self._load_live())
        scope = payload.get("_scope", SCOPE_FULL)

        if scope == SCOPE_GROUP:
            left = (payload.get("reply_style") or {}).get("group_chat_prompt") or ""
            right = (live.get("reply_style") or {}).get("group_chat_prompt") or ""
            if left == right:
                return []
            return [
                {
                    "path": "chat.reply_style.group_chat_prompt",
                    "label": FIELD_LABELS["group_chat_prompt"],
                    "archive": left,
                    "live": right,
                }
            ]

        if scope == SCOPE_STREAM:
            target = payload.get("_target") or {}
            want = self._stream_prompt_of(payload)
            current = self._find_stream_entry(live, target)
            have = (current or {}).get("prompt") or ""
            if want == have:
                return []
            return [
                {
                    "path": f"chat.reply_style.chat_prompts[{target.get('item_id', '')}]",
                    "label": f"{target.get('platform', '')} {target.get('item_id', '')} 的专属提示词",
                    "archive": want,
                    "live": have,
                }
            ]

        diffs: List[Dict[str, Any]] = []

        def walk(path: str, left: Any, right: Any) -> None:
            if type(left) is not type(right):
                diffs.append({"path": path, "archive": left, "live": right})
                return
            if isinstance(left, dict):
                for key in sorted(set(left) | set(right)):
                    walk(f"{path}.{key}" if path else key, left.get(key), right.get(key))
            elif isinstance(left, list):
                if len(left) != len(right):
                    diffs.append(
                        {
                            "path": path,
                            "archive": f"{len(left)} 项",
                            "live": f"{len(right)} 项",
                        }
                    )
                    return
                for index, (item_left, item_right) in enumerate(zip(left, right)):
                    walk(f"{path}[{index}]", item_left, item_right)
            elif left != right:
                diffs.append({"path": path, "archive": left, "live": right})

        trimmed = {key: value for key, value in payload.items() if not key.startswith("_")}
        walk("", trimmed, live)
        for item in diffs:
            leaf = item["path"].split(".")[-1].split("[")[0]
            item["label"] = FIELD_LABELS.get(leaf, leaf)
            for key in ("archive", "live"):
                value = item.get(key)
                item[key] = "" if value is None else str(value)
        return diffs

    @staticmethod
    def _find_stream_entry(
        live: Dict[str, Any], target: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """在线上人设里找到某个聊天流对应的提示词条目。"""
        prompts = (live.get("reply_style") or {}).get("chat_prompts") or []
        for item in prompts:
            if (
                str(item.get("platform")) == str(target.get("platform"))
                and str(item.get("item_id")) == str(target.get("item_id"))
                and str(item.get("rule_type")) == str(target.get("rule_type"))
            ):
                return item
        return None

    # ===== 生效状态与限时恢复 =====

    def _read_state(self) -> Dict[str, Any]:
        payload = _read_json(self._state_path, {})
        if not isinstance(payload, dict):
            return {}
        return payload

    def _write_state(self, payload: Dict[str, Any]) -> None:
        _write_json(self._state_path, payload)

    def _clear_state(self) -> None:
        if self._state_path.is_file():
            try:
                self._state_path.unlink()
            except OSError as exc:
                self._log("warning", "清理生效状态失败: %s", exc)

    def _cancel_restore(self) -> None:
        if self._restore_task is not None and not self._restore_task.done():
            self._restore_task.cancel()
        self._restore_task = None

    def _restore_section(self) -> Any:
        config = getattr(self, "_plugin_config_instance", None)
        return getattr(config, "auto_restore", None)

    def _restore_enabled(self) -> bool:
        section = self._restore_section()
        return True if section is None else bool(getattr(section, "enabled", True))

    def _default_minutes(self) -> int:
        section = self._restore_section()
        value = 0 if section is None else int(getattr(section, "default_minutes", 0) or 0)
        return max(0, value)

    def _restore_on_restart(self) -> str:
        section = self._restore_section()
        value = "restore" if section is None else str(getattr(section, "on_restart", "restore") or "restore")
        return value if value in {"restore", "keep"} else "restore"

    def _describe_state(self) -> Dict[str, Any]:
        """把生效状态整理成前端好用的形状。

        除了「当前用的是哪份存档」，还顺手核对线上人设与存档是否仍然一致
        ——手动改过 bot_config.toml 之后，状态就不该再说「正在用这份存档」。
        """
        state = self._read_state()
        name = str(state.get("archive") or "").strip()
        if not name:
            return {"active": False}

        try:
            payload = self._read_archive(name)
        except ValueError:
            self._clear_state()
            return {"active": False, "stale_reason": "存档已被删除"}

        expires_at = str(state.get("expires_at") or "")
        remaining = None
        expired = False
        if expires_at:
            try:
                deadline = datetime.fromisoformat(expires_at)
                seconds = (deadline - datetime.now()).total_seconds()
                remaining = max(0, int(seconds))
                expired = seconds <= 0
            except ValueError:
                expires_at = ""

        drifted = False
        try:
            drifted = bool(self._diff(payload))
        except Exception:  # noqa: BLE001
            drifted = False

        return {
            "active": True,
            "archive": name,
            "scope": payload.get("_scope", SCOPE_FULL),
            "scope_label": SCOPE_LABELS.get(payload.get("_scope", SCOPE_FULL), ""),
            "since": state.get("since", ""),
            "expires_at": expires_at,
            "remaining_seconds": remaining,
            "expired": expired,
            "timed": bool(expires_at),
            "backup": state.get("backup", ""),
            "drifted": drifted,
            "drifted_hint": "线上人设与这份存档已不一致（可能有人手动改过配置文件）"
            if drifted
            else "",
        }

    def _schedule_restore(self, delay_seconds: float) -> None:
        """安排一次「到点恢复」。"""
        self._cancel_restore()
        if delay_seconds <= 0:
            return
        self._restore_task = asyncio.create_task(self._restore_later(delay_seconds))

    async def _restore_later(self, delay_seconds: float) -> None:
        try:
            await asyncio.sleep(delay_seconds)
        except asyncio.CancelledError:
            return
        state = self._read_state()
        name = str(state.get("archive") or "").strip()
        backup = str(state.get("backup") or "").strip()
        if not name or not backup:
            self._clear_state()
            return
        try:
            payload = self._read_archive(backup)
            await self._apply_archive(backup, payload, remember=False)
            self._clear_state()
            self._log("info", "限时人设已到期，已自动恢复到「%s」", backup)
        except Exception as exc:  # noqa: BLE001
            self._log("error", "限时人设自动恢复失败: %s", exc)

    def _resume_pending_restore(self) -> None:
        """插件重载后接着处理上一次没走完的限时。"""
        state = self._read_state()
        name = str(state.get("archive") or "").strip()
        if not name:
            return
        expires_at = str(state.get("expires_at") or "")
        if not expires_at:
            self._log("debug", "当前生效的人设「%s」为永久生效", name)
            return
        try:
            deadline = datetime.fromisoformat(expires_at)
        except ValueError:
            self._clear_state()
            return
        remaining = (deadline - datetime.now()).total_seconds()
        if remaining > 0:
            self._schedule_restore(remaining)
            self._log("info", "限时人设「%s」还剩 %s 秒，已重新挂起恢复任务", name, int(remaining))
            return

        if not self._restore_enabled() or self._restore_on_restart() == "keep":
            self._log(
                "warning",
                "限时人设「%s」已过期但配置为保持现状，未自动恢复",
                name,
            )
            return

        backup = str(state.get("backup") or "").strip()
        if not backup:
            self._clear_state()
            return
        try:
            payload = self._read_archive(backup)
        except ValueError:
            self._log("error", "限时人设「%s」已过期，但备份存档「%s」不见了", name, backup)
            self._clear_state()
            return
        try:
            payload_doc = self._load_live()
            self._apply(payload_doc, {k: v for k, v in payload.items() if not k.startswith("_")})
            self._write_live(payload_doc)
            self._clear_state()
            self._log("info", "插件重启时发现限时人设已过期，已恢复到「%s」", backup)
        except Exception as exc:  # noqa: BLE001
            self._log("error", "插件重启时自动恢复失败: %s", exc)
    # ===== 聊天流 =====

    async def _streams(self) -> List[Dict[str, Any]]:
        """取全部聊天流，并标注每个流当前有没有专属提示词。"""
        try:
            raw = await self.ctx.chat.get_all_streams()
        except Exception as exc:  # noqa: BLE001
            self._log("warning", "读取聊天流失败: %s", exc)
            raw = []
        streams = raw if isinstance(raw, list) else []
        live = self._extract(self._load_live())
        prompts = (live.get("reply_style") or {}).get("chat_prompts") or []

        result: List[Dict[str, Any]] = []
        seen = set()
        for item in streams:
            if not isinstance(item, dict):
                continue
            stream_id = str(item.get("stream_id") or item.get("session_id") or "")
            if not stream_id or stream_id in seen:
                continue
            seen.add(stream_id)
            platform = str(item.get("platform") or "")
            item_id = str(item.get("user_id") or item.get("group_id") or "")
            is_group = bool(item.get("is_group_session")) or str(
                item.get("chat_type") or ""
            ) == "group"
            rule_type = "group" if is_group else "private"
            entry = self._find_stream_entry(
                live,
                {"platform": platform, "item_id": item_id, "rule_type": rule_type},
            )
            result.append(
                {
                    "stream_id": stream_id,
                    "platform": platform,
                    "item_id": item_id,
                    "rule_type": rule_type,
                    "chat_type": rule_type,
                    "label": f"{platform} {'群' if is_group else '私聊'} {item_id}",
                    "has_prompt": entry is not None,
                    "prompt_chars": len((entry or {}).get("prompt") or ""),
                }
            )
        result.sort(key=lambda item: (item["rule_type"], item["platform"], item["item_id"]))
        return result

    async def _stream_by_id(self, stream_id: str) -> Dict[str, Any]:
        for item in await self._streams():
            if item["stream_id"] == str(stream_id):
                return item
        raise ValueError("找不到该聊天流")

    def _set_stream_prompt(
        self, target: Dict[str, Any], prompt: str
    ) -> None:
        """写入或删除某个聊天流的专属提示词。"""
        live = self._extract(self._load_live())
        prompts = list((live.get("reply_style") or {}).get("chat_prompts") or [])
        index = -1
        for position, item in enumerate(prompts):
            if (
                str(item.get("platform")) == str(target.get("platform"))
                and str(item.get("item_id")) == str(target.get("item_id"))
                and str(item.get("rule_type")) == str(target.get("rule_type"))
            ):
                index = position
                break

        if not str(prompt or "").strip():
            if index >= 0:
                prompts.pop(index)
        else:
            record = {
                "platform": target.get("platform", ""),
                "item_id": str(target.get("item_id", "")),
                "rule_type": target.get("rule_type", ""),
                "prompt": str(prompt),
            }
            if index >= 0:
                prompts[index] = record
            else:
                prompts.append(record)

        document = self._load_live()
        self._apply(document, {"reply_style": {"chat_prompts": prompts}})
        self._write_live(document)

    # ===== WebUI API：概览 =====

    @API("persona_overview", description="读取当前线上人设摘要与两类存档数量", version="1")
    async def persona_overview(self) -> Dict[str, Any]:
        live = self._extract(self._load_live())
        all_archives = self._list_archives()
        return {
            "config_path": str(self._config_path),
            "store_dir": str(self._store_dir),
            "live": {
                "bot": live.get("bot") or {},
                "personality": live.get("personality") or {},
                "reply_style": live.get("reply_style") or {},
                "summary": self._summarize(live),
            },
            "counts": {
                SCOPE_FULL: sum(1 for item in all_archives if item["scope"] == SCOPE_FULL),
                SCOPE_GROUP: sum(1 for item in all_archives if item["scope"] == SCOPE_GROUP),
                SCOPE_STREAM: sum(1 for item in all_archives if item["scope"] == SCOPE_STREAM),
            },
            "active": self._describe_state(),
        }

    @API("persona_list", description="按范围列出存档", version="1")
    async def persona_list(self, scope: str = "") -> Dict[str, Any]:
        wanted = str(scope or "").strip() or None
        if wanted is not None and wanted not in SCOPE_LABELS:
            raise ValueError(f"未知范围: {scope}")
        return {"scope": wanted or "all", "archives": self._list_archives(wanted)}

    @API("persona_detail", description="读取一份存档的完整内容与和线上的差异", version="1")
    async def persona_detail(self, name: str) -> Dict[str, Any]:
        payload = self._read_archive(name)
        meta = _read_json(self._archive_dir(name) / "meta.json", {}) or {}
        return {
            "name": _safe_name(name),
            "scope": payload.get("_scope", SCOPE_FULL),
            "scope_label": SCOPE_LABELS.get(payload.get("_scope", SCOPE_FULL), ""),
            "saved_at": meta.get("saved_at", ""),
            "note": meta.get("note", ""),
            "target": payload.get("_target") or {},
            "persona": {key: value for key, value in payload.items() if not key.startswith("_")},
            "diff": self._diff(payload),
        }

    # ===== WebUI API：整套人设 =====

    @API("persona_save_full", description="把当前整套人设存成一份存档", version="1")
    async def persona_save_full(self, name: str, note: str = "") -> Dict[str, Any]:
        clean = self._validate_name(name)
        payload = self._extract(self._load_live())
        payload["_scope"] = SCOPE_FULL
        return self._store(name=clean, payload=payload, note=note)

    # ===== WebUI API：群聊人设 =====

    @API("persona_save_group", description="把当前群聊通用提示词存成一份存档", version="1")
    async def persona_save_group(self, name: str, note: str = "") -> Dict[str, Any]:
        clean = self._validate_name(name)
        live = self._extract(self._load_live())
        payload = {
            "_scope": SCOPE_GROUP,
            "reply_style": {
                "group_chat_prompt": (live.get("reply_style") or {}).get(
                    "group_chat_prompt", ""
                )
            },
        }
        return self._store(name=clean, payload=payload, note=note)

    @API("persona_apply_group", description="把群聊人设存档写回线上（只改群聊通用提示词）", version="1")
    async def persona_apply_group(self, name: str, minutes: int = -1) -> Dict[str, Any]:
        payload = self._read_archive(name)
        if payload.get("_scope", SCOPE_FULL) != SCOPE_GROUP:
            raise ValueError("这份存档不是群聊人设，请用「应用」而不是「应用群聊人设」")
        return await self._apply_archive(
            name, payload, minutes=None if minutes < 0 else int(minutes)
        )

    # ===== WebUI API：聊天流人设 =====

    @API("persona_streams", description="列出全部聊天流，以及每个流有没有专属提示词", version="1")
    async def persona_streams(self) -> Dict[str, Any]:
        return {"streams": await self._streams()}

    @API("persona_stream_prompt", description="读取某个聊天流当前的专属提示词", version="1")
    async def persona_stream_prompt(self, stream_id: str) -> Dict[str, Any]:
        stream = await self._stream_by_id(stream_id)
        live = self._extract(self._load_live())
        entry = self._find_stream_entry(live, stream)
        return {
            "stream_id": stream["stream_id"],
            "label": stream["label"],
            "platform": stream["platform"],
            "item_id": stream["item_id"],
            "rule_type": stream["rule_type"],
            "prompt": (entry or {}).get("prompt", ""),
            "exists": entry is not None,
        }

    @API("persona_set_stream_prompt", description="写入或清空某个聊天流的专属提示词", version="1")
    async def persona_set_stream_prompt(self, stream_id: str, prompt: str) -> Dict[str, Any]:
        stream = await self._stream_by_id(stream_id)
        text = str(prompt or "")
        if len(text) > 20000:
            raise ValueError("提示词过长（上限 20000 字）")
        self._set_stream_prompt(stream, text)
        self._log("info", "已更新聊天流人设: %s (%s 字)", stream["label"], len(text))
        return {
            "stream_id": stream["stream_id"],
            "chars": len(text),
            "cleared": not text.strip(),
            "message": "已保存，配置会自动热重载",
        }

    @API("persona_save_stream", description="把某个聊天流的专属提示词存成一份存档", version="1")
    async def persona_save_stream(self, name: str, stream_id: str, note: str = "") -> Dict[str, Any]:
        clean = self._validate_name(name)
        stream = await self._stream_by_id(stream_id)
        live = self._extract(self._load_live())
        entry = self._find_stream_entry(live, stream)
        if entry is None:
            raise ValueError("这个聊天流还没有专属提示词，先在下面的编辑框里写好再保存")
        payload = {
            "_scope": SCOPE_STREAM,
            "_target": {
                "platform": stream["platform"],
                "item_id": stream["item_id"],
                "rule_type": stream["rule_type"],
            },
            "reply_style": {"chat_prompts": [copy.deepcopy(entry)]},
        }
        return self._store(name=clean, payload=payload, note=note)

    @API("persona_apply_stream", description="把聊天流人设存档写回线上对应那个流", version="1")
    async def persona_apply_stream(self, name: str, minutes: int = -1) -> Dict[str, Any]:
        payload = self._read_archive(name)
        if payload.get("_scope", SCOPE_FULL) != SCOPE_STREAM:
            raise ValueError("这份存档不是聊天流人设")
        return await self._apply_archive(
            name, payload, minutes=None if minutes < 0 else int(minutes)
        )

    # ===== WebUI API：通用 =====

    @API("persona_apply", description="把任意存档写回线上（会自动先备份当前人设）", version="1")
    async def persona_apply(self, name: str, minutes: int = -1) -> Dict[str, Any]:
        payload = self._read_archive(name)
        return await self._apply_archive(
            name, payload, minutes=None if minutes < 0 else int(minutes)
        )

    @API("persona_active", description="读取当前生效的是哪份存档、还剩多久自动恢复", version="1")
    async def persona_active(self) -> Dict[str, Any]:
        state = self._describe_state()
        state["auto_restore_enabled"] = self._restore_enabled()
        state["default_minutes"] = self._default_minutes()
        return state

    @API("persona_restore_now", description="立刻把当前生效的人设切回应用前的样子", version="1")
    async def persona_restore_now(self) -> Dict[str, Any]:
        state = self._read_state()
        backup = str(state.get("backup") or "").strip()
        if not backup:
            # 状态里没有备份，可能是旧版本留下的残留记录，清掉并说明清楚，
            # 不要抛异常让前端只看到一句「执行失败」。
            self._clear_state()
            self._cancel_restore()
            return {
                "restored_to": "",
                "message": "这份人设没有可恢复的备份，已经清掉失效的生效记录",
                "scope": "",
            }
        try:
            payload = self._read_archive(backup)
        except ValueError:
            self._clear_state()
            self._cancel_restore()
            return {
                "restored_to": "",
                "message": f"备份存档「{backup}」已被删除，无法恢复；已清掉失效的生效记录",
                "scope": "",
            }
        self._cancel_restore()
        result = await self._apply_archive(backup, payload, remember=False)
        self._clear_state()
        self._log("info", "已手动恢复应用前人设: %s", backup)
        return {
            "restored_to": backup,
            "message": "已恢复应用前的人设，配置会自动热重载",
            "scope": result.get("scope", ""),
        }

    @API("persona_cancel_timer", description="取消限时，让当前人设变成永久生效", version="1")
    async def persona_cancel_timer(self) -> Dict[str, Any]:
        state = self._read_state()
        if not state.get("archive"):
            raise ValueError("当前没有生效的存档")
        self._cancel_restore()
        state["expires_at"] = ""
        self._write_state(state)
        self._log("info", "已取消限时恢复，当前人设改为永久生效: %s", state.get("archive"))
        return {"message": "已取消限时，这份人设会一直生效到手动切换"}

    @API("persona_delete", description="删除一份存档", version="1")
    async def persona_delete(self, name: str) -> Dict[str, Any]:
        directory = self._archive_dir(name)
        if not directory.is_dir():
            raise ValueError(f"没有名为「{name}」的存档")
        shutil.rmtree(directory)
        self._log("info", "已删除人设存档: %s", _safe_name(name))
        return {"name": _safe_name(name), "message": f"已删除存档「{_safe_name(name)}」"}

    @API("persona_rename", description="给存档改名", version="1")
    async def persona_rename(self, name: str, new_name: str) -> Dict[str, Any]:
        clean = self._validate_name(new_name)
        source = self._archive_dir(name)
        if not source.is_dir():
            raise ValueError(f"没有名为「{name}」的存档")
        target = self._archive_dir(clean)
        if target.exists():
            raise ValueError(f"已存在同名存档「{clean}」")
        source.rename(target)
        meta = _read_json(target / "meta.json", {}) or {}
        meta["name"] = clean
        _write_json(target / "meta.json", meta)
        return {"name": clean, "message": f"已改名为「{clean}」"}

    # ===== 内部流程 =====

    @staticmethod
    def _validate_name(name: str) -> str:
        clean = str(name or "").strip()
        if not clean:
            raise ValueError("存档名不能为空")
        if len(clean) > 40:
            raise ValueError("存档名不能超过 40 个字符")
        return clean

    def _store(self, name: str, payload: Dict[str, Any], note: str) -> Dict[str, Any]:
        """写存档；同名且内容不同则先把旧版转存，不静默覆盖。"""
        directory = self._archive_dir(name)
        superseded = ""
        if (directory / "persona.json").is_file():
            old = _read_json(directory / "persona.json", {})
            if old == payload:
                return {
                    "name": _safe_name(name),
                    "unchanged": True,
                    "superseded": "",
                    "summary": self._summarize(payload),
                    "message": "内容与已有存档完全相同，未重复保存",
                }
            stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
            shutil.copy2(directory / "persona.json", directory / f"persona.superseded-{stamp}.json")
            superseded = f"persona.superseded-{stamp}.json"
        self._write_archive(name, payload, note=note)
        self._log("info", "已保存人设存档: %s (%s)", _safe_name(name), payload.get("_scope"))
        return {
            "name": _safe_name(name),
            "superseded": superseded,
            "summary": self._summarize(payload),
            "message": f"已保存存档「{_safe_name(name)}」",
        }

    async def _apply_archive(
        self,
        name: str,
        payload: Dict[str, Any],
        minutes: Optional[int] = None,
        remember: bool = True,
    ) -> Dict[str, Any]:
        """应用存档：先备份当前相关字段，再写回。

        Args:
            name: 存档名。
            payload: 存档内容。
            minutes: 多少分钟后自动恢复；``None`` 用插件配置里的默认值，0 表示不自动恢复。
            remember: 是否记录生效状态。自动恢复自己调用时传 False，避免把自己覆盖掉。
        """
        scope = payload.get("_scope", SCOPE_FULL)
        live = self._extract(self._load_live())

        if scope == SCOPE_STREAM:
            target = payload.get("_target") or {}
            current = self._find_stream_entry(live, target) or {}
            backup_payload = {
                "_scope": SCOPE_STREAM,
                "_target": target,
                "reply_style": {"chat_prompts": [copy.deepcopy(current)] if current else []},
            }
            if current:
                self._write_archive(
                    f"_应用前自动备份_{datetime.now().strftime('%Y%m%d-%H%M%S')}",
                    backup_payload,
                    note="应用聊天流人设前自动备份",
                )
            self._set_stream_prompt(target, self._stream_prompt_of(payload))
            self._log("info", "已应用聊天流人设: %s", _safe_name(name))
            return {
                "name": _safe_name(name),
                "scope": scope,
                "message": f"已切换到「{_safe_name(name)}」，配置会自动热重载",
            }

        if scope == SCOPE_GROUP:
            backup_payload = {
                "_scope": SCOPE_GROUP,
                "reply_style": {
                    "group_chat_prompt": (live.get("reply_style") or {}).get(
                        "group_chat_prompt", ""
                    )
                },
            }
        else:
            backup_payload = dict(live)
            backup_payload["_scope"] = SCOPE_FULL

        backup_name = ""
        if self._diff(payload):
            backup_name = f"_应用前自动备份_{datetime.now().strftime('%Y%m%d-%H%M%S')}"
            self._write_archive(backup_name, backup_payload, note="应用存档前自动备份")

        document = self._load_live()
        trimmed = {key: value for key, value in payload.items() if not key.startswith("_")}
        self._apply(document, trimmed)
        self._write_live(document)
        self._log("info", "已应用人设存档: %s (%s)", _safe_name(name), scope)

        result: Dict[str, Any] = {
            "name": _safe_name(name),
            "scope": scope,
            "message": f"已切换到「{_safe_name(name)}」，配置会自动热重载",
        }

        if remember:
            minutes_value = self._default_minutes() if minutes is None else int(minutes)
            minutes_value = max(0, minutes_value)
            if minutes_value and not self._restore_enabled():
                # 关掉限时恢复后，即便调用方传了时长也不生效，避免「配了禁用却仍会自己切回去」。
                minutes_value = 0

            self._cancel_restore()
            if not backup_name:
                # 没产生备份说明线上本来就和这份存档一模一样，等于没切换：
                # 这时候记状态会让「立刻恢复」无东西可恢复，所以干脆不记。
                self._clear_state()
                result["unchanged"] = True
                result["message"] = f"线上人设已经和「{_safe_name(name)}」一致，无需切换"
                return result

            state: Dict[str, Any] = {
                "archive": _safe_name(name),
                "scope": scope,
                "since": datetime.now().isoformat(timespec="seconds"),
                "expires_at": "",
                "backup": backup_name,
            }
            if minutes_value:
                deadline = datetime.now() + timedelta(minutes=minutes_value)
                state["expires_at"] = deadline.isoformat(timespec="seconds")
                self._write_state(state)
                self._schedule_restore(minutes_value * 60)
                result["expires_at"] = state["expires_at"]
                result["message"] = (
                    f"已切换到「{_safe_name(name)}」，{minutes_value} 分钟后自动恢复"
                )
            else:
                self._write_state(state)

        return result


def create_plugin() -> PersonaSwitcherPlugin:
    return PersonaSwitcherPlugin()
