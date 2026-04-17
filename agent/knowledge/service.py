"""
Knowledge service for handling knowledge base operations.

Provides a unified interface for listing, reading, and graphing knowledge files,
callable from the web console, API, or CLI.

Knowledge file layout (under workspace_root):
    knowledge/index.md
    knowledge/log.md
    knowledge/<category>/<slug>.md
"""

import fnmatch
import os
import re
import threading
import time
from pathlib import Path
from typing import Optional

from common.log import logger
from common.package_manager import check_dulwich
from common.utils import parse_env_bool
from config import conf


class KnowledgeService:
    """
    High-level service for knowledge base queries.
    Operates directly on the filesystem.
    """

    def __init__(self, workspace_root: str):
        self.workspace_root = workspace_root
        self.knowledge_dir = os.path.join(workspace_root, "knowledge")

    # ------------------------------------------------------------------
    # list — directory tree with stats
    # ------------------------------------------------------------------
    def list_tree(self) -> dict:
        """
        Return the knowledge directory tree grouped by category,
        supporting arbitrarily nested sub-directories.

        Returns::

            {
                "tree": [
                    {
                        "dir": "concepts",
                        "files": [
                            {"name": "moe.md", "title": "MoE", "size": 1234},
                        ],
                        "children": []
                    },
                    {
                        "dir": "platform",
                        "files": [],
                        "children": [
                            {
                                "dir": "analysis",
                                "files": [{"name": "perf.md", ...}],
                                "children": []
                            }
                        ]
                    },
                ],
                "stats": {"pages": 15, "size": 32768},
                "enabled": true
            }
        """
        if not os.path.isdir(self.knowledge_dir):
            return {"tree": [], "stats": {"pages": 0, "size": 0}, "enabled": conf().get("knowledge", True)}

        stats = {"pages": 0, "size": 0}
        root_files, tree = self._scan_dir(self.knowledge_dir, stats, is_root=True)

        return {
            "root_files": root_files,
            "tree": tree,
            "stats": stats,
            "enabled": conf().get("knowledge", True),
        }

    def _scan_dir(self, dir_path: str, stats: dict, is_root: bool = False) -> tuple:
        """
        Recursively scan a directory.

        :return: (files, children) where files is a list of .md file dicts
                 in this directory and children is a list of sub-directory nodes.
        """
        files = []
        children = []
        for name in sorted(os.listdir(dir_path)):
            if name.startswith("."):
                continue
            full = os.path.join(dir_path, name)
            if os.path.isdir(full):
                sub_files, sub_children = self._scan_dir(full, stats)
                children.append({"dir": name, "files": sub_files, "children": sub_children})
            elif name.endswith(".md"):
                size = os.path.getsize(full)
                if not is_root:
                    stats["pages"] += 1
                    stats["size"] += size
                title = name.replace(".md", "")
                try:
                    with open(full, "r", encoding="utf-8") as f:
                        first_line = f.readline().strip()
                    if first_line.startswith("# "):
                        title = first_line[2:].strip()
                except Exception:
                    pass
                files.append({"name": name, "title": title, "size": size})
        return files, children

    # ------------------------------------------------------------------
    # read — single file content
    # ------------------------------------------------------------------
    def read_file(self, rel_path: str) -> dict:
        """
        Read a single knowledge markdown file.

        :param rel_path: Relative path within knowledge/, e.g. ``concepts/moe.md``
        :return: dict with ``content`` and ``path``
        :raises ValueError: if path is invalid or escapes knowledge dir
        :raises FileNotFoundError: if file does not exist
        """
        if not rel_path or ".." in rel_path:
            raise ValueError("invalid path")

        full_path = os.path.normpath(os.path.join(self.knowledge_dir, rel_path))
        allowed = os.path.normpath(self.knowledge_dir)
        if not full_path.startswith(allowed + os.sep) and full_path != allowed:
            raise ValueError("path outside knowledge dir")

        if not os.path.isfile(full_path):
            raise FileNotFoundError(f"file not found: {rel_path}")

        with open(full_path, "r", encoding="utf-8") as f:
            content = f.read()
        return {"content": content, "path": rel_path}

    # ------------------------------------------------------------------
    # graph — nodes and links for visualization
    # ------------------------------------------------------------------
    def build_graph(self) -> dict:
        """
        Parse all knowledge pages and extract cross-reference links.

        Returns::

            {
                "nodes": [
                    {"id": "concepts/moe.md", "label": "MoE", "category": "concepts"},
                    ...
                ],
                "links": [
                    {"source": "concepts/moe.md", "target": "entities/deepseek.md"},
                    ...
                ]
            }
        """
        knowledge_path = Path(self.knowledge_dir)
        if not knowledge_path.is_dir():
            return {"nodes": [], "links": []}

        nodes = {}
        links = []
        link_re = re.compile(r'\[([^\]]*)\]\(([^)]+\.md)\)')

        for md_file in knowledge_path.rglob("*.md"):
            rel = str(md_file.relative_to(knowledge_path))
            if rel in ("index.md", "log.md"):
                continue
            parts = rel.split("/")
            category = parts[0] if len(parts) > 1 else "root"
            title = md_file.stem.replace("-", " ").title()
            try:
                content = md_file.read_text(encoding="utf-8")
                first_line = content.strip().split("\n")[0]
                if first_line.startswith("# "):
                    title = first_line[2:].strip()
                for _, link_target in link_re.findall(content):
                    resolved = (md_file.parent / link_target).resolve()
                    try:
                        target_rel = str(resolved.relative_to(knowledge_path))
                    except ValueError:
                        continue
                    if target_rel != rel:
                        links.append({"source": rel, "target": target_rel})
            except Exception:
                pass
            nodes[rel] = {"id": rel, "label": title, "category": category}

        valid_ids = set(nodes.keys())
        links = [l for l in links if l["source"] in valid_ids and l["target"] in valid_ids]
        seen = set()
        deduped = []
        for l in links:
            key = tuple(sorted([l["source"], l["target"]]))
            if key not in seen:
                seen.add(key)
                deduped.append(l)

        return {"nodes": list(nodes.values()), "links": deduped}

    # ------------------------------------------------------------------
    # dispatch — single entry point for protocol messages
    # ------------------------------------------------------------------
    def dispatch(self, action: str, payload: Optional[dict] = None) -> dict:
        """
        Dispatch a knowledge management action.

        :param action: ``list``, ``read``, or ``graph``
        :param payload: action-specific payload
        :return: protocol-compatible response dict
        """
        payload = payload or {}
        try:
            if action == "list":
                result = self.list_tree()
                return {"action": action, "code": 200, "message": "success", "payload": result}

            elif action == "read":
                path = payload.get("path")
                if not path:
                    return {"action": action, "code": 400, "message": "path is required", "payload": None}
                result = self.read_file(path)
                return {"action": action, "code": 200, "message": "success", "payload": result}

            elif action == "graph":
                result = self.build_graph()
                return {"action": action, "code": 200, "message": "success", "payload": result}

            else:
                return {"action": action, "code": 400, "message": f"unknown action: {action}", "payload": None}

        except ValueError as e:
            return {"action": action, "code": 403, "message": str(e), "payload": None}
        except FileNotFoundError as e:
            return {"action": action, "code": 404, "message": str(e), "payload": None}
        except Exception as e:
            logger.error(f"[KnowledgeService] dispatch error: action={action}, error={e}")
            return {"action": action, "code": 500, "message": str(e), "payload": None}


class KnowledgeGitSync:
    def __init__(self, workspace_root: str):
        self.workspace_root = workspace_root
        self.knowledge_dir = os.path.join(workspace_root, "knowledge")
        self.git_url = (os.environ.get("KNOWLEDGE_GIT_URL") or "").strip()
        self.branch = (os.environ.get("KNOWLEDGE_GIT_BRANCH") or "main").strip() or "main"
        self.interval_seconds = int(os.environ.get("KNOWLEDGE_GIT_SYNC_INTERVAL") or "60")
        self.enabled = parse_env_bool("KNOWLEDGE_GIT_SYNC_ENABLED", default=True)
        self._stop_event = threading.Event()
        self._last_synced_mtime = 0.0

    def start(self):
        if not self.enabled or not self.git_url:
            return
        t = threading.Thread(target=self._loop, daemon=True)
        t.start()

    def stop(self):
        self._stop_event.set()

    def _loop(self):
        try:
            self._ensure_repo_ready()
            self._last_synced_mtime = self._calc_content_mtime()
        except Exception as e:
            logger.warning(f"[KnowledgeGitSync] init failed: {e}")

        while not self._stop_event.is_set():
            try:
                self.sync_once()
            except Exception as e:
                logger.warning(f"[KnowledgeGitSync] sync failed: {e}")
            self._stop_event.wait(timeout=max(5, self.interval_seconds))

    def _ensure_repo_ready(self):
        check_dulwich()
        from dulwich import porcelain

        os.makedirs(self.workspace_root, exist_ok=True)

        git_dir = os.path.join(self.knowledge_dir, ".git")
        if os.path.isdir(git_dir):
            return

        if os.path.exists(self.knowledge_dir) and os.listdir(self.knowledge_dir):
            raise RuntimeError("knowledge dir is not empty but not a git repo")

        if os.path.exists(self.knowledge_dir) and not os.path.isdir(self.knowledge_dir):
            raise RuntimeError("knowledge path exists but is not a directory")

        logger.info("[KnowledgeGitSync] Cloning knowledge repo...")
        porcelain.clone(self.git_url, self.knowledge_dir, checkout=True)
        logger.info("[KnowledgeGitSync] Knowledge repo cloned")

    def _calc_content_mtime(self) -> float:
        base = self.knowledge_dir
        if not os.path.isdir(base):
            return 0.0
        latest = 0.0
        for root, dirs, files in os.walk(base):
            dirs[:] = [d for d in dirs if d != ".git" and not d.startswith(".")]
            for f in files:
                if f.startswith("."):
                    continue
                full = os.path.join(root, f)
                try:
                    mt = os.path.getmtime(full)
                except OSError:
                    continue
                if mt > latest:
                    latest = mt
        return latest

    def sync_once(self):
        if not self.enabled or not self.git_url:
            return

        self._ensure_repo_ready()

        current_mtime = self._calc_content_mtime()
        if current_mtime <= self._last_synced_mtime:
            return

        check_dulwich()
        from dulwich import porcelain

        porcelain.add(self.knowledge_dir, ".")
        msg = f"knowledge sync {time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())}".encode("utf-8")
        author = (os.environ.get("KNOWLEDGE_GIT_AUTHOR") or "CowAgent <cowagent@local>").encode("utf-8")
        try:
            porcelain.commit(self.knowledge_dir, msg, author=author, committer=author)
        except Exception:
            self._last_synced_mtime = current_mtime
            return

        try:
            porcelain.push(self.knowledge_dir, self.git_url, self.branch)
        except Exception as e:
            logger.warning(f"[KnowledgeGitSync] push failed: {e}")
            return

        self._last_synced_mtime = current_mtime
        logger.info("[KnowledgeGitSync] synced")


class WorkspacePartialGitSync:
    def __init__(self, workspace_root: str):
        self.workspace_root = workspace_root
        self.git_url = (os.environ.get("WORKSPACE_GIT_URL") or "").strip()
        self.branch = (os.environ.get("WORKSPACE_GIT_BRANCH") or "main").strip() or "main"
        self.interval_seconds = int(os.environ.get("WORKSPACE_GIT_SYNC_INTERVAL") or "120")
        self.enabled = parse_env_bool("WORKSPACE_GIT_SYNC_ENABLED", default=True)

        raw_include = (os.environ.get("WORKSPACE_GIT_INCLUDE") or "").strip()
        if raw_include:
            self.include = [p.strip().lstrip("/") for p in raw_include.split(",") if p.strip()]
        else:
            self.include = [
                "knowledge",
                "skills",
                "memory",
                "scheduler",
                "config.json",
                "AGENT.md",
                "USER.md",
                "RULE.md",
                "MEMORY.md",
            ]

        raw_exclude = (os.environ.get("WORKSPACE_GIT_EXCLUDE") or "").strip()
        if raw_exclude:
            self.exclude = [p.strip().lstrip("/") for p in raw_exclude.split(",") if p.strip()]
        else:
            self.exclude = [
                "memory/long-term/*",
                "**/*.db",
                "**/*.db-wal",
                "**/*.db-shm",
            ]

        self._stop_event = threading.Event()
        self._last_synced_mtime = 0.0

    def start(self):
        if not self.enabled or not self.git_url:
            return
        t = threading.Thread(target=self._loop, daemon=True)
        t.start()

    def stop(self):
        self._stop_event.set()

    def _loop(self):
        try:
            self._ensure_repo_ready()
            self._last_synced_mtime = self._calc_included_mtime()
        except Exception as e:
            logger.warning(f"[WorkspacePartialGitSync] init failed: {e}")

        while not self._stop_event.is_set():
            try:
                self.sync_once()
            except Exception as e:
                logger.warning(f"[WorkspacePartialGitSync] sync failed: {e}")
            self._stop_event.wait(timeout=max(5, self.interval_seconds))

    def _ensure_repo_ready(self):
        check_dulwich()
        from dulwich import porcelain

        os.makedirs(self.workspace_root, exist_ok=True)
        git_dir = os.path.join(self.workspace_root, ".git")
        if os.path.isdir(git_dir):
            return

        # Prefer cloning on a fresh instance (empty workspace).
        is_empty = True
        for name in os.listdir(self.workspace_root):
            if name.startswith("."):
                continue
            is_empty = False
            break

        if is_empty:
            logger.info("[WorkspacePartialGitSync] Cloning workspace repo...")
            porcelain.clone(self.git_url, self.workspace_root, checkout=True)
            logger.info("[WorkspacePartialGitSync] Workspace repo cloned")
            return

        # Fallback: init a repo in-place and push selected content.
        logger.info("[WorkspacePartialGitSync] Initializing workspace repo in-place...")
        porcelain.init(self.workspace_root)

    def _is_excluded(self, rel_posix: str) -> bool:
        for pat in self.exclude:
            pat2 = pat.replace("\\", "/")
            if fnmatch.fnmatch(rel_posix, pat2) or fnmatch.fnmatch(rel_posix, pat2.lstrip("./")):
                return True
        return False

    def _iter_included_files(self):
        root = self.workspace_root
        for entry in self.include:
            abs_path = os.path.join(root, entry)
            if os.path.isfile(abs_path):
                rel = os.path.relpath(abs_path, root).replace("\\", "/")
                if not self._is_excluded(rel):
                    yield rel
                continue
            if os.path.isdir(abs_path):
                for r, dirs, files in os.walk(abs_path):
                    dirs[:] = [d for d in dirs if d != ".git" and not d.startswith(".")]
                    for f in files:
                        if f.startswith("."):
                            continue
                        full = os.path.join(r, f)
                        rel = os.path.relpath(full, root).replace("\\", "/")
                        if not self._is_excluded(rel):
                            yield rel

    def _calc_included_mtime(self) -> float:
        latest = 0.0
        root = self.workspace_root
        for rel in self._iter_included_files():
            full = os.path.join(root, rel.replace("/", os.sep))
            try:
                mt = os.path.getmtime(full)
            except OSError:
                continue
            if mt > latest:
                latest = mt
        return latest

    def sync_once(self):
        if not self.enabled or not self.git_url:
            return

        self._ensure_repo_ready()

        current_mtime = self._calc_included_mtime()
        if current_mtime <= self._last_synced_mtime:
            return

        check_dulwich()
        from dulwich import porcelain

        added_any = False
        for rel in self._iter_included_files():
            try:
                porcelain.add(self.workspace_root, rel)
                added_any = True
            except Exception:
                continue

        if not added_any:
            self._last_synced_mtime = current_mtime
            return

        msg = f"workspace sync {time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())}".encode("utf-8")
        author = (os.environ.get("WORKSPACE_GIT_AUTHOR") or "CowAgent <cowagent@local>").encode("utf-8")
        try:
            porcelain.commit(self.workspace_root, msg, author=author, committer=author)
        except Exception:
            self._last_synced_mtime = current_mtime
            return

        try:
            porcelain.push(self.workspace_root, self.git_url, self.branch)
        except Exception as e:
            logger.warning(f"[WorkspacePartialGitSync] push failed: {e}")
            return

        self._last_synced_mtime = current_mtime
        logger.info("[WorkspacePartialGitSync] synced")
