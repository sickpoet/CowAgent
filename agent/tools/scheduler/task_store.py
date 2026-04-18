"""
Task storage management for scheduler
"""

import json
import os
import sqlite3
import threading
from urllib.parse import urlsplit, urlunsplit
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional
from common.utils import expand_path
from common.log import logger

try:
    from zoneinfo import ZoneInfo

    _BEIJING_TZ = ZoneInfo("Asia/Shanghai")
except Exception:
    _BEIJING_TZ = timezone(timedelta(hours=8))


def _now_beijing_iso() -> str:
    return datetime.now(tz=_BEIJING_TZ).isoformat()


def _get_database_url(explicit: Optional[str] = None) -> str:
    if explicit is not None:
        return explicit.strip()
    env_url = (os.environ.get("DATABASE_URL") or "").strip()
    if env_url:
        return env_url
    try:
        from config import conf
        return (conf().get("database_url") or "").strip()
    except Exception:
        return ""


def _mask_db_url(db_url: str) -> str:
    if not db_url:
        return ""
    try:
        parts = urlsplit(db_url)
        netloc = parts.netloc
        if "@" in netloc:
            userinfo, hostport = netloc.rsplit("@", 1)
            if ":" in userinfo:
                username = userinfo.split(":", 1)[0]
                netloc = f"{username}:***@{hostport}"
            else:
                netloc = f"{userinfo}@{hostport}"
        return urlunsplit((parts.scheme, netloc, parts.path, "", ""))
    except Exception:
        return "<redacted>"


class _SQLiteTaskStore:
    def __init__(self, store_path: str):
        if store_path.lower().endswith(".json"):
            self.legacy_json_path = store_path
            store_path = store_path[:-5] + ".db"
        else:
            candidate = os.path.join(os.path.dirname(store_path), "tasks.json")
            self.legacy_json_path = candidate if os.path.exists(candidate) else None

        self.store_path = store_path
        self.lock = threading.Lock()
        self._ensure_store_dir()
        self._init_db()
        self._maybe_migrate_from_json()

    def _ensure_store_dir(self):
        store_dir = os.path.dirname(self.store_path)
        os.makedirs(store_dir, exist_ok=True)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.store_path, timeout=10, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=5000")
        return conn

    def _init_db(self):
        with self.lock:
            conn = self._connect()
            try:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS scheduler_tasks (
                        id TEXT PRIMARY KEY,
                        enabled INTEGER NOT NULL DEFAULT 1,
                        next_run_at TEXT,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        data TEXT NOT NULL
                    )
                    """
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_scheduler_tasks_enabled_next_run ON scheduler_tasks(enabled, next_run_at)"
                )
                conn.commit()
            finally:
                conn.close()

    def _maybe_migrate_from_json(self) -> None:
        json_path = self.legacy_json_path
        if not json_path or not os.path.exists(json_path):
            return

        with self.lock:
            conn = self._connect()
            try:
                existing = conn.execute("SELECT COUNT(1) FROM scheduler_tasks").fetchone()[0]
                if existing:
                    return

                try:
                    with open(json_path, "r", encoding="utf-8") as f:
                        payload = json.load(f) or {}
                except Exception:
                    return

                tasks = payload.get("tasks", {})
                if not isinstance(tasks, dict) or not tasks:
                    return

                now = _now_beijing_iso()
                rows = []
                for task_id, task in tasks.items():
                    if not isinstance(task, dict):
                        continue
                    tid = task.get("id") or task_id
                    if not tid:
                        continue
                    created_at = task.get("created_at") or now
                    updated_at = task.get("updated_at") or created_at
                    enabled = 1 if task.get("enabled", True) else 0
                    next_run_at = task.get("next_run_at")
                    data = json.dumps(task, ensure_ascii=False)
                    rows.append((tid, enabled, next_run_at, created_at, updated_at, data))

                if not rows:
                    return

                conn.executemany(
                    """
                    INSERT OR REPLACE INTO scheduler_tasks(id, enabled, next_run_at, created_at, updated_at, data)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    rows,
                )
                conn.commit()
                logger.info(f"[TaskStore] Migrated {len(rows)} task(s) from legacy json '{json_path}' to sqlite '{self.store_path}'")
            finally:
                conn.close()
    
    def load_tasks(self) -> Dict[str, dict]:
        """
        Load all tasks from storage
        
        Returns:
            Dictionary of task_id -> task_data
        """
        with self.lock:
            conn = self._connect()
            try:
                rows = conn.execute("SELECT id, data FROM scheduler_tasks").fetchall()
                out: Dict[str, dict] = {}
                for r in rows:
                    try:
                        task = json.loads(r["data"])
                        if isinstance(task, dict):
                            out[r["id"]] = task
                    except Exception:
                        continue
                return out
            finally:
                conn.close()
    
    def save_tasks(self, tasks: Dict[str, dict]):
        """
        Save all tasks to storage
        
        Args:
            tasks: Dictionary of task_id -> task_data
        """
        with self.lock:
            conn = self._connect()
            try:
                conn.execute("DELETE FROM scheduler_tasks")
                now = _now_beijing_iso()
                rows = []
                for task_id, task in (tasks or {}).items():
                    if not isinstance(task, dict):
                        continue
                    tid = task.get("id") or task_id
                    if not tid:
                        continue
                    created_at = task.get("created_at") or now
                    updated_at = task.get("updated_at") or now
                    enabled = 1 if task.get("enabled", True) else 0
                    next_run_at = task.get("next_run_at")
                    data = json.dumps(task, ensure_ascii=False)
                    rows.append((tid, enabled, next_run_at, created_at, updated_at, data))

                if rows:
                    conn.executemany(
                        """
                        INSERT INTO scheduler_tasks(id, enabled, next_run_at, created_at, updated_at, data)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        rows,
                    )
                conn.commit()
            finally:
                conn.close()
    
    def add_task(self, task: dict) -> bool:
        """
        Add a new task
        
        Args:
            task: Task data dictionary
            
        Returns:
            True if successful
        """
        task_id = task.get("id")

        if not task_id:
            raise ValueError("Task must have an 'id' field")

        with self.lock:
            conn = self._connect()
            try:
                exists = conn.execute(
                    "SELECT 1 FROM scheduler_tasks WHERE id = ? LIMIT 1", (task_id,)
                ).fetchone()
                if exists:
                    raise ValueError(f"Task with id '{task_id}' already exists")

                now = _now_beijing_iso()
                created_at = task.get("created_at") or now
                updated_at = task.get("updated_at") or created_at
                enabled = 1 if task.get("enabled", True) else 0
                next_run_at = task.get("next_run_at")
                data = json.dumps(task, ensure_ascii=False)

                conn.execute(
                    """
                    INSERT INTO scheduler_tasks(id, enabled, next_run_at, created_at, updated_at, data)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (task_id, enabled, next_run_at, created_at, updated_at, data),
                )
                conn.commit()
            finally:
                conn.close()
        return True
    
    def update_task(self, task_id: str, updates: dict) -> bool:
        """
        Update an existing task
        
        Args:
            task_id: Task ID
            updates: Dictionary of fields to update
            
        Returns:
            True if successful
        """
        with self.lock:
            conn = self._connect()
            try:
                row = conn.execute(
                    "SELECT data, created_at FROM scheduler_tasks WHERE id = ?",
                    (task_id,),
                ).fetchone()
                if not row:
                    raise ValueError(f"Task '{task_id}' not found")

                try:
                    task = json.loads(row["data"]) or {}
                except Exception:
                    task = {}
                if not isinstance(task, dict):
                    task = {}

                task.update(updates or {})
                task["id"] = task_id
                task["updated_at"] = _now_beijing_iso()
                if not task.get("created_at"):
                    task["created_at"] = row["created_at"]

                enabled = 1 if task.get("enabled", True) else 0
                next_run_at = task.get("next_run_at")
                data = json.dumps(task, ensure_ascii=False)
                conn.execute(
                    """
                    UPDATE scheduler_tasks
                    SET enabled = ?, next_run_at = ?, updated_at = ?, data = ?
                    WHERE id = ?
                    """,
                    (enabled, next_run_at, task["updated_at"], data, task_id),
                )
                conn.commit()
            finally:
                conn.close()
        return True
    
    def delete_task(self, task_id: str) -> bool:
        """
        Delete a task
        
        Args:
            task_id: Task ID
            
        Returns:
            True if successful
        """
        with self.lock:
            conn = self._connect()
            try:
                cur = conn.execute("DELETE FROM scheduler_tasks WHERE id = ?", (task_id,))
                if cur.rowcount == 0:
                    raise ValueError(f"Task '{task_id}' not found")
                conn.commit()
            finally:
                conn.close()
        return True
    
    def get_task(self, task_id: str) -> Optional[dict]:
        """
        Get a specific task
        
        Args:
            task_id: Task ID
            
        Returns:
            Task data or None if not found
        """
        with self.lock:
            conn = self._connect()
            try:
                row = conn.execute(
                    "SELECT data FROM scheduler_tasks WHERE id = ?",
                    (task_id,),
                ).fetchone()
                if not row:
                    return None
                try:
                    task = json.loads(row["data"])
                    return task if isinstance(task, dict) else None
                except Exception:
                    return None
            finally:
                conn.close()
    
    def list_tasks(self, enabled_only: bool = False) -> List[dict]:
        """
        List all tasks
        
        Args:
            enabled_only: If True, only return enabled tasks
            
        Returns:
            List of task dictionaries
        """
        with self.lock:
            conn = self._connect()
            try:
                if enabled_only:
                    rows = conn.execute(
                        """
                        SELECT data FROM scheduler_tasks
                        WHERE enabled = 1
                        ORDER BY (next_run_at IS NULL) ASC, next_run_at ASC
                        """
                    ).fetchall()
                else:
                    rows = conn.execute(
                        """
                        SELECT data FROM scheduler_tasks
                        ORDER BY (next_run_at IS NULL) ASC, next_run_at ASC
                        """
                    ).fetchall()

                tasks: List[dict] = []
                for r in rows:
                    try:
                        task = json.loads(r["data"])
                        if isinstance(task, dict):
                            tasks.append(task)
                    except Exception:
                        continue
                return tasks
            finally:
                conn.close()
    
    def enable_task(self, task_id: str, enabled: bool = True) -> bool:
        """
        Enable or disable a task
        
        Args:
            task_id: Task ID
            enabled: True to enable, False to disable
            
        Returns:
            True if successful
        """
        return self.update_task(task_id, {"enabled": enabled})


class _PostgresTaskStore:
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.lock = threading.Lock()
        self._init_db()

    def _connect(self):
        import psycopg2
        return psycopg2.connect(self.database_url)

    def _init_db(self):
        with self.lock:
            conn = self._connect()
            try:
                cur = conn.cursor()
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS scheduler_tasks (
                        id TEXT PRIMARY KEY,
                        enabled BOOLEAN NOT NULL DEFAULT TRUE,
                        next_run_at TIMESTAMPTZ NULL,
                        created_at TIMESTAMPTZ NOT NULL,
                        updated_at TIMESTAMPTZ NOT NULL,
                        data JSONB NOT NULL
                    )
                    """
                )
                cur.execute(
                    "CREATE INDEX IF NOT EXISTS idx_scheduler_tasks_enabled_next_run ON scheduler_tasks(enabled, next_run_at)"
                )
                conn.commit()
            finally:
                conn.close()

    def _is_empty(self) -> bool:
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(1) FROM scheduler_tasks")
            return int(cur.fetchone()[0]) == 0
        finally:
            conn.close()

    def migrate_from_sqlite(self, sqlite_path: str) -> int:
        if not sqlite_path or not os.path.exists(sqlite_path):
            return 0
        if not self._is_empty():
            return 0

        conn_sqlite = sqlite3.connect(sqlite_path)
        conn_sqlite.row_factory = sqlite3.Row
        try:
            rows = conn_sqlite.execute("SELECT id, enabled, next_run_at, created_at, updated_at, data FROM scheduler_tasks").fetchall()
        except Exception:
            return 0
        finally:
            conn_sqlite.close()

        if not rows:
            return 0

        now_iso = _now_beijing_iso()
        payloads = []
        for r in rows:
            tid = r["id"]
            enabled = bool(r["enabled"])
            next_run_at = r["next_run_at"]
            created_at = r["created_at"] or now_iso
            updated_at = r["updated_at"] or created_at
            try:
                data = json.loads(r["data"])
            except Exception:
                data = {}
            if not isinstance(data, dict):
                data = {}
            payloads.append((tid, enabled, next_run_at, created_at, updated_at, json.dumps(data, ensure_ascii=False)))

        import psycopg2.extras
        with self.lock:
            conn = self._connect()
            try:
                cur = conn.cursor()
                psycopg2.extras.execute_batch(
                    cur,
                    """
                    INSERT INTO scheduler_tasks(id, enabled, next_run_at, created_at, updated_at, data)
                    VALUES (%s, %s, %s, %s, %s, %s::jsonb)
                    ON CONFLICT (id) DO UPDATE
                    SET enabled = EXCLUDED.enabled,
                        next_run_at = EXCLUDED.next_run_at,
                        created_at = EXCLUDED.created_at,
                        updated_at = EXCLUDED.updated_at,
                        data = EXCLUDED.data
                    """,
                    payloads,
                    page_size=200,
                )
                conn.commit()
                logger.info(f"[TaskStore] Migrated {len(payloads)} task(s) from sqlite '{sqlite_path}' to postgres")
            finally:
                conn.close()
        return len(payloads)

    def load_tasks(self) -> Dict[str, dict]:
        with self.lock:
            conn = self._connect()
            try:
                cur = conn.cursor()
                cur.execute("SELECT id, data FROM scheduler_tasks")
                out: Dict[str, dict] = {}
                for tid, data in cur.fetchall():
                    if isinstance(data, dict):
                        out[tid] = data
                    else:
                        try:
                            decoded = json.loads(data)
                            if isinstance(decoded, dict):
                                out[tid] = decoded
                        except Exception:
                            continue
                return out
            finally:
                conn.close()

    def save_tasks(self, tasks: Dict[str, dict]):
        import psycopg2.extras
        now_iso = _now_beijing_iso()
        rows = []
        for task_id, task in (tasks or {}).items():
            if not isinstance(task, dict):
                continue
            tid = task.get("id") or task_id
            if not tid:
                continue
            created_at = task.get("created_at") or now_iso
            updated_at = task.get("updated_at") or now_iso
            enabled = bool(task.get("enabled", True))
            next_run_at = task.get("next_run_at")
            rows.append((tid, enabled, next_run_at, created_at, updated_at, json.dumps(task, ensure_ascii=False)))

        with self.lock:
            conn = self._connect()
            try:
                cur = conn.cursor()
                cur.execute("TRUNCATE TABLE scheduler_tasks")
                if rows:
                    psycopg2.extras.execute_batch(
                        cur,
                        """
                        INSERT INTO scheduler_tasks(id, enabled, next_run_at, created_at, updated_at, data)
                        VALUES (%s, %s, %s, %s, %s, %s::jsonb)
                        """,
                        rows,
                        page_size=200,
                    )
                conn.commit()
            finally:
                conn.close()

    def add_task(self, task: dict) -> bool:
        task_id = task.get("id")
        if not task_id:
            raise ValueError("Task must have an 'id' field")

        now_iso = _now_beijing_iso()
        created_at = task.get("created_at") or now_iso
        updated_at = task.get("updated_at") or created_at
        enabled = bool(task.get("enabled", True))
        next_run_at = task.get("next_run_at")
        data = json.dumps(task, ensure_ascii=False)

        with self.lock:
            conn = self._connect()
            try:
                cur = conn.cursor()
                cur.execute(
                    """
                    INSERT INTO scheduler_tasks(id, enabled, next_run_at, created_at, updated_at, data)
                    VALUES (%s, %s, %s, %s, %s, %s::jsonb)
                    """,
                    (task_id, enabled, next_run_at, created_at, updated_at, data),
                )
                conn.commit()
                return True
            except Exception as e:
                conn.rollback()
                if "duplicate key value" in str(e) or "unique constraint" in str(e):
                    raise ValueError(f"Task with id '{task_id}' already exists")
                raise
            finally:
                conn.close()

    def update_task(self, task_id: str, updates: dict) -> bool:
        with self.lock:
            conn = self._connect()
            try:
                cur = conn.cursor()
                cur.execute("SELECT data, created_at FROM scheduler_tasks WHERE id = %s", (task_id,))
                row = cur.fetchone()
                if not row:
                    raise ValueError(f"Task '{task_id}' not found")
                raw_data, created_at = row
                task = raw_data if isinstance(raw_data, dict) else {}
                if not isinstance(task, dict):
                    task = {}
                task.update(updates or {})
                task["id"] = task_id
                task["updated_at"] = _now_beijing_iso()
                if not task.get("created_at"):
                    task["created_at"] = created_at.isoformat() if hasattr(created_at, "isoformat") else str(created_at)

                enabled = bool(task.get("enabled", True))
                next_run_at = task.get("next_run_at")
                data = json.dumps(task, ensure_ascii=False)
                cur.execute(
                    """
                    UPDATE scheduler_tasks
                    SET enabled = %s, next_run_at = %s, updated_at = %s, data = %s::jsonb
                    WHERE id = %s
                    """,
                    (enabled, next_run_at, task["updated_at"], data, task_id),
                )
                conn.commit()
                return True
            finally:
                conn.close()

    def delete_task(self, task_id: str) -> bool:
        with self.lock:
            conn = self._connect()
            try:
                cur = conn.cursor()
                cur.execute("DELETE FROM scheduler_tasks WHERE id = %s", (task_id,))
                if cur.rowcount == 0:
                    raise ValueError(f"Task '{task_id}' not found")
                conn.commit()
                return True
            finally:
                conn.close()

    def get_task(self, task_id: str) -> Optional[dict]:
        with self.lock:
            conn = self._connect()
            try:
                cur = conn.cursor()
                cur.execute("SELECT data FROM scheduler_tasks WHERE id = %s", (task_id,))
                row = cur.fetchone()
                if not row:
                    return None
                data = row[0]
                if isinstance(data, dict):
                    return data
                try:
                    decoded = json.loads(data)
                    return decoded if isinstance(decoded, dict) else None
                except Exception:
                    return None
            finally:
                conn.close()

    def list_tasks(self, enabled_only: bool = False) -> List[dict]:
        with self.lock:
            conn = self._connect()
            try:
                cur = conn.cursor()
                if enabled_only:
                    cur.execute(
                        "SELECT data FROM scheduler_tasks WHERE enabled = TRUE ORDER BY (next_run_at IS NULL) ASC, next_run_at ASC"
                    )
                else:
                    cur.execute(
                        "SELECT data FROM scheduler_tasks ORDER BY (next_run_at IS NULL) ASC, next_run_at ASC"
                    )
                out: List[dict] = []
                for (data,) in cur.fetchall():
                    if isinstance(data, dict):
                        out.append(data)
                    else:
                        try:
                            decoded = json.loads(data)
                            if isinstance(decoded, dict):
                                out.append(decoded)
                        except Exception:
                            continue
                return out
            finally:
                conn.close()

    def enable_task(self, task_id: str, enabled: bool = True) -> bool:
        return self.update_task(task_id, {"enabled": enabled})


class TaskStore:
    def __init__(self, store_path: str = None, database_url: Optional[str] = None):
        if store_path is None:
            home = expand_path("~")
            store_path = os.path.join(home, "cow", "scheduler", "tasks.db")

        db_url = _get_database_url(database_url)
        if db_url:
            impl = _PostgresTaskStore(db_url)
            migrated = impl.migrate_from_sqlite(store_path if store_path.lower().endswith(".db") else "")
            self._impl = impl
            self.backend = "postgres"
            self.store_path = store_path
            self.database_url = db_url
            logger.info(f"[TaskStore] Initialized backend=postgres db='{_mask_db_url(db_url)}' migrated={migrated}")
        else:
            self._impl = _SQLiteTaskStore(store_path)
            self.backend = "sqlite"
            self.store_path = getattr(self._impl, "store_path", store_path)
            self.database_url = ""
            logger.info(f"[TaskStore] Initialized backend=sqlite store='{self.store_path}'")

        try:
            for task in self.list_tasks() or []:
                if not isinstance(task, dict):
                    continue
                tid = str(task.get("id", ""))
                if tid == "db_scheduler_smoke_test" or tid.startswith("db_scheduler_"):
                    try:
                        self.delete_task(tid)
                    except Exception:
                        pass
        except Exception:
            pass

    def load_tasks(self) -> Dict[str, dict]:
        return self._impl.load_tasks()

    def save_tasks(self, tasks: Dict[str, dict]):
        self._impl.save_tasks(tasks)
        logger.info(f"[TaskStore] save_tasks ok backend={self.backend} count={len(tasks or {})}")

    def add_task(self, task: dict) -> bool:
        ok = self._impl.add_task(task)
        tid = (task or {}).get("id", "")
        logger.info(f"[TaskStore] add_task ok backend={self.backend} id={tid}")
        return ok

    def update_task(self, task_id: str, updates: dict) -> bool:
        ok = self._impl.update_task(task_id, updates)
        keys = list((updates or {}).keys())
        logger.info(f"[TaskStore] update_task ok backend={self.backend} id={task_id} keys={keys}")
        return ok

    def delete_task(self, task_id: str) -> bool:
        ok = self._impl.delete_task(task_id)
        logger.info(f"[TaskStore] delete_task ok backend={self.backend} id={task_id}")
        return ok

    def get_task(self, task_id: str) -> Optional[dict]:
        task = self._impl.get_task(task_id)
        logger.debug(f"[TaskStore] get_task backend={self.backend} id={task_id} found={bool(task)}")
        return task

    def list_tasks(self, enabled_only: bool = False) -> List[dict]:
        tasks = self._impl.list_tasks(enabled_only=enabled_only)
        logger.debug(f"[TaskStore] list_tasks backend={self.backend} enabled_only={enabled_only} count={len(tasks or [])}")
        return tasks

    def enable_task(self, task_id: str, enabled: bool = True) -> bool:
        ok = self._impl.enable_task(task_id, enabled=enabled)
        logger.info(f"[TaskStore] enable_task ok backend={self.backend} id={task_id} enabled={enabled}")
        return ok
