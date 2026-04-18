"""
Task storage management for scheduler
"""

import json
import os
import sqlite3
import threading
from datetime import datetime
from typing import Dict, List, Optional
from common.utils import expand_path


class TaskStore:
    """
    Manages persistent storage of scheduled tasks
    """
    
    def __init__(self, store_path: str = None):
        """
        Initialize task store
        
        Args:
            store_path: Path to tasks DB file. Defaults to ~/cow/scheduler/tasks.db
        """
        if store_path is None:
            # Default to ~/cow/scheduler/tasks.db
            home = expand_path("~")
            store_path = os.path.join(home, "cow", "scheduler", "tasks.db")

        self.legacy_json_path: Optional[str] = None
        if store_path.lower().endswith(".json"):
            self.legacy_json_path = store_path
            store_path = store_path[:-5] + ".db"
        else:
            candidate = os.path.join(os.path.dirname(store_path), "tasks.json")
            if os.path.exists(candidate):
                self.legacy_json_path = candidate

        self.store_path = store_path
        self.lock = threading.Lock()
        self._ensure_store_dir()
        self._init_db()
        self._maybe_migrate_from_json()
    
    def _ensure_store_dir(self):
        """Ensure the storage directory exists"""
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

                now = datetime.now().isoformat()
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
                now = datetime.now().isoformat()
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

                now = datetime.now().isoformat()
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
                task["updated_at"] = datetime.now().isoformat()
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
