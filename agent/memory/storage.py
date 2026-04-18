"""
Storage layer for memory using SQLite + FTS5

Provides vector and keyword search capabilities
"""

from __future__ import annotations
import os
import sqlite3
import json
import hashlib
import threading
from typing import List, Dict, Optional, Any
from pathlib import Path
from dataclasses import dataclass


@dataclass
class MemoryChunk:
    """Represents a memory chunk with text and embedding"""
    id: str
    user_id: Optional[str]
    scope: str  # "shared" | "user" | "session"
    source: str  # "memory" | "session"
    path: str
    start_line: int
    end_line: int
    text: str
    embedding: Optional[List[float]]
    hash: str
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class SearchResult:
    """Search result with score and snippet"""
    path: str
    start_line: int
    end_line: int
    score: float
    snippet: str
    source: str
    user_id: Optional[str] = None


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


class _SQLiteMemoryStorage:
    """SQLite-based storage with FTS5 for keyword search"""
    
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None
        self.fts5_available = False  # Track FTS5 availability
        self._init_db()
    
    def _check_fts5_support(self) -> bool:
        """Check if SQLite has FTS5 support"""
        try:
            self.conn.execute("CREATE VIRTUAL TABLE IF NOT EXISTS fts5_test USING fts5(test)")
            self.conn.execute("DROP TABLE IF EXISTS fts5_test")
            return True
        except sqlite3.OperationalError as e:
            if "no such module: fts5" in str(e):
                return False
            raise
    
    def _init_db(self):
        """Initialize database with schema"""
        try:
            self.conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
            self.conn.row_factory = sqlite3.Row
            
            # Check FTS5 support
            self.fts5_available = self._check_fts5_support()
            if not self.fts5_available:
                from common.log import logger
                logger.debug("[MemoryStorage] FTS5 not available, using LIKE-based keyword search")
            
            # Check database integrity
            try:
                result = self.conn.execute("PRAGMA integrity_check").fetchone()
                if result[0] != 'ok':
                    print(f"⚠️  Database integrity check failed: {result[0]}")
                    print(f"   Recreating database...")
                    self.conn.close()
                    self.conn = None
                    # Remove corrupted database
                    self.db_path.unlink(missing_ok=True)
                    # Remove WAL files
                    Path(str(self.db_path) + '-wal').unlink(missing_ok=True)
                    Path(str(self.db_path) + '-shm').unlink(missing_ok=True)
                    # Reconnect to create new database
                    self.conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
                    self.conn.row_factory = sqlite3.Row
            except sqlite3.DatabaseError:
                # Database is corrupted, recreate it
                print(f"⚠️  Database is corrupted, recreating...")
                if self.conn:
                    self.conn.close()
                    self.conn = None
                self.db_path.unlink(missing_ok=True)
                Path(str(self.db_path) + '-wal').unlink(missing_ok=True)
                Path(str(self.db_path) + '-shm').unlink(missing_ok=True)
                self.conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
                self.conn.row_factory = sqlite3.Row
            
            # Enable WAL mode for better concurrency
            self.conn.execute("PRAGMA journal_mode=WAL")
            # Set busy timeout to avoid "database is locked" errors
            self.conn.execute("PRAGMA busy_timeout=5000")
        except Exception as e:
            print(f"⚠️  Unexpected error during database initialization: {e}")
            raise
        
        # Create chunks table with embeddings
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS chunks (
                id TEXT PRIMARY KEY,
                user_id TEXT,
                scope TEXT NOT NULL DEFAULT 'shared',
                source TEXT NOT NULL DEFAULT 'memory',
                path TEXT NOT NULL,
                start_line INTEGER NOT NULL,
                end_line INTEGER NOT NULL,
                text TEXT NOT NULL,
                embedding TEXT,
                hash TEXT NOT NULL,
                metadata TEXT,
                created_at INTEGER DEFAULT (strftime('%s', 'now')),
                updated_at INTEGER DEFAULT (strftime('%s', 'now'))
            )
        """)
        
        # Create indexes
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_chunks_user 
            ON chunks(user_id)
        """)
        
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_chunks_scope 
            ON chunks(scope)
        """)
        
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_chunks_hash 
            ON chunks(path, hash)
        """)
        
        # Create FTS5 virtual table for keyword search (only if supported)
        if self.fts5_available:
            # Use default unicode61 tokenizer (stable and compatible)
            # For CJK support, we'll use LIKE queries as fallback
            self.conn.execute("""
                CREATE VIRTUAL TABLE IF NOT EXISTS chunks_fts USING fts5(
                    text,
                    id UNINDEXED,
                    user_id UNINDEXED,
                    path UNINDEXED,
                    source UNINDEXED,
                    scope UNINDEXED,
                    content='chunks',
                    content_rowid='rowid'
                )
            """)
            
            # Create triggers to keep FTS in sync
            self.conn.execute("""
                CREATE TRIGGER IF NOT EXISTS chunks_ai AFTER INSERT ON chunks BEGIN
                    INSERT INTO chunks_fts(rowid, text, id, user_id, path, source, scope)
                    VALUES (new.rowid, new.text, new.id, new.user_id, new.path, new.source, new.scope);
                END
            """)
            
            self.conn.execute("""
                CREATE TRIGGER IF NOT EXISTS chunks_ad AFTER DELETE ON chunks BEGIN
                    DELETE FROM chunks_fts WHERE rowid = old.rowid;
                END
            """)
            
            self.conn.execute("""
                CREATE TRIGGER IF NOT EXISTS chunks_au AFTER UPDATE ON chunks BEGIN
                    UPDATE chunks_fts SET text = new.text, id = new.id,
                                         user_id = new.user_id, path = new.path, source = new.source, scope = new.scope
                    WHERE rowid = new.rowid;
                END
            """)
        
        # Create files metadata table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS files (
                path TEXT PRIMARY KEY,
                source TEXT NOT NULL DEFAULT 'memory',
                hash TEXT NOT NULL,
                mtime INTEGER NOT NULL,
                size INTEGER NOT NULL,
                updated_at INTEGER DEFAULT (strftime('%s', 'now'))
            )
        """)
        
        self.conn.commit()
    
    def save_chunk(self, chunk: MemoryChunk):
        """Save a memory chunk"""
        self.conn.execute("""
            INSERT OR REPLACE INTO chunks 
            (id, user_id, scope, source, path, start_line, end_line, text, embedding, hash, metadata, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, strftime('%s', 'now'))
        """, (
            chunk.id,
            chunk.user_id,
            chunk.scope,
            chunk.source,
            chunk.path,
            chunk.start_line,
            chunk.end_line,
            chunk.text,
            json.dumps(chunk.embedding) if chunk.embedding else None,
            chunk.hash,
            json.dumps(chunk.metadata) if chunk.metadata else None
        ))
        self.conn.commit()
    
    def save_chunks_batch(self, chunks: List[MemoryChunk]):
        """Save multiple chunks in a batch"""
        self.conn.executemany("""
            INSERT OR REPLACE INTO chunks 
            (id, user_id, scope, source, path, start_line, end_line, text, embedding, hash, metadata, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, strftime('%s', 'now'))
        """, [
            (
                c.id, c.user_id, c.scope, c.source, c.path,
                c.start_line, c.end_line, c.text,
                json.dumps(c.embedding) if c.embedding else None,
                c.hash,
                json.dumps(c.metadata) if c.metadata else None
            )
            for c in chunks
        ])
        self.conn.commit()
    
    def get_chunk(self, chunk_id: str) -> Optional[MemoryChunk]:
        """Get a chunk by ID"""
        row = self.conn.execute("""
            SELECT * FROM chunks WHERE id = ?
        """, (chunk_id,)).fetchone()
        
        if not row:
            return None
        
        return self._row_to_chunk(row)
    
    def search_vector(
        self,
        query_embedding: List[float],
        user_id: Optional[str] = None,
        scopes: List[str] = None,
        limit: int = 10
    ) -> List[SearchResult]:
        """
        Vector similarity search using in-memory cosine similarity
        (sqlite-vec can be added later for better performance)
        """
        if scopes is None:
            scopes = ["shared"]
            if user_id:
                scopes.append("user")
        
        # Build query
        scope_placeholders = ','.join('?' * len(scopes))
        params = scopes
        
        if user_id:
            query = f"""
                SELECT * FROM chunks 
                WHERE scope IN ({scope_placeholders})
                AND (scope = 'shared' OR user_id = ?)
                AND embedding IS NOT NULL
            """
            params.append(user_id)
        else:
            query = f"""
                SELECT * FROM chunks 
                WHERE scope IN ({scope_placeholders})
                AND embedding IS NOT NULL
            """
        
        rows = self.conn.execute(query, params).fetchall()
        
        # Calculate cosine similarity
        results = []
        for row in rows:
            embedding = json.loads(row['embedding'])
            similarity = self._cosine_similarity(query_embedding, embedding)
            
            if similarity > 0:
                results.append((similarity, row))
        
        # Sort by similarity and limit
        results.sort(key=lambda x: x[0], reverse=True)
        results = results[:limit]
        
        return [
            SearchResult(
                path=row['path'],
                start_line=row['start_line'],
                end_line=row['end_line'],
                score=score,
                snippet=self._truncate_text(row['text'], 500),
                source=row['source'],
                user_id=row['user_id']
            )
            for score, row in results
        ]
    
    def search_keyword(
        self,
        query: str,
        user_id: Optional[str] = None,
        scopes: List[str] = None,
        limit: int = 10
    ) -> List[SearchResult]:
        """
        Keyword search using FTS5 + LIKE fallback
        
        Strategy:
        1. If FTS5 available: Try FTS5 search first (good for English and word-based languages)
        2. If no FTS5 or no results and query contains CJK: Use LIKE search
        """
        if scopes is None:
            scopes = ["shared"]
            if user_id:
                scopes.append("user")
        
        # Try FTS5 search first (if available)
        if self.fts5_available:
            fts_results = self._search_fts5(query, user_id, scopes, limit)
            if fts_results:
                return fts_results
        
        # Fallback to LIKE search (always for CJK, or if FTS5 not available)
        if not self.fts5_available or _SQLiteMemoryStorage._contains_cjk(query):
            return self._search_like(query, user_id, scopes, limit)
        
        return []
    
    def _search_fts5(
        self,
        query: str,
        user_id: Optional[str],
        scopes: List[str],
        limit: int
    ) -> List[SearchResult]:
        """FTS5 full-text search"""
        fts_query = self._build_fts_query(query)
        if not fts_query:
            return []
        
        scope_placeholders = ','.join('?' * len(scopes))
        params = [fts_query] + scopes
        
        if user_id:
            sql_query = f"""
                SELECT chunks.*, bm25(chunks_fts) as rank
                FROM chunks_fts
                JOIN chunks ON chunks.id = chunks_fts.id
                WHERE chunks_fts MATCH ? 
                AND chunks.scope IN ({scope_placeholders})
                AND (chunks.scope = 'shared' OR chunks.user_id = ?)
                ORDER BY rank
                LIMIT ?
            """
            params.extend([user_id, limit])
        else:
            sql_query = f"""
                SELECT chunks.*, bm25(chunks_fts) as rank
                FROM chunks_fts
                JOIN chunks ON chunks.id = chunks_fts.id
                WHERE chunks_fts MATCH ? 
                AND chunks.scope IN ({scope_placeholders})
                ORDER BY rank
                LIMIT ?
            """
            params.append(limit)
        
        try:
            rows = self.conn.execute(sql_query, params).fetchall()
            return [
                SearchResult(
                    path=row['path'],
                    start_line=row['start_line'],
                    end_line=row['end_line'],
                    score=self._bm25_rank_to_score(row['rank']),
                    snippet=self._truncate_text(row['text'], 500),
                    source=row['source'],
                    user_id=row['user_id']
                )
                for row in rows
            ]
        except Exception:
            return []
    
    def _search_like(
        self,
        query: str,
        user_id: Optional[str],
        scopes: List[str],
        limit: int
    ) -> List[SearchResult]:
        """LIKE-based search for CJK characters"""
        import re
        # Extract CJK words (2+ characters)
        cjk_words = re.findall(r'[\u4e00-\u9fff]{2,}', query)
        if not cjk_words:
            return []
        
        scope_placeholders = ','.join('?' * len(scopes))
        
        # Build LIKE conditions for each word
        like_conditions = []
        params = []
        for word in cjk_words:
            like_conditions.append("text LIKE ?")
            params.append(f'%{word}%')
        
        where_clause = ' OR '.join(like_conditions)
        params.extend(scopes)
        
        if user_id:
            sql_query = f"""
                SELECT * FROM chunks
                WHERE ({where_clause})
                AND scope IN ({scope_placeholders})
                AND (scope = 'shared' OR user_id = ?)
                LIMIT ?
            """
            params.extend([user_id, limit])
        else:
            sql_query = f"""
                SELECT * FROM chunks
                WHERE ({where_clause})
                AND scope IN ({scope_placeholders})
                LIMIT ?
            """
            params.append(limit)
        
        try:
            rows = self.conn.execute(sql_query, params).fetchall()
            return [
                SearchResult(
                    path=row['path'],
                    start_line=row['start_line'],
                    end_line=row['end_line'],
                    score=0.5,  # Fixed score for LIKE search
                    snippet=self._truncate_text(row['text'], 500),
                    source=row['source'],
                    user_id=row['user_id']
                )
                for row in rows
            ]
        except Exception:
            return []
    
    def delete_by_path(self, path: str):
        """Delete all chunks from a file"""
        self.conn.execute("""
            DELETE FROM chunks WHERE path = ?
        """, (path,))
        self.conn.commit()
    
    def get_file_hash(self, path: str) -> Optional[str]:
        """Get stored file hash"""
        row = self.conn.execute("""
            SELECT hash FROM files WHERE path = ?
        """, (path,)).fetchone()
        return row['hash'] if row else None
    
    def update_file_metadata(self, path: str, source: str, file_hash: str, mtime: int, size: int):
        """Update file metadata"""
        self.conn.execute("""
            INSERT OR REPLACE INTO files (path, source, hash, mtime, size, updated_at)
            VALUES (?, ?, ?, ?, ?, strftime('%s', 'now'))
        """, (path, source, file_hash, mtime, size))
        self.conn.commit()
    
    def get_stats(self) -> Dict[str, int]:
        """Get storage statistics"""
        chunks_count = self.conn.execute("""
            SELECT COUNT(*) as cnt FROM chunks
        """).fetchone()['cnt']
        
        files_count = self.conn.execute("""
            SELECT COUNT(*) as cnt FROM files
        """).fetchone()['cnt']
        
        return {
            'chunks': chunks_count,
            'files': files_count
        }
    
    def close(self):
        """Close database connection"""
        if self.conn:
            try:
                self.conn.commit()  # Ensure all changes are committed
                self.conn.close()
                self.conn = None  # Mark as closed
            except Exception as e:
                print(f"⚠️  Error closing database connection: {e}")
    
    def __del__(self):
        """Destructor to ensure connection is closed"""
        try:
            self.close()
        except Exception:
            pass  # Ignore errors during cleanup
    
    # Helper methods
    
    def _row_to_chunk(self, row) -> MemoryChunk:
        """Convert database row to MemoryChunk"""
        return MemoryChunk(
            id=row['id'],
            user_id=row['user_id'],
            scope=row['scope'],
            source=row['source'],
            path=row['path'],
            start_line=row['start_line'],
            end_line=row['end_line'],
            text=row['text'],
            embedding=json.loads(row['embedding']) if row['embedding'] else None,
            hash=row['hash'],
            metadata=json.loads(row['metadata']) if row['metadata'] else None
        )
    
    @staticmethod
    def _cosine_similarity(vec1: List[float], vec2: List[float]) -> float:
        """Calculate cosine similarity between two vectors"""
        if len(vec1) != len(vec2):
            return 0.0
        
        dot_product = sum(a * b for a, b in zip(vec1, vec2))
        norm1 = sum(a * a for a in vec1) ** 0.5
        norm2 = sum(b * b for b in vec2) ** 0.5
        
        if norm1 == 0 or norm2 == 0:
            return 0.0
        
        return dot_product / (norm1 * norm2)
    
    @staticmethod
    def _contains_cjk(text: str) -> bool:
        """Check if text contains CJK (Chinese/Japanese/Korean) characters"""
        import re
        return bool(re.search(r'[\u4e00-\u9fff]', text))
    
    @staticmethod
    def _build_fts_query(raw_query: str) -> Optional[str]:
        """
        Build FTS5 query from raw text
        
        Works best for English and word-based languages.
        For CJK characters, LIKE search will be used as fallback.
        """
        import re
        # Extract words (primarily English words and numbers)
        tokens = re.findall(r'[A-Za-z0-9_]+', raw_query)
        if not tokens:
            return None
        
        # Quote tokens for exact matching
        quoted = [f'"{t}"' for t in tokens]
        # Use OR for more flexible matching
        return ' OR '.join(quoted)
    
    @staticmethod
    def _bm25_rank_to_score(rank: float) -> float:
        """Convert BM25 rank to 0-1 score"""
        normalized = max(0, rank) if rank is not None else 999
        return 1 / (1 + normalized)
    
    @staticmethod
    def _truncate_text(text: str, max_chars: int) -> str:
        """Truncate text to max characters"""
        if len(text) <= max_chars:
            return text
        return text[:max_chars] + "..."
    
    @staticmethod
    def compute_hash(content: str) -> str:
        """Compute SHA256 hash of content"""
        return hashlib.sha256(content.encode('utf-8')).hexdigest()


class _PostgresMemoryStorage:
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
                    CREATE TABLE IF NOT EXISTS chunks (
                        id TEXT PRIMARY KEY,
                        user_id TEXT,
                        scope TEXT NOT NULL DEFAULT 'shared',
                        source TEXT NOT NULL DEFAULT 'memory',
                        path TEXT NOT NULL,
                        start_line INTEGER NOT NULL,
                        end_line INTEGER NOT NULL,
                        text TEXT NOT NULL,
                        embedding JSONB,
                        hash TEXT NOT NULL,
                        metadata JSONB,
                        created_at BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM NOW())::BIGINT),
                        updated_at BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM NOW())::BIGINT)
                    )
                    """
                )
                cur.execute("CREATE INDEX IF NOT EXISTS idx_chunks_user ON chunks(user_id)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_chunks_scope ON chunks(scope)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_chunks_hash ON chunks(path, hash)")
                cur.execute(
                    "CREATE INDEX IF NOT EXISTS idx_chunks_tsv ON chunks USING GIN (to_tsvector('simple', text))"
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS files (
                        path TEXT PRIMARY KEY,
                        source TEXT NOT NULL DEFAULT 'memory',
                        hash TEXT NOT NULL,
                        mtime BIGINT NOT NULL,
                        size BIGINT NOT NULL,
                        updated_at BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM NOW())::BIGINT)
                    )
                    """
                )
                conn.commit()
            finally:
                conn.close()

    def save_chunk(self, chunk: MemoryChunk):
        with self.lock:
            conn = self._connect()
            try:
                cur = conn.cursor()
                cur.execute(
                    """
                    INSERT INTO chunks
                        (id, user_id, scope, source, path, start_line, end_line, text, embedding, hash, metadata, updated_at)
                    VALUES
                        (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s::jsonb, EXTRACT(EPOCH FROM NOW())::BIGINT)
                    ON CONFLICT (id) DO UPDATE SET
                        user_id = EXCLUDED.user_id,
                        scope = EXCLUDED.scope,
                        source = EXCLUDED.source,
                        path = EXCLUDED.path,
                        start_line = EXCLUDED.start_line,
                        end_line = EXCLUDED.end_line,
                        text = EXCLUDED.text,
                        embedding = EXCLUDED.embedding,
                        hash = EXCLUDED.hash,
                        metadata = EXCLUDED.metadata,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (
                        chunk.id,
                        chunk.user_id,
                        chunk.scope,
                        chunk.source,
                        chunk.path,
                        chunk.start_line,
                        chunk.end_line,
                        chunk.text,
                        json.dumps(chunk.embedding) if chunk.embedding else None,
                        chunk.hash,
                        json.dumps(chunk.metadata) if chunk.metadata else None,
                    ),
                )
                conn.commit()
            finally:
                conn.close()

    def save_chunks_batch(self, chunks: List[MemoryChunk]):
        if not chunks:
            return
        import psycopg2.extras
        rows = [
            (
                c.id,
                c.user_id,
                c.scope,
                c.source,
                c.path,
                c.start_line,
                c.end_line,
                c.text,
                json.dumps(c.embedding) if c.embedding else None,
                c.hash,
                json.dumps(c.metadata) if c.metadata else None,
            )
            for c in chunks
        ]
        with self.lock:
            conn = self._connect()
            try:
                cur = conn.cursor()
                psycopg2.extras.execute_batch(
                    cur,
                    """
                    INSERT INTO chunks
                        (id, user_id, scope, source, path, start_line, end_line, text, embedding, hash, metadata, updated_at)
                    VALUES
                        (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s::jsonb, EXTRACT(EPOCH FROM NOW())::BIGINT)
                    ON CONFLICT (id) DO UPDATE SET
                        user_id = EXCLUDED.user_id,
                        scope = EXCLUDED.scope,
                        source = EXCLUDED.source,
                        path = EXCLUDED.path,
                        start_line = EXCLUDED.start_line,
                        end_line = EXCLUDED.end_line,
                        text = EXCLUDED.text,
                        embedding = EXCLUDED.embedding,
                        hash = EXCLUDED.hash,
                        metadata = EXCLUDED.metadata,
                        updated_at = EXCLUDED.updated_at
                    """,
                    rows,
                    page_size=200,
                )
                conn.commit()
            finally:
                conn.close()

    def get_chunk(self, chunk_id: str) -> Optional[MemoryChunk]:
        with self.lock:
            conn = self._connect()
            try:
                cur = conn.cursor()
                cur.execute("SELECT id, user_id, scope, source, path, start_line, end_line, text, embedding, hash, metadata FROM chunks WHERE id = %s", (chunk_id,))
                row = cur.fetchone()
                if not row:
                    return None
                return MemoryChunk(
                    id=row[0],
                    user_id=row[1],
                    scope=row[2],
                    source=row[3],
                    path=row[4],
                    start_line=int(row[5]),
                    end_line=int(row[6]),
                    text=row[7],
                    embedding=row[8] if isinstance(row[8], list) else (json.loads(row[8]) if row[8] else None),
                    hash=row[9],
                    metadata=row[10] if isinstance(row[10], dict) else (json.loads(row[10]) if row[10] else None),
                )
            finally:
                conn.close()

    def search_vector(self, query_embedding: List[float], user_id: Optional[str] = None, scopes: List[str] = None, limit: int = 10) -> List[SearchResult]:
        if scopes is None:
            scopes = ["shared"]
            if user_id:
                scopes.append("user")

        params: List[Any] = [scopes]
        where_parts = ["scope = ANY(%s)", "embedding IS NOT NULL"]
        if user_id:
            where_parts.append("(scope = 'shared' OR user_id = %s)")
            params.append(user_id)
        where_sql = " AND ".join(where_parts)
        sql = f"SELECT path, start_line, end_line, text, source, user_id, embedding FROM chunks WHERE {where_sql} LIMIT 2000"

        with self.lock:
            conn = self._connect()
            try:
                cur = conn.cursor()
                cur.execute(sql, tuple(params))
                rows = cur.fetchall()
            finally:
                conn.close()

        scored = []
        for path, start_line, end_line, text, source, uid, embedding in rows:
            emb = embedding
            if not isinstance(emb, list):
                try:
                    emb = json.loads(emb) if emb else None
                except Exception:
                    emb = None
            if not emb:
                continue
            sim = _SQLiteMemoryStorage._cosine_similarity(query_embedding, emb)
            if sim > 0:
                scored.append((sim, path, start_line, end_line, text, source, uid))

        scored.sort(key=lambda x: x[0], reverse=True)
        scored = scored[:limit]
        return [
            SearchResult(
                path=path,
                start_line=int(start_line),
                end_line=int(end_line),
                score=float(score),
                snippet=_SQLiteMemoryStorage._truncate_text(text, 500),
                source=source,
                user_id=uid,
            )
            for score, path, start_line, end_line, text, source, uid in scored
        ]

    def search_keyword(self, query: str, user_id: Optional[str] = None, scopes: List[str] = None, limit: int = 10) -> List[SearchResult]:
        if scopes is None:
            scopes = ["shared"]
            if user_id:
                scopes.append("user")

        if _SQLiteMemoryStorage._contains_cjk(query):
            return self._search_like(query, user_id, scopes, limit)
        return self._search_fts(query, user_id, scopes, limit)

    def _search_fts(self, query: str, user_id: Optional[str], scopes: List[str], limit: int) -> List[SearchResult]:
        where_parts = ["scope = ANY(%s)", "to_tsvector('simple', text) @@ plainto_tsquery('simple', %s)"]
        params: List[Any] = [scopes, query]
        if user_id:
            where_parts.append("(scope = 'shared' OR user_id = %s)")
            params.append(user_id)
        where_sql = " AND ".join(where_parts)
        sql = f"""
            SELECT path, start_line, end_line, text, source, user_id
            FROM chunks
            WHERE {where_sql}
            LIMIT %s
        """
        params.append(limit)
        with self.lock:
            conn = self._connect()
            try:
                cur = conn.cursor()
                cur.execute(sql, tuple(params))
                rows = cur.fetchall()
            finally:
                conn.close()

        return [
            SearchResult(
                path=r[0],
                start_line=int(r[1]),
                end_line=int(r[2]),
                score=0.5,
                snippet=_SQLiteMemoryStorage._truncate_text(r[3], 500),
                source=r[4],
                user_id=r[5],
            )
            for r in rows
        ]

    def _search_like(self, query: str, user_id: Optional[str], scopes: List[str], limit: int) -> List[SearchResult]:
        import re
        cjk_words = re.findall(r'[\u4e00-\u9fff]{2,}', query)
        if not cjk_words:
            return []
        like_clauses = []
        params: List[Any] = [scopes]
        for w in cjk_words:
            like_clauses.append("text ILIKE %s")
            params.append(f"%{w}%")
        where_parts = [f"scope = ANY(%s)", "(" + " OR ".join(like_clauses) + ")"]
        if user_id:
            where_parts.append("(scope = 'shared' OR user_id = %s)")
            params.append(user_id)
        where_sql = " AND ".join(where_parts)
        sql = f"""
            SELECT path, start_line, end_line, text, source, user_id
            FROM chunks
            WHERE {where_sql}
            LIMIT %s
        """
        params.append(limit)
        with self.lock:
            conn = self._connect()
            try:
                cur = conn.cursor()
                cur.execute(sql, tuple(params))
                rows = cur.fetchall()
            finally:
                conn.close()

        return [
            SearchResult(
                path=r[0],
                start_line=int(r[1]),
                end_line=int(r[2]),
                score=0.5,
                snippet=_SQLiteMemoryStorage._truncate_text(r[3], 500),
                source=r[4],
                user_id=r[5],
            )
            for r in rows
        ]

    def delete_by_path(self, path: str):
        with self.lock:
            conn = self._connect()
            try:
                cur = conn.cursor()
                cur.execute("DELETE FROM chunks WHERE path = %s", (path,))
                conn.commit()
            finally:
                conn.close()

    def get_file_hash(self, path: str) -> Optional[str]:
        with self.lock:
            conn = self._connect()
            try:
                cur = conn.cursor()
                cur.execute("SELECT hash FROM files WHERE path = %s", (path,))
                row = cur.fetchone()
                return row[0] if row else None
            finally:
                conn.close()

    def update_file_metadata(self, path: str, source: str, file_hash: str, mtime: int, size: int):
        with self.lock:
            conn = self._connect()
            try:
                cur = conn.cursor()
                cur.execute(
                    """
                    INSERT INTO files(path, source, hash, mtime, size, updated_at)
                    VALUES (%s, %s, %s, %s, %s, EXTRACT(EPOCH FROM NOW())::BIGINT)
                    ON CONFLICT (path) DO UPDATE SET
                        source = EXCLUDED.source,
                        hash = EXCLUDED.hash,
                        mtime = EXCLUDED.mtime,
                        size = EXCLUDED.size,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (path, source, file_hash, int(mtime), int(size)),
                )
                conn.commit()
            finally:
                conn.close()

    def get_stats(self) -> Dict[str, int]:
        with self.lock:
            conn = self._connect()
            try:
                cur = conn.cursor()
                cur.execute("SELECT COUNT(*) FROM chunks")
                chunks_count = int(cur.fetchone()[0])
                cur.execute("SELECT COUNT(*) FROM files")
                files_count = int(cur.fetchone()[0])
                return {"chunks": chunks_count, "files": files_count}
            finally:
                conn.close()

    def close(self):
        return


class MemoryStorage:
    def __init__(self, db_path: Path, database_url: Optional[str] = None):
        db_url = _get_database_url(database_url)
        self._impl = _PostgresMemoryStorage(db_url) if db_url else _SQLiteMemoryStorage(db_path)

    def save_chunk(self, chunk: MemoryChunk):
        return self._impl.save_chunk(chunk)

    def save_chunks_batch(self, chunks: List[MemoryChunk]):
        return self._impl.save_chunks_batch(chunks)

    def get_chunk(self, chunk_id: str) -> Optional[MemoryChunk]:
        return self._impl.get_chunk(chunk_id)

    def search_vector(self, query_embedding: List[float], user_id: Optional[str] = None, scopes: List[str] = None, limit: int = 10) -> List[SearchResult]:
        return self._impl.search_vector(query_embedding, user_id=user_id, scopes=scopes, limit=limit)

    def search_keyword(self, query: str, user_id: Optional[str] = None, scopes: List[str] = None, limit: int = 10) -> List[SearchResult]:
        return self._impl.search_keyword(query, user_id=user_id, scopes=scopes, limit=limit)

    def delete_by_path(self, path: str):
        return self._impl.delete_by_path(path)

    def get_file_hash(self, path: str) -> Optional[str]:
        return self._impl.get_file_hash(path)

    def update_file_metadata(self, path: str, source: str, file_hash: str, mtime: int, size: int):
        return self._impl.update_file_metadata(path, source, file_hash, mtime, size)

    def get_stats(self) -> Dict[str, int]:
        return self._impl.get_stats()

    def close(self):
        return self._impl.close()

    @staticmethod
    def compute_hash(content: str) -> str:
        return _SQLiteMemoryStorage.compute_hash(content)
