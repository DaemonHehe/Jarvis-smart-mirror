"""
Knowledge graph storage for Jarvis Smart Mirror.

Persistent, bidirectionally-linked knowledge base backed by SQLite.
Stores conversation history and user-editable notes as nodes with edges
representing relationships. Supports full-text search and RAG context retrieval.
"""

import json
import re
import sqlite3
import threading
import time
from typing import Optional


def synchronized(method):
    """Serialize access to the shared SQLite connection across worker threads."""
    def wrapper(self, *args, **kwargs):
        with self._lock:
            return method(self, *args, **kwargs)
    return wrapper


class KnowledgeGraph:
    """SQLite-backed knowledge graph with conversation ingestion, notes, linking, and RAG retrieval."""

    SCHEMA_SQL = """
    CREATE TABLE IF NOT EXISTS nodes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        type TEXT NOT NULL CHECK(type IN ('conversation', 'note')),
        title TEXT NOT NULL,
        content TEXT NOT NULL DEFAULT '',
        user_text TEXT,
        assistant_text TEXT,
        tags TEXT NOT NULL DEFAULT '[]',
        timestamp REAL NOT NULL
    );

    CREATE TABLE IF NOT EXISTS edges (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_id INTEGER NOT NULL,
        target_id INTEGER NOT NULL,
        relation TEXT NOT NULL DEFAULT 'related',
        timestamp REAL NOT NULL,
        FOREIGN KEY (source_id) REFERENCES nodes(id) ON DELETE CASCADE,
        FOREIGN KEY (target_id) REFERENCES nodes(id) ON DELETE CASCADE,
        UNIQUE(source_id, target_id, relation)
    );

    CREATE VIRTUAL TABLE IF NOT EXISTS nodes_fts USING fts5(
        title, content, user_text, assistant_text,
        content_rowid='id',
        tokenize='porter unicode61'
    );

    CREATE TRIGGER IF NOT EXISTS nodes_ai AFTER INSERT ON nodes BEGIN
        INSERT INTO nodes_fts(rowid, title, content, user_text, assistant_text)
        VALUES (new.id, new.title, new.content, new.user_text, new.assistant_text);
    END;

    CREATE TRIGGER IF NOT EXISTS nodes_ad AFTER DELETE ON nodes BEGIN
        DELETE FROM nodes_fts WHERE rowid = old.id;
    END;

    CREATE TRIGGER IF NOT EXISTS nodes_au AFTER UPDATE ON nodes BEGIN
        DELETE FROM nodes_fts WHERE rowid = old.id;
        INSERT INTO nodes_fts(rowid, title, content, user_text, assistant_text)
        VALUES (new.id, new.title, new.content, new.user_text, new.assistant_text);
    END;
    """

    def __init__(self, db_path: str = "knowledge_graph.db"):
        """Initialize the knowledge graph, creating the database and schema if needed.

        Args:
            db_path: Path to the SQLite database file.
        """
        self.db_path = db_path
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.executescript(self.SCHEMA_SQL)
        self._conn.commit()

    @synchronized
    def close(self):
        """Close the database connection."""
        if self._conn:
            self._conn.close()

    @synchronized
    def add_conversation(
        self,
        user_text: str,
        assistant_text: str,
        tags: Optional[list[str]] = None,
    ) -> int:
        """Store a conversation turn as a node and auto-link to related nodes.

        Args:
            user_text: The user's spoken query.
            assistant_text: The assistant's response.
            tags: Optional list of tag strings.

        Returns:
            The ID of the newly created conversation node.
        """
        ts = time.time()
        title = user_text[:80] + ("..." if len(user_text) > 80 else "")
        content = f"User: {user_text}\nAssistant: {assistant_text}"
        tags_json = json.dumps(tags or [])

        cursor = self._conn.execute(
            """INSERT INTO nodes (type, title, content, user_text, assistant_text, tags, timestamp)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            ("conversation", title, content, user_text, assistant_text, tags_json, ts),
        )
        node_id = cursor.lastrowid
        self._conn.commit()

        # Auto-link to related existing nodes via keyword matching
        self._auto_link(node_id, user_text + " " + assistant_text)

        return node_id

    @synchronized
    def add_note(
        self,
        title: str,
        content: str,
        tags: Optional[list[str]] = None,
    ) -> int:
        """Store a user-created knowledge note.

        Args:
            title: Note title.
            content: Note body content.
            tags: Optional list of tag strings.

        Returns:
            The ID of the newly created note node.
        """
        ts = time.time()
        tags_json = json.dumps(tags or [])

        cursor = self._conn.execute(
            """INSERT INTO nodes (type, title, content, tags, timestamp)
               VALUES (?, ?, ?, ?, ?)""",
            ("note", title, content, tags_json, ts),
        )
        node_id = cursor.lastrowid
        self._conn.commit()
        return node_id

    @synchronized
    def link_nodes(self, node_id_a: int, node_id_b: int, relation: str = "related") -> None:
        """Create a bidirectional edge between two nodes.

        Args:
            node_id_a: First node ID.
            node_id_b: Second node ID.
            relation: Relationship label (default: 'related').
        """
        ts = time.time()
        try:
            self._conn.execute(
                "INSERT OR IGNORE INTO edges (source_id, target_id, relation, timestamp) VALUES (?, ?, ?, ?)",
                (node_id_a, node_id_b, relation, ts),
            )
            self._conn.execute(
                "INSERT OR IGNORE INTO edges (source_id, target_id, relation, timestamp) VALUES (?, ?, ?, ?)",
                (node_id_b, node_id_a, relation, ts),
            )
            self._conn.commit()
        except sqlite3.IntegrityError:
            pass  # Link already exists or node doesn't exist

    @synchronized
    def get_node(self, node_id: int) -> Optional[dict]:
        """Retrieve a node and its linked neighbors.

        Args:
            node_id: The node ID to look up.

        Returns:
            Node dict with 'neighbors' list, or None if not found.
        """
        row = self._conn.execute("SELECT * FROM nodes WHERE id = ?", (node_id,)).fetchone()
        if not row:
            return None

        node = self._row_to_dict(row)

        # Fetch neighbors
        neighbors = self._conn.execute(
            """SELECT n.id, n.type, n.title, e.relation
               FROM edges e JOIN nodes n ON e.target_id = n.id
               WHERE e.source_id = ?""",
            (node_id,),
        ).fetchall()
        node["neighbors"] = [
            {"id": n["id"], "type": n["type"], "title": n["title"], "relation": n["relation"]}
            for n in neighbors
        ]
        return node

    @synchronized
    def search_nodes(self, query: str, limit: int = 10) -> list[dict]:
        """Full-text search across node content.

        Args:
            query: Search query string.
            limit: Maximum number of results.

        Returns:
            List of matching node dicts ordered by relevance.
        """
        # Sanitize query for FTS5
        sanitized = re.sub(r"[^\w\s]", "", query).strip()
        if not sanitized:
            return []

        # Use FTS5 match with prefix matching for partial words
        fts_query = " OR ".join(f'"{word}"*' for word in sanitized.split())

        try:
            rows = self._conn.execute(
                """SELECT n.*, rank
                   FROM nodes_fts fts
                   JOIN nodes n ON fts.rowid = n.id
                   WHERE nodes_fts MATCH ?
                   ORDER BY rank
                   LIMIT ?""",
                (fts_query, limit),
            ).fetchall()
            return [self._row_to_dict(r) for r in rows]
        except sqlite3.OperationalError:
            # Fallback to LIKE search if FTS query is malformed
            rows = self._conn.execute(
                """SELECT * FROM nodes
                   WHERE content LIKE ? OR title LIKE ?
                   ORDER BY timestamp DESC LIMIT ?""",
                (f"%{sanitized}%", f"%{sanitized}%", limit),
            ).fetchall()
            return [self._row_to_dict(r) for r in rows]

    @synchronized
    def get_relevant_context(self, query: str, max_tokens: int = 500) -> str:
        """Retrieve relevant knowledge context for LLM prompt augmentation (RAG).

        Searches the graph for nodes matching the query and formats them into
        a context string suitable for injection into a Llama 3.2 prompt.

        Args:
            query: The user's current query to find context for.
            max_tokens: Approximate max token budget (chars / 4 heuristic).

        Returns:
            Formatted context string, or empty string if no relevant context found.
        """
        results = self.search_nodes(query, limit=5)
        if not results:
            return ""

        max_chars = max_tokens * 4  # Rough chars-to-tokens estimate
        context_parts = []
        char_count = 0

        for node in results:
            if node["type"] == "conversation":
                entry = f"[Previous conversation] User asked: \"{node.get('user_text', '')}\"\nAssistant replied: \"{node.get('assistant_text', '')}\""
            else:
                entry = f"[Knowledge note: {node['title']}] {node['content']}"

            if char_count + len(entry) > max_chars:
                break

            context_parts.append(entry)
            char_count += len(entry)

        if not context_parts:
            return ""

        return "--- Relevant Context ---\n" + "\n\n".join(context_parts) + "\n--- End Context ---"

    @synchronized
    def get_graph_data(self) -> dict:
        """Return full graph structure for visualization.

        Returns:
            Dict with 'nodes' and 'edges' lists suitable for vis-network or similar.
        """
        nodes = self._conn.execute("SELECT * FROM nodes ORDER BY timestamp DESC").fetchall()
        edges = self._conn.execute(
            "SELECT DISTINCT MIN(id) as id, source_id, target_id, relation, timestamp FROM edges GROUP BY MIN(source_id, target_id), MAX(source_id, target_id), relation"
        ).fetchall()

        return {
            "nodes": [self._row_to_dict(n) for n in nodes],
            "edges": [
                {
                    "id": e["id"],
                    "source": e["source_id"],
                    "target": e["target_id"],
                    "relation": e["relation"],
                    "timestamp": e["timestamp"],
                }
                for e in edges
            ],
        }

    @synchronized
    def get_stats(self) -> dict:
        """Return graph statistics.

        Returns:
            Dict with counts of total nodes, conversations, notes, and edges.
        """
        total = self._conn.execute("SELECT COUNT(*) FROM nodes").fetchone()[0]
        conversations = self._conn.execute(
            "SELECT COUNT(*) FROM nodes WHERE type = 'conversation'"
        ).fetchone()[0]
        notes = self._conn.execute(
            "SELECT COUNT(*) FROM nodes WHERE type = 'note'"
        ).fetchone()[0]
        edge_count = self._conn.execute(
            "SELECT COUNT(*) FROM edges"
        ).fetchone()[0]

        return {
            "total_nodes": total,
            "conversations": conversations,
            "notes": notes,
            "edges": edge_count // 2,  # Bidirectional edges counted once
        }

    @synchronized
    def delete_node(self, node_id: int) -> bool:
        """Delete a node and all its edges.

        Args:
            node_id: The node ID to delete.

        Returns:
            True if node was deleted, False if not found.
        """
        existing = self._conn.execute("SELECT id FROM nodes WHERE id = ?", (node_id,)).fetchone()
        if not existing:
            return False

        self._conn.execute("DELETE FROM edges WHERE source_id = ? OR target_id = ?", (node_id, node_id))
        self._conn.execute("DELETE FROM nodes WHERE id = ?", (node_id,))
        self._conn.commit()
        return True

    def _auto_link(self, node_id: int, text: str, max_links: int = 3) -> None:
        """Automatically link a new node to related existing nodes via keyword overlap.

        Args:
            node_id: The newly created node to link from.
            text: Text content to extract keywords from.
            max_links: Maximum number of auto-links to create.
        """
        # Extract Unicode letter sequences so non-Latin conversations can be linked.
        words = set(
            w.lower()
            for w in re.findall(r"\b[^\W\d_]{4,}\b", text, flags=re.UNICODE)
            if w.lower() not in _STOP_WORDS
        )

        if not words:
            return

        # Search for related nodes using the keywords
        for word in list(words)[:5]:  # Limit keyword probes
            try:
                results = self._conn.execute(
                    """SELECT rowid FROM nodes_fts
                       WHERE nodes_fts MATCH ? LIMIT ?""",
                    (f'"{word}"', max_links + 1),
                ).fetchall()

                for row in results:
                    target_id = row[0]
                    if target_id != node_id:
                        self.link_nodes(node_id, target_id, "auto_related")
                        max_links -= 1
                        if max_links <= 0:
                            return
            except sqlite3.OperationalError:
                continue

    def _row_to_dict(self, row: sqlite3.Row) -> dict:
        """Convert a sqlite3.Row to a plain dict with parsed tags."""
        d = dict(row)
        # Remove FTS rank column if present
        d.pop("rank", None)
        # Parse tags JSON
        if "tags" in d and isinstance(d["tags"], str):
            try:
                d["tags"] = json.loads(d["tags"])
            except (json.JSONDecodeError, TypeError):
                d["tags"] = []
        return d


# Common English stop words to skip during auto-linking
_STOP_WORDS = frozenset({
    "that", "this", "with", "from", "your", "have", "been", "will",
    "would", "could", "should", "about", "their", "there", "where",
    "when", "what", "which", "these", "those", "then", "than",
    "them", "they", "some", "more", "other", "into", "over",
    "also", "just", "only", "very", "much", "like", "does",
    "doing", "done", "make", "made", "know", "known", "take",
    "taken", "come", "came", "going", "gone", "each", "every",
    "both", "most", "many", "such", "same", "back", "well",
})
