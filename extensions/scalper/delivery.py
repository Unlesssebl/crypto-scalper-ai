"""Persistent per-episode cards with bot-wide 429 pause and safe ambiguity."""

import asyncio
import hashlib
import time


class CardOutbox:
    def __init__(self, journal, alerts):
        self.db, self.journal, self.alerts = journal.db, journal, alerts
        self.db.executescript("""CREATE TABLE IF NOT EXISTS episode_cards(
            episode_id TEXT PRIMARY KEY,message_id INTEGER,rich INTEGER DEFAULT 1,
            text TEXT NOT NULL,html TEXT NOT NULL,hash TEXT NOT NULL,status TEXT NOT NULL,
            next_at REAL NOT NULL,expires_at REAL NOT NULL,attempts INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS telegram_pause(id INTEGER PRIMARY KEY CHECK(id=1),until_at REAL NOT NULL);
        """)
        with self.db:
            self.db.execute(
                "UPDATE episode_cards SET status=CASE WHEN message_id IS NULL THEN 'unknown' "
                "ELSE 'pending' END WHERE status='sending'"
            )

    def enqueue(self, episode, text, html, expires_at):
        digest = hashlib.sha256((text + html).encode()).hexdigest()
        with self.db:
            self.db.execute(
                """INSERT INTO episode_cards(episode_id,text,html,hash,status,next_at,expires_at)
                VALUES(?,?,?,?,'pending',0,?) ON CONFLICT(episode_id) DO UPDATE SET
                text=excluded.text,html=excluded.html,hash=excluded.hash,expires_at=excluded.expires_at,
                status=CASE WHEN episode_cards.status='unknown' AND episode_cards.message_id IS NULL
                       THEN 'unknown' ELSE 'pending' END,
                attempts=0 WHERE episode_cards.hash<>excluded.hash""",
                (episode, text, html, digest, expires_at),
            )

    async def once(self, now=None):
        now = time.time() if now is None else now
        pause = self.db.execute("SELECT until_at FROM telegram_pause WHERE id=1").fetchone()
        if pause and pause[0] > now:
            return
        with self.db:
            self.db.execute("UPDATE episode_cards SET status='expired' WHERE status='pending' AND expires_at<?", (now,))
        row = self.db.execute(
            "SELECT * FROM episode_cards WHERE status='pending' AND next_at<=? "
            "AND expires_at>=? ORDER BY next_at,episode_id LIMIT 1",
            (now, now),
        ).fetchone()
        if not row:
            return
        card = dict(row)
        with self.db:
            self.db.execute(
                "UPDATE episode_cards SET status='sending',attempts=attempts+1 WHERE episode_id=?",
                (card["episode_id"],),
            )
        try:
            if card["message_id"] is None:
                result = await self.alerts.send_telegram(card["text"], rich_html=card["html"], return_message=True)
                if result:
                    with self.db:
                        self.db.execute(
                            "UPDATE episode_cards SET message_id=?,rich=? WHERE episode_id=?",
                            (*result, card["episode_id"]),
                        )
            else:
                result = await self.alerts.edit_telegram(
                    card["message_id"], card["text"], rich_html=card["html"] if card["rich"] else None
                )
            detail = dict(getattr(self.alerts, "last_telegram_result", {}))
        except Exception as exc:
            result = None
            detail = dict(state="outcome_unknown", reason=type(exc).__name__)
        self.journal.record_telegram_attempt(
            card["episode_id"], "send" if card["message_id"] is None else "edit", detail
        )
        status, next_at = "sent" if result else "failed", 0
        if not result and detail.get("http_status") == 429:
            next_at = now + max(1, int(detail.get("retry_after") or 60))
            status = "pending"
            with self.db:
                self.db.execute("INSERT OR REPLACE INTO telegram_pause VALUES(1,?)", (next_at,))
        elif not result and card["message_id"] is None and detail.get("state") == "outcome_unknown":
            status = "unknown"
        elif (
            not result
            and card["message_id"] is not None
            and card["attempts"] < 3
            and (detail.get("state") == "outcome_unknown" or (detail.get("http_status") or 0) >= 500)
        ):
            status, next_at = "pending", now + min(60, 5 * 2 ** card["attempts"])
        with self.db:
            self.db.execute(
                "UPDATE episode_cards SET status=?,next_at=? WHERE episode_id=? AND hash=?",
                (status, next_at, card["episode_id"], card["hash"]),
            )

    def summary(self):
        return [dict(r) for r in self.db.execute("SELECT status,COUNT(*) AS count FROM episode_cards GROUP BY status")]

    async def run(self):
        while True:
            await self.once()
            await asyncio.sleep(1)
