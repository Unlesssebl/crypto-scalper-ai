"""Persistent per-episode cards with bot-wide 429 pause and safe ambiguity."""

import asyncio
import hashlib
import json
import os
import time


class CardOutbox:
    def __init__(self, journal, alerts, senders=None):
        self.db, self.journal, self.alerts = journal.db, journal, alerts
        # destination -> bot; cards of other destinations go through the main (anomalies) bot.
        self.senders = dict(senders or {})
        self.db.executescript("""CREATE TABLE IF NOT EXISTS episode_cards(
            episode_id TEXT PRIMARY KEY,message_id INTEGER,rich INTEGER DEFAULT 1,
            text TEXT NOT NULL,html TEXT NOT NULL,hash TEXT NOT NULL,status TEXT NOT NULL,
            next_at REAL NOT NULL,expires_at REAL NOT NULL,attempts INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS telegram_pause(id INTEGER PRIMARY KEY CHECK(id=1),until_at REAL NOT NULL);
        """)
        columns = {r[1] for r in self.db.execute("PRAGMA table_info(episode_cards)")}
        for name, definition in (
            ("chat_id", "TEXT"),
            ("destination", "TEXT"),
            ("reply_markup", "TEXT"),
            ("silent", "INTEGER DEFAULT 0"),
        ):
            if name not in columns:
                self.db.execute(f"ALTER TABLE episode_cards ADD COLUMN {name} {definition}")
        with self.db:
            self.db.execute(
                "UPDATE episode_cards SET status=CASE WHEN message_id IS NULL THEN 'unknown' "
                "ELSE 'pending' END WHERE status='sending'"
            )

    def enqueue(
        self, episode, text, html, expires_at, *, chat_id=None, destination=None, reply_markup=None, silent=False
    ):
        markup = json.dumps(reply_markup, ensure_ascii=False, sort_keys=True) if reply_markup is not None else None
        digest = hashlib.sha256((text + html + str(chat_id) + str(markup) + str(silent)).encode()).hexdigest()
        status = "blocked_destination" if destination == "analytics" and not chat_id else "pending"
        with self.db:
            old = self.db.execute(
                "SELECT chat_id,message_id FROM episode_cards WHERE episode_id=?", (episode,)
            ).fetchone()
            if old and old["message_id"] is not None and old["chat_id"] != chat_id:
                raise ValueError("A delivered message cannot move between chats")
            self.db.execute(
                """INSERT INTO episode_cards(episode_id,text,html,hash,status,next_at,expires_at)
                VALUES(?,?,?,?,?,0,?) ON CONFLICT(episode_id) DO UPDATE SET
                text=excluded.text,html=excluded.html,hash=excluded.hash,expires_at=excluded.expires_at,
                status=CASE WHEN episode_cards.status='unknown' AND episode_cards.message_id IS NULL
                       THEN 'unknown' ELSE excluded.status END,
                attempts=0 WHERE episode_cards.hash<>excluded.hash""",
                (episode, text, html, digest, status, expires_at),
            )
            self.db.execute(
                "UPDATE episode_cards SET chat_id=?,destination=?,reply_markup=?,silent=? WHERE episode_id=?",
                (chat_id, destination, markup, int(silent), episode),
            )

    async def once(self, now=None):
        now = time.time() if now is None else now
        analytics = os.getenv("SCALPER_ANALYTICS_CHAT_ID", "").strip()
        with self.db:
            self.db.execute(
                "UPDATE episode_cards SET status='expired' WHERE status='blocked_destination' AND expires_at<?", (now,)
            )
            if analytics and analytics != str(self.alerts.telegram_chat_id):
                self.db.execute(
                    "UPDATE episode_cards SET chat_id=?,status='pending' "
                    "WHERE status='blocked_destination' AND expires_at>=?",
                    (analytics, now),
                )
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
        sender = self.senders.get(card.get("destination"), self.alerts)
        options = {}
        if card["chat_id"] is not None:
            options["chat_id"] = card["chat_id"]
        if card["reply_markup"] is not None:
            options["reply_markup"] = json.loads(card["reply_markup"])
        with self.db:
            self.db.execute(
                "UPDATE episode_cards SET status='sending',attempts=attempts+1 WHERE episode_id=?",
                (card["episode_id"],),
            )
        try:
            if card["message_id"] is None:
                result = await sender.send_telegram(
                    card["text"],
                    rich_html=card["html"],
                    return_message=True,
                    **options,
                    **({"silent": True} if card["silent"] else {}),
                )
                if result:
                    with self.db:
                        self.db.execute(
                            "UPDATE episode_cards SET message_id=?,rich=? WHERE episode_id=?",
                            (*result, card["episode_id"]),
                        )
            else:
                result = await sender.edit_telegram(
                    card["message_id"], card["text"], rich_html=card["html"] if card["rich"] else None, **options
                )
            detail = dict(getattr(sender, "last_telegram_result", {}))
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
