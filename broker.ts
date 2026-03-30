#!/usr/bin/env bun
/**
 * claude-peers broker daemon
 *
 * A singleton HTTP server on localhost:7899 backed by SQLite.
 * Tracks all registered Claude Code peers and routes messages between them.
 *
 * Auto-launched by the MCP server if not already running.
 * Run directly: bun broker.ts
 */

import { Database } from "bun:sqlite";
import type {
  RegisterRequest,
  RegisterResponse,
  HeartbeatRequest,
  SetNameRequest,
  SetSummaryRequest,
  ListPeersRequest,
  SendMessageRequest,
  PollMessagesRequest,
  PollMessagesResponse,
  AckMessagesRequest,
  Peer,
  Message,
} from "./shared/types.ts";
import {
  ValidationError,
  MAX_PATH_CHARS,
  MAX_TTY_CHARS,
  MAX_SUMMARY_CHARS,
  MAX_MESSAGE_CHARS,
  MAX_NAME_CHARS,
  isRecord,
  requirePeerId,
  requirePositiveInt,
  requireScope,
  requireString,
  requireOptionalString,
  parseRequiredToken,
  parsePositiveIntEnv,
} from "./shared/validation.ts";

const PORT = parsePositiveIntEnv(process.env.CLAUDE_PEERS_PORT, 7899, "CLAUDE_PEERS_PORT");
const DB_PATH = process.env.CLAUDE_PEERS_DB ?? `${process.env.HOME}/.claude-peers.db`;
const AUTH_HEADER = "x-claude-peers-token";
const AUTH_TOKEN = parseRequiredToken(process.env.CLAUDE_PEERS_TOKEN);
const MAX_UNDELIVERED_PER_PEER = parsePositiveIntEnv(
  process.env.CLAUDE_PEERS_MAX_UNDELIVERED_PER_PEER,
  200,
  "CLAUDE_PEERS_MAX_UNDELIVERED_PER_PEER"
);
const DELIVERED_RETENTION_HOURS = parsePositiveIntEnv(
  process.env.CLAUDE_PEERS_DELIVERED_RETENTION_HOURS,
  72,
  "CLAUDE_PEERS_DELIVERED_RETENTION_HOURS"
);
const STALE_UNDELIVERED_RETENTION_HOURS = parsePositiveIntEnv(
  process.env.CLAUDE_PEERS_STALE_UNDELIVERED_RETENTION_HOURS,
  168,
  "CLAUDE_PEERS_STALE_UNDELIVERED_RETENTION_HOURS"
);

// --- Database setup ---

const db = new Database(DB_PATH);
db.run("PRAGMA journal_mode = WAL");
db.run("PRAGMA busy_timeout = 3000");

db.run(`
  CREATE TABLE IF NOT EXISTS peers (
    id TEXT PRIMARY KEY,
    pid INTEGER NOT NULL,
    display_name TEXT NOT NULL DEFAULT '',
    cwd TEXT NOT NULL,
    git_root TEXT,
    tty TEXT,
    summary TEXT NOT NULL DEFAULT '',
    registered_at TEXT NOT NULL,
    last_seen TEXT NOT NULL
  )
`);

// Backward compatibility for older databases created before display_name existed.
try {
  db.run("ALTER TABLE peers ADD COLUMN display_name TEXT NOT NULL DEFAULT ''");
} catch {
  // already exists
}

db.run(`
  CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    from_id TEXT NOT NULL,
    to_id TEXT NOT NULL,
    text TEXT NOT NULL,
    sent_at TEXT NOT NULL,
    delivered INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (from_id) REFERENCES peers(id),
    FOREIGN KEY (to_id) REFERENCES peers(id)
  )
`);

db.run(`
  CREATE INDEX IF NOT EXISTS idx_messages_to_delivered_sent
  ON messages (to_id, delivered, sent_at)
`);

// Clean up stale peers (PIDs that no longer exist) on startup
function cleanStalePeers() {
  const peers = db.query("SELECT id, pid FROM peers").all() as { id: string; pid: number }[];
  for (const peer of peers) {
    try {
      // Check if process is still alive (signal 0 doesn't kill, just checks)
      process.kill(peer.pid, 0);
    } catch {
      // Process doesn't exist, remove it
      db.run("DELETE FROM peers WHERE id = ?", [peer.id]);
      db.run("DELETE FROM messages WHERE to_id = ? AND delivered = 0", [peer.id]);
    }
  }
}

function pruneMessages() {
  const deliveredCutoff = new Date(
    Date.now() - DELIVERED_RETENTION_HOURS * 60 * 60 * 1000
  ).toISOString();
  const staleUndeliveredCutoff = new Date(
    Date.now() - STALE_UNDELIVERED_RETENTION_HOURS * 60 * 60 * 1000
  ).toISOString();

  db.run("DELETE FROM messages WHERE delivered = 1 AND sent_at < ?", [deliveredCutoff]);
  db.run("DELETE FROM messages WHERE delivered = 0 AND sent_at < ?", [staleUndeliveredCutoff]);
}

cleanStalePeers();
pruneMessages();

// Periodically clean stale peers (every 30s)
setInterval(cleanStalePeers, 30_000);
setInterval(pruneMessages, 5 * 60_000);

// --- Prepared statements ---

const insertPeer = db.prepare(`
  INSERT INTO peers (id, pid, display_name, cwd, git_root, tty, summary, registered_at, last_seen)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
`);

const updateLastSeen = db.prepare(`
  UPDATE peers SET last_seen = ? WHERE id = ?
`);

const updateSummary = db.prepare(`
  UPDATE peers SET summary = ? WHERE id = ?
`);

const updateDisplayName = db.prepare(`
  UPDATE peers SET display_name = ? WHERE id = ?
`);

const deletePeer = db.prepare(`
  DELETE FROM peers WHERE id = ?
`);

const selectAllPeers = db.prepare(`
  SELECT * FROM peers
`);

const selectPeersByDirectory = db.prepare(`
  SELECT * FROM peers WHERE cwd = ?
`);

const selectPeersByGitRoot = db.prepare(`
  SELECT * FROM peers WHERE git_root = ?
`);

const selectPeerById = db.prepare(`
  SELECT id FROM peers WHERE id = ?
`);

const insertMessage = db.prepare(`
  INSERT INTO messages (from_id, to_id, text, sent_at, delivered)
  VALUES (?, ?, ?, ?, 0)
`);

const countUndeliveredForPeer = db.prepare(`
  SELECT COUNT(*) AS count FROM messages WHERE to_id = ? AND delivered = 0
`);

const deleteOldestUndeliveredForPeer = db.prepare(`
  DELETE FROM messages WHERE id IN (
    SELECT id
    FROM messages
    WHERE to_id = ? AND delivered = 0
    ORDER BY sent_at ASC
    LIMIT ?
  )
`);

const selectUndelivered = db.prepare(`
  SELECT * FROM messages WHERE to_id = ? AND delivered = 0 ORDER BY sent_at ASC
`);

// --- Generate peer ID ---

function generateId(): string {
  const chars = "abcdefghijklmnopqrstuvwxyz0123456789";
  let id = "";
  for (let i = 0; i < 8; i++) {
    id += chars[Math.floor(Math.random() * chars.length)];
  }
  return id;
}

// --- Request handlers ---

function parseRegisterRequest(body: unknown): RegisterRequest {
  if (!isRecord(body)) {
    throw new ValidationError("Request body must be a JSON object");
  }
  return {
    pid: requirePositiveInt(body.pid, "pid"),
    display_name: requireString(body.display_name ?? "", "display_name", {
      max: MAX_NAME_CHARS,
      allowEmpty: true,
    }),
    cwd: requireString(body.cwd, "cwd", { max: MAX_PATH_CHARS }),
    git_root: requireOptionalString(body.git_root, "git_root", { max: MAX_PATH_CHARS }),
    tty: requireOptionalString(body.tty, "tty", { max: MAX_TTY_CHARS }),
    summary: requireString(body.summary, "summary", {
      max: MAX_SUMMARY_CHARS,
      allowEmpty: true,
    }),
  };
}

function parseSetNameRequest(body: unknown): SetNameRequest {
  if (!isRecord(body)) {
    throw new ValidationError("Request body must be a JSON object");
  }
  return {
    id: requirePeerId(body.id, "id"),
    name: requireString(body.name, "name", {
      max: MAX_NAME_CHARS,
      allowEmpty: true,
    }),
  };
}

function parseHeartbeatRequest(body: unknown): HeartbeatRequest {
  if (!isRecord(body)) {
    throw new ValidationError("Request body must be a JSON object");
  }
  return { id: requirePeerId(body.id, "id") };
}

function parseSetSummaryRequest(body: unknown): SetSummaryRequest {
  if (!isRecord(body)) {
    throw new ValidationError("Request body must be a JSON object");
  }
  return {
    id: requirePeerId(body.id, "id"),
    summary: requireString(body.summary, "summary", {
      max: MAX_SUMMARY_CHARS,
      allowEmpty: true,
    }),
  };
}

function parseListPeersRequest(body: unknown): ListPeersRequest {
  if (!isRecord(body)) {
    throw new ValidationError("Request body must be a JSON object");
  }
  const exclude = body.exclude_id;
  if (exclude !== undefined && exclude !== null) {
    requirePeerId(exclude, "exclude_id");
  }
  return {
    scope: requireScope(body.scope),
    cwd: requireString(body.cwd, "cwd", { max: MAX_PATH_CHARS }),
    git_root: requireOptionalString(body.git_root, "git_root", { max: MAX_PATH_CHARS }),
    exclude_id: (exclude ?? undefined) as string | undefined,
  };
}

function parseSendMessageRequest(body: unknown): SendMessageRequest {
  if (!isRecord(body)) {
    throw new ValidationError("Request body must be a JSON object");
  }
  return {
    from_id: requirePeerId(body.from_id, "from_id"),
    to_id: requirePeerId(body.to_id, "to_id"),
    text: requireString(body.text, "text", { max: MAX_MESSAGE_CHARS }),
  };
}

function parsePollMessagesRequest(body: unknown): PollMessagesRequest {
  if (!isRecord(body)) {
    throw new ValidationError("Request body must be a JSON object");
  }
  return { id: requirePeerId(body.id, "id") };
}

function parseUnregisterRequest(body: unknown): { id: string } {
  if (!isRecord(body)) {
    throw new ValidationError("Request body must be a JSON object");
  }
  return { id: requirePeerId(body.id, "id") };
}

function parseAckMessagesRequest(body: unknown): AckMessagesRequest {
  if (!isRecord(body)) {
    throw new ValidationError("Request body must be a JSON object");
  }
  const id = requirePeerId(body.id, "id");
  const messageIds = body.message_ids;
  if (!Array.isArray(messageIds)) {
    throw new ValidationError("message_ids must be an array");
  }
  const parsed = messageIds.map((value, idx) => {
    if (typeof value !== "number" || !Number.isInteger(value) || value <= 0) {
      throw new ValidationError(`message_ids[${idx}] must be a positive integer`);
    }
    return value;
  });
  return { id, message_ids: parsed };
}

function handleRegister(body: RegisterRequest): RegisterResponse {
  const id = generateId();
  const now = new Date().toISOString();

  // Remove any existing registration for this PID (re-registration)
  const existing = db.query("SELECT id FROM peers WHERE pid = ?").get(body.pid) as { id: string } | null;
  if (existing) {
    deletePeer.run(existing.id);
  }

  insertPeer.run(
    id,
    body.pid,
    body.display_name,
    body.cwd,
    body.git_root,
    body.tty,
    body.summary,
    now,
    now
  );
  return { id };
}

function handleHeartbeat(body: HeartbeatRequest): void {
  updateLastSeen.run(new Date().toISOString(), body.id);
}

function handleSetSummary(body: SetSummaryRequest): void {
  updateSummary.run(body.summary, body.id);
}

function handleSetName(body: SetNameRequest): void {
  updateDisplayName.run(body.name, body.id);
}

function handleListPeers(body: ListPeersRequest): Peer[] {
  let peers: Peer[];

  switch (body.scope) {
    case "machine":
      peers = selectAllPeers.all() as Peer[];
      break;
    case "directory":
      peers = selectPeersByDirectory.all(body.cwd) as Peer[];
      break;
    case "repo":
      if (body.git_root) {
        peers = selectPeersByGitRoot.all(body.git_root) as Peer[];
      } else {
        // No git root, fall back to directory
        peers = selectPeersByDirectory.all(body.cwd) as Peer[];
      }
      break;
    default:
      peers = selectAllPeers.all() as Peer[];
  }

  // Exclude the requesting peer
  if (body.exclude_id) {
    peers = peers.filter((p) => p.id !== body.exclude_id);
  }

  // Verify each peer's process is still alive
  return peers.filter((p) => {
    try {
      process.kill(p.pid, 0);
      return true;
    } catch {
      // Clean up dead peer
      deletePeer.run(p.id);
      return false;
    }
  });
}

function handleSendMessage(body: SendMessageRequest): { ok: boolean; error?: string } {
  // Verify sender exists to prevent spoofed messages from arbitrary local processes
  const sender = selectPeerById.get(body.from_id) as { id: string } | null;
  if (!sender) {
    return { ok: false, error: `Sender ${body.from_id} is not a registered peer` };
  }

  // Verify target exists
  const target = selectPeerById.get(body.to_id) as { id: string } | null;
  if (!target) {
    return { ok: false, error: `Peer ${body.to_id} not found` };
  }

  // Keep queue bounded so a noisy sender cannot grow the database indefinitely.
  const countResult = countUndeliveredForPeer.get(body.to_id) as { count: number } | null;
  const undeliveredCount = countResult?.count ?? 0;
  if (undeliveredCount >= MAX_UNDELIVERED_PER_PEER) {
    const overBy = undeliveredCount - MAX_UNDELIVERED_PER_PEER + 1;
    deleteOldestUndeliveredForPeer.run(body.to_id, overBy);
  }

  insertMessage.run(body.from_id, body.to_id, body.text, new Date().toISOString());
  return { ok: true };
}

function handlePollMessages(body: PollMessagesRequest): PollMessagesResponse {
  const messages = selectUndelivered.all(body.id) as Message[];
  return { messages };
}

function handleAckMessages(body: AckMessagesRequest): void {
  for (const messageId of body.message_ids) {
    // Only acknowledge messages for this peer ID.
    db.run("UPDATE messages SET delivered = 1 WHERE id = ? AND to_id = ?", [messageId, body.id]);
  }
}

function handleUnregister(body: { id: string }): void {
  deletePeer.run(body.id);
}

// --- HTTP Server ---

Bun.serve({
  port: PORT,
  hostname: "127.0.0.1",
  async fetch(req) {
    const url = new URL(req.url);
    const path = url.pathname;

    if (req.method !== "POST") {
      if (path === "/health") {
        return Response.json({ status: "ok", peers: (selectAllPeers.all() as Peer[]).length });
      }
      return new Response("claude-peers broker", { status: 200 });
    }

    try {
      const isHealthPath = path === "/health";
      const presentedToken = req.headers.get(AUTH_HEADER);
      if (!isHealthPath && presentedToken !== AUTH_TOKEN) {
        return Response.json(
          { error: `Unauthorized. Expected header ${AUTH_HEADER}.` },
          { status: 401 }
        );
      }

      const contentType = req.headers.get("content-type")?.toLowerCase() ?? "";
      if (!contentType.includes("application/json")) {
        return Response.json({ error: "Content-Type must be application/json" }, { status: 415 });
      }

      const body = await req.json();

      switch (path) {
        case "/register":
          return Response.json(handleRegister(parseRegisterRequest(body)));
        case "/heartbeat":
          handleHeartbeat(parseHeartbeatRequest(body));
          return Response.json({ ok: true });
        case "/set-summary":
          handleSetSummary(parseSetSummaryRequest(body));
          return Response.json({ ok: true });
        case "/set-name":
          handleSetName(parseSetNameRequest(body));
          return Response.json({ ok: true });
        case "/list-peers":
          return Response.json(handleListPeers(parseListPeersRequest(body)));
        case "/send-message":
          {
            const result = handleSendMessage(parseSendMessageRequest(body));
            if (!result.ok) {
              return Response.json(result, { status: 400 });
            }
            return Response.json(result);
          }
        case "/poll-messages":
          return Response.json(handlePollMessages(parsePollMessagesRequest(body)));
        case "/ack-messages":
          handleAckMessages(parseAckMessagesRequest(body));
          return Response.json({ ok: true });
        case "/unregister":
          handleUnregister(parseUnregisterRequest(body));
          return Response.json({ ok: true });
        default:
          return Response.json({ error: "not found" }, { status: 404 });
      }
    } catch (e) {
      if (e instanceof ValidationError) {
        return Response.json({ error: e.message }, { status: e.status });
      }
      const msg = e instanceof Error ? e.message : String(e);
      return Response.json({ error: msg }, { status: 500 });
    }
  },
});

console.error(
  `[claude-peers broker] listening on 127.0.0.1:${PORT} (db: ${DB_PATH}, auth header: ${AUTH_HEADER})`
);
