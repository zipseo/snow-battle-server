'use strict';

const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = Number(process.env.PORT || 3000);
const PATH = process.env.WS_PATH || '/snow-battle-ws';
const PLAYER_TTL_MS = Number(process.env.PLAYER_TTL_MS || 15000);
const CLEANUP_MS = Number(process.env.CLEANUP_MS || 3000);

const rooms = new Map();

function safeRoom(value) {
  return String(value || 'default').slice(0, 160).replace(/[^a-zA-Z0-9:_./-]/g, '_');
}

function safePlayerId(value) {
  return String(value || ('p_' + Math.random().toString(36).slice(2))).slice(0, 80).replace(/[^a-zA-Z0-9_-]/g, '_');
}

function now() {
  return Date.now();
}

function send(ws, data) {
  if (!ws || ws.readyState !== ws.OPEN) return false;
  try {
    ws.send(JSON.stringify(data));
    return true;
  } catch (_) {
    return false;
  }
}

function roomFor(name) {
  if (!rooms.has(name)) rooms.set(name, new Map());
  return rooms.get(name);
}

function publicPlayer(record) {
  return record && record.payload ? record.payload : null;
}

function broadcast(roomName, message, exceptWs = null) {
  const room = rooms.get(roomName);
  if (!room) return;
  for (const record of room.values()) {
    if (record.ws === exceptWs) continue;
    send(record.ws, message);
  }
}

function removeClient(roomName, playerId, ws, announce = true) {
  const room = rooms.get(roomName);
  if (!room) return;
  const record = room.get(playerId);
  if (!record || record.ws !== ws) return;
  room.delete(playerId);
  if (announce) broadcast(roomName, { type: 'player-left', playerId, now: now() });
  if (room.size === 0) rooms.delete(roomName);
}

const server = http.createServer((req, res) => {
  if (req.url === '/health' || req.url === '/') {
    res.writeHead(200, { 'content-type': 'application/json; charset=utf-8' });
    res.end(JSON.stringify({ ok: true, service: 'snow-battle-ws', path: PATH, rooms: rooms.size }));
    return;
  }
  res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' });
  res.end('Not found');
});

const wss = new WebSocketServer({ server, path: PATH });

wss.on('connection', (ws, req) => {
  const url = new URL(req.url, 'http://localhost');
  const roomName = safeRoom(url.searchParams.get('room'));
  const playerId = safePlayerId(url.searchParams.get('playerId'));
  const room = roomFor(roomName);

  const existing = room.get(playerId);
  if (existing && existing.ws && existing.ws.readyState === existing.ws.OPEN) {
    try { existing.ws.close(4000, 'duplicate-player'); } catch (_) {}
  }

  const record = {
    ws,
    playerId,
    roomName,
    payload: {
      playerId,
      visible: false,
      name: 'Jugador',
      page: roomName,
      updatedAt: now()
    },
    lastSeen: now()
  };

  room.set(playerId, record);

  send(ws, {
    type: 'welcome',
    playerId,
    room: roomName,
    now: now()
  });

  const snapshot = [];
  for (const [id, other] of room.entries()) {
    if (id === playerId) continue;
    const player = publicPlayer(other);
    if (player && now() - other.lastSeen < PLAYER_TTL_MS) snapshot.push(player);
  }
  send(ws, { type: 'snapshot', players: snapshot, now: now() });

  ws.on('message', (buffer) => {
    let msg;
    try { msg = JSON.parse(buffer.toString()); } catch (_) { return; }

    record.lastSeen = now();

    if (msg.type === 'ping') {
      send(ws, { type: 'pong', now: now() });
      return;
    }

    if (msg.type === 'player-update' && msg.player && msg.player.playerId === playerId) {
      const payload = {
        ...msg.player,
        playerId,
        page: roomName,
        updatedAt: now()
      };
      record.payload = payload;
      broadcast(roomName, { type: 'player-update', player: payload, now: now() }, ws);
    }
  });

  ws.on('close', () => removeClient(roomName, playerId, ws, true));
  ws.on('error', () => removeClient(roomName, playerId, ws, true));
});

setInterval(() => {
  const t = now();
  for (const [roomName, room] of rooms.entries()) {
    for (const [playerId, record] of room.entries()) {
      if (!record.ws || record.ws.readyState !== record.ws.OPEN || t - record.lastSeen > PLAYER_TTL_MS) {
        try { record.ws && record.ws.close(); } catch (_) {}
        room.delete(playerId);
        broadcast(roomName, { type: 'player-left', playerId, now: t });
      }
    }
    if (room.size === 0) rooms.delete(roomName);
  }
}, CLEANUP_MS);

server.listen(PORT, () => {
  console.log(`Snow Battle WebSocket relay listening on port ${PORT}${PATH}`);
});
