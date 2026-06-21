'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = Number(process.env.PORT || 3000);
const PATH = process.env.WS_PATH || '/snow-battle-ws';
const PLAYER_TTL_MS = Number(process.env.PLAYER_TTL_MS || 15000);
const CLEANUP_MS = Number(process.env.CLEANUP_MS || 3000);
const DB_FILE = process.env.DB_FILE || path.join(__dirname, 'snow-battle-db.json');

const rooms = new Map();
const sessions = new Map();

function now() { return Date.now(); }

function send(ws, data) {
  if (!ws || ws.readyState !== ws.OPEN) return false;
  try { ws.send(JSON.stringify(data)); return true; } catch (_) { return false; }
}

function sendJson(res, status, payload) {
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'access-control-allow-origin': '*',
    'access-control-allow-methods': 'GET,POST,OPTIONS',
    'access-control-allow-headers': 'content-type,authorization'
  });
  res.end(JSON.stringify(payload));
}

function sendText(res, status, text) {
  res.writeHead(status, {
    'content-type': 'text/plain; charset=utf-8',
    'access-control-allow-origin': '*',
    'access-control-allow-methods': 'GET,POST,OPTIONS',
    'access-control-allow-headers': 'content-type,authorization'
  });
  res.end(text);
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let body = '';
    req.on('data', chunk => {
      body += chunk.toString();
      if (body.length > 1024 * 1024) {
        reject(new Error('body_too_large'));
        try { req.destroy(); } catch (_) {}
      }
    });
    req.on('end', () => {
      if (!body) return resolve({});
      try { resolve(JSON.parse(body)); } catch (_) { reject(new Error('invalid_json')); }
    });
    req.on('error', reject);
  });
}

function loadDb() {
  try {
    const raw = fs.readFileSync(DB_FILE, 'utf8');
    const parsed = JSON.parse(raw);
    if (!parsed.users) parsed.users = {};
    return parsed;
  } catch (_) {
    return { users: {} };
  }
}

function saveDb(db) {
  const tmp = DB_FILE + '.tmp';
  fs.writeFileSync(tmp, JSON.stringify(db, null, 2));
  fs.renameSync(tmp, DB_FILE);
}

function normalizeUsername(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_-]/g, '')
    .slice(0, 18);
}

function publicUsername(value) {
  return String(value || '')
    .trim()
    .replace(/[^a-zA-Z0-9_-]/g, '')
    .slice(0, 18);
}

function hashPassword(password, salt) {
  return crypto.pbkdf2Sync(String(password), salt, 120000, 32, 'sha256').toString('hex');
}

function createToken(userId) {
  const token = crypto.randomBytes(32).toString('hex');
  sessions.set(token, { userId, createdAt: now(), lastSeen: now() });
  return token;
}

function getToken(req) {
  const header = req.headers.authorization || '';
  const match = /^Bearer\s+(.+)$/i.exec(header);
  return match ? match[1].trim() : '';
}

function getSessionUser(req, db) {
  const token = getToken(req);
  const session = sessions.get(token);
  if (!token || !session) return null;
  const user = db.users[session.userId];
  if (!user) return null;
  session.lastSeen = now();
  return user;
}

function safeRoom(value) {
  return String(value || 'default').slice(0, 160).replace(/[^a-zA-Z0-9:_./-]/g, '_');
}

function safePlayerId(value) {
  return String(value || ('p_' + Math.random().toString(36).slice(2))).slice(0, 80).replace(/[^a-zA-Z0-9_-]/g, '_');
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

function sanitizeSave(save) {
  const clean = save && typeof save === 'object' ? save : {};
  return {
    version: 1,
    map: String(clean.map || 'belencia').slice(0, 32),
    playerName: String(clean.playerName || 'Jugador').slice(0, 18),
    selectedType: String(clean.selectedType || 'mage').slice(0, 20),
    hero: clean.hero && typeof clean.hero === 'object' ? clean.hero : null,
    dragons: Array.isArray(clean.dragons) ? clean.dragons.slice(0, 20) : [],
    updatedAt: now()
  };
}

async function handleApi(req, res) {
  if (req.method === 'OPTIONS') return sendJson(res, 204, {});

  const db = loadDb();

  if (req.url === '/api/register' && req.method === 'POST') {
    try {
      const body = await readBody(req);
      const usernameKey = normalizeUsername(body.username);
      const username = publicUsername(body.username);
      const password = String(body.password || '');

      if (usernameKey.length < 3) return sendJson(res, 400, { ok: false, error: 'El usuario debe tener al menos 3 caracteres.' });
      if (password.length < 4) return sendJson(res, 400, { ok: false, error: 'La contraseña debe tener al menos 4 caracteres.' });
      if (db.users[usernameKey]) return sendJson(res, 409, { ok: false, error: 'Ese usuario ya existe.' });

      const salt = crypto.randomBytes(16).toString('hex');
      const passwordHash = hashPassword(password, salt);
      const user = {
        id: usernameKey,
        username: username || usernameKey,
        salt,
        passwordHash,
        createdAt: now(),
        updatedAt: now(),
        save: null
      };
      db.users[usernameKey] = user;
      saveDb(db);
      const token = createToken(usernameKey);
      return sendJson(res, 200, { ok: true, token, user: { id: user.id, username: user.username }, save: null });
    } catch (_) {
      return sendJson(res, 400, { ok: false, error: 'No pude crear la cuenta.' });
    }
  }

  if (req.url === '/api/login' && req.method === 'POST') {
    try {
      const body = await readBody(req);
      const usernameKey = normalizeUsername(body.username);
      const password = String(body.password || '');
      const user = db.users[usernameKey];
      if (!user || hashPassword(password, user.salt) !== user.passwordHash) {
        return sendJson(res, 401, { ok: false, error: 'Usuario o contraseña incorrectos.' });
      }
      const token = createToken(usernameKey);
      return sendJson(res, 200, { ok: true, token, user: { id: user.id, username: user.username }, save: user.save || null });
    } catch (_) {
      return sendJson(res, 400, { ok: false, error: 'No pude iniciar sesión.' });
    }
  }

  if (req.url === '/api/save' && req.method === 'GET') {
    const user = getSessionUser(req, db);
    if (!user) return sendJson(res, 401, { ok: false, error: 'Sesión inválida. Iniciá sesión de nuevo.' });
    return sendJson(res, 200, { ok: true, save: user.save || null, user: { id: user.id, username: user.username } });
  }

  if (req.url === '/api/save' && req.method === 'POST') {
    try {
      const user = getSessionUser(req, db);
      if (!user) return sendJson(res, 401, { ok: false, error: 'Sesión inválida. Iniciá sesión de nuevo.' });
      const body = await readBody(req);
      user.save = sanitizeSave(body.save || {});
      user.updatedAt = now();
      db.users[user.id] = user;
      saveDb(db);
      return sendJson(res, 200, { ok: true, save: user.save });
    } catch (_) {
      return sendJson(res, 400, { ok: false, error: 'No pude guardar la partida.' });
    }
  }

  return false;
}

const server = http.createServer(async (req, res) => {
  if (req.method === 'OPTIONS') return sendJson(res, 204, {});
  if (req.url && req.url.startsWith('/api/')) {
    const handled = await handleApi(req, res);
    if (handled !== false) return;
  }
  if (req.url === '/health' || req.url === '/') {
    const db = loadDb();
    sendJson(res, 200, { ok: true, service: 'snow-battle-ws', path: PATH, rooms: rooms.size, users: Object.keys(db.users || {}).length });
    return;
  }
  sendText(res, 404, 'Not found');
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

  send(ws, { type: 'welcome', playerId, room: roomName, now: now() });

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
      const payload = { ...msg.player, playerId, page: roomName, updatedAt: now() };
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

  // Limpieza simple de sesiones viejas: 7 días sin uso.
  for (const [token, session] of sessions.entries()) {
    if (t - session.lastSeen > 7 * 24 * 60 * 60 * 1000) sessions.delete(token);
  }
}, CLEANUP_MS);

server.listen(PORT, () => {
  console.log(`Snow Battle WebSocket + accounts listening on port ${PORT}${PATH}`);
  console.log(`Database file: ${DB_FILE}`);
});
