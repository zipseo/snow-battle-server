'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = Number(process.env.PORT || 3000);
const WS_PATH = process.env.WS_PATH || '/snow-battle-ws';
const PLAYER_TTL_MS = Number(process.env.PLAYER_TTL_MS || 15000);
const CLEANUP_MS = Number(process.env.CLEANUP_MS || 3000);
const DB_FILE = process.env.DB_FILE || path.join(process.env.RENDER_DISK_PATH || __dirname, 'snow-battle-db.json');
const HERO_TYPES = ['mage', 'knight', 'elf'];
const MAPS = new Set(['belencia', 'boreas', 'inferno']);

const rooms = new Map();
const sessions = new Map();

function now() { return Date.now(); }

function send(ws, data) {
  if (!ws || ws.readyState !== ws.OPEN) return false;
  try { ws.send(JSON.stringify(data)); return true; } catch (_) { return false; }
}

function corsHeaders(extra = {}) {
  return {
    'content-type': 'application/json; charset=utf-8',
    'access-control-allow-origin': '*',
    'access-control-allow-methods': 'GET,POST,OPTIONS',
    'access-control-allow-headers': 'content-type,authorization',
    ...extra
  };
}

function sendJson(res, status, payload) {
  res.writeHead(status, corsHeaders());
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
      if (body.length > 2 * 1024 * 1024) {
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

function ensureDbDir() {
  try { fs.mkdirSync(path.dirname(DB_FILE), { recursive: true }); } catch (_) {}
}

function loadDb() {
  try {
    const raw = fs.readFileSync(DB_FILE, 'utf8');
    const parsed = JSON.parse(raw);
    if (!parsed.users || typeof parsed.users !== 'object') parsed.users = {};
    return parsed;
  } catch (_) {
    return { users: {} };
  }
}

function saveDb(db) {
  ensureDbDir();
  const tmp = DB_FILE + '.tmp';
  fs.writeFileSync(tmp, JSON.stringify(db, null, 2));
  fs.renameSync(tmp, DB_FILE);
}

function normalizeUsername(value) {
  return String(value || '').trim().toLowerCase().replace(/[^a-z0-9_-]/g, '').slice(0, 18);
}

function publicUsername(value) {
  return String(value || '').trim().replace(/[^a-zA-Z0-9_-]/g, '').slice(0, 18);
}

function safeString(value, fallback, max = 64) {
  const clean = String(value || fallback || '').replace(/[<>]/g, '').replace(/\s+/g, ' ').trim().slice(0, max);
  return clean || fallback || '';
}

function safeMap(value) {
  const map = String(value || 'belencia').trim().toLowerCase().replace(/[^a-z0-9_-]/g, '');
  return MAPS.has(map) ? map : 'belencia';
}

function safeHeroType(value, fallback = 'mage') {
  const type = String(value || fallback || 'mage').trim().toLowerCase().replace(/[^a-z0-9_-]/g, '');
  return HERO_TYPES.includes(type) ? type : 'mage';
}

function number(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function int(value, fallback = 0, min = -Infinity, max = Infinity) {
  const n = Math.floor(number(value, fallback));
  return Math.max(min, Math.min(max, n));
}

function sanitizePickups(list) {
  if (!Array.isArray(list)) return [];
  return list.slice(0, 120).map((c, index) => ({
    id: safeString(c && c.id, 'pickup_' + index, 80),
    type: c && c.type === 'potion' ? 'potion' : 'coin',
    map: safeMap(c && c.map),
    x: int(c && c.x, 0, 0, 2400),
    y: int(c && c.y, 0, 0, 1400),
    value: int(c && c.value, 1, 1, 999),
    createdAt: int(c && c.createdAt, now(), 0, Number.MAX_SAFE_INTEGER)
  }));
}

function sanitizeHero(hero, type, playerName) {
  if (!hero || typeof hero !== 'object') return null;
  return {
    type,
    className: safeString(hero.className, '', 40),
    name: safeString(hero.name || playerName, playerName || 'Jugador', 18),
    x: int(hero.x, 170, 0, 2400),
    y: int(hero.y, 315, 0, 1400),
    hp: int(hero.hp, 100, 0, 100000),
    maxHp: int(hero.maxHp, 100, 1, 100000),
    alive: hero.alive !== false,
    facing: number(hero.facing, 1) >= 0 ? 1 : -1,
    state: safeString(hero.state, 'idle', 20)
  };
}

function sanitizeDragons(list) {
  if (!Array.isArray(list)) return [];
  return list.slice(0, 30).map((d, index) => ({
    id: int(d && d.id, index + 1, 1, 9999),
    x: int(d && d.x, 0, 0, 2400),
    y: int(d && d.y, 0, 0, 1400),
    hp: int(d && d.hp, 0, 0, 100000),
    maxHp: int(d && d.maxHp, 100, 1, 100000),
    dead: !!(d && d.dead),
    respawnAt: int(d && d.respawnAt, 0, 0, Number.MAX_SAFE_INTEGER)
  }));
}

function sanitizeCharacterSave(save, fallbackType = 'mage') {
  if (!save || typeof save !== 'object') return null;
  const type = safeHeroType(save.selectedType || (save.hero && save.hero.type), fallbackType);
  const playerName = safeString(save.playerName || (save.hero && save.hero.name), 'Jugador', 18);
  const hero = sanitizeHero(save.hero, type, playerName);
  if (!hero) return null;
  return {
    version: 1,
    map: safeMap(save.map),
    playerName,
    selectedType: type,
    playerGold: int(save.playerGold, 0, 0, 999999999),
    playerPoints: int(save.playerPoints, 0, 0, 99),
    playerLevel: int(save.playerLevel, 1, 1, 9999),
    playerPotions: int(save.playerPotions, 0, 0, 999999),
    potionDropMeter: int(save.potionDropMeter, 0, 0, 999999),
    coins: sanitizePickups(save.coins),
    hero,
    dragons: sanitizeDragons(save.dragons),
    updatedAt: int(save.updatedAt, now(), 0, Number.MAX_SAFE_INTEGER)
  };
}

function sanitizeSave(save) {
  const clean = save && typeof save === 'object' ? save : {};
  const account = {
    version: 2,
    activeType: safeHeroType(clean.activeType || clean.selectedType || (clean.hero && clean.hero.type), 'mage'),
    characters: {},
    updatedAt: int(clean.updatedAt, now(), 0, Number.MAX_SAFE_INTEGER)
  };

  // Formato nuevo del HTML actual: { version:2, activeType, characters:{ mage, knight, elf } }
  if (clean.characters && typeof clean.characters === 'object') {
    for (const type of HERO_TYPES) {
      const character = sanitizeCharacterSave(clean.characters[type], type);
      if (character) account.characters[type] = character;
    }
  } else {
    // Compatibilidad con formato viejo: una sola partida con hero en la raíz.
    const legacyType = safeHeroType(clean.selectedType || (clean.hero && clean.hero.type), 'mage');
    const legacy = sanitizeCharacterSave(clean, legacyType);
    if (legacy) account.characters[legacyType] = legacy;
  }

  if (!account.characters[account.activeType]) {
    const firstType = HERO_TYPES.find(type => account.characters[type]);
    if (firstType) account.activeType = firstType;
  }
  account.updatedAt = Math.max(account.updatedAt, ...Object.values(account.characters).map(c => c.updatedAt || 0), now());
  return account;
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

function publicPlayer(record) { return record && record.payload ? record.payload : null; }

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

async function handleApi(req, res) {
  if (req.method === 'OPTIONS') return sendJson(res, 204, {});

  const db = loadDb();
  const url = new URL(req.url, 'http://localhost');

  if (url.pathname === '/api/register' && req.method === 'POST') {
    try {
      const body = await readBody(req);
      const usernameKey = normalizeUsername(body.username);
      const username = publicUsername(body.username);
      const password = String(body.password || '');
      if (usernameKey.length < 3) return sendJson(res, 400, { ok: false, error: 'El usuario debe tener al menos 3 caracteres.' });
      if (password.length < 4) return sendJson(res, 400, { ok: false, error: 'La contraseña debe tener al menos 4 caracteres.' });
      if (db.users[usernameKey]) return sendJson(res, 409, { ok: false, error: 'Ese usuario ya existe.' });
      const salt = crypto.randomBytes(16).toString('hex');
      const user = {
        id: usernameKey,
        username: username || usernameKey,
        salt,
        passwordHash: hashPassword(password, salt),
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

  if (url.pathname === '/api/login' && req.method === 'POST') {
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

  if (url.pathname === '/api/save' && req.method === 'GET') {
    const user = getSessionUser(req, db);
    if (!user) return sendJson(res, 401, { ok: false, error: 'Sesión inválida. Iniciá sesión de nuevo.' });
    return sendJson(res, 200, { ok: true, save: user.save || null, user: { id: user.id, username: user.username } });
  }

  if (url.pathname === '/api/save' && req.method === 'POST') {
    try {
      const user = getSessionUser(req, db);
      if (!user) return sendJson(res, 401, { ok: false, error: 'Sesión inválida. Iniciá sesión de nuevo.' });
      const body = await readBody(req);
      const incoming = sanitizeSave(body.save || {});
      const previous = sanitizeSave(user.save || {});
      // No borrar personajes existentes si el cliente mandó un save incompleto por cierre de pestaña.
      for (const type of HERO_TYPES) {
        if (!incoming.characters[type] && previous.characters[type]) incoming.characters[type] = previous.characters[type];
      }
      if (!incoming.characters[incoming.activeType]) {
        const firstType = HERO_TYPES.find(type => incoming.characters[type]);
        if (firstType) incoming.activeType = firstType;
      }
      incoming.updatedAt = now();
      user.save = incoming;
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
    return sendJson(res, 200, {
      ok: true,
      service: 'snow-battle-ws-accounts-v34',
      path: WS_PATH,
      dbFile: DB_FILE,
      rooms: rooms.size,
      users: Object.keys(db.users || {}).length
    });
  }
  return sendText(res, 404, 'Not found');
});

const wss = new WebSocketServer({ server, path: WS_PATH });

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
    payload: { playerId, visible: false, name: 'Jugador', page: roomName, updatedAt: now() },
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
  for (const [token, session] of sessions.entries()) {
    if (t - session.lastSeen > 7 * 24 * 60 * 60 * 1000) sessions.delete(token);
  }
}, CLEANUP_MS);

server.listen(PORT, () => {
  console.log(`Snow Battle WebSocket + accounts v34 listening on port ${PORT}${WS_PATH}`);
  console.log(`Database file: ${DB_FILE}`);
});
