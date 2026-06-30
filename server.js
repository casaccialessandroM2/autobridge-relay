'use strict';

const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const HEARTBEAT_TIMEOUT_MS = 10000;

// sessions: session_id -> { mac: ws | null, windows: ws | null }
const sessions = new Map();

// wsInfo: ws -> { session_id, platform, last_heartbeat, timer }
const wsInfo = new Map();

function generateSessionId() {
  // Caratteri NON ambigui: niente 0/O, 1/I, per evitare errori di lettura del codice.
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let id;
  do {
    id = Array.from({ length: 6 }, () => chars[Math.floor(Math.random() * chars.length)]).join('');
  } while (sessions.has(id));
  return id;
}

function send(ws, obj) {
  if (ws && ws.readyState === ws.OPEN) {
    ws.send(JSON.stringify(obj));
  }
}

function getPeer(session, platform) {
  if (!session) return null;
  return platform === 'mac' ? session.windows : session.mac;
}

function cleanupClient(ws) {
  const info = wsInfo.get(ws);
  if (!info) return;

  clearTimeout(info.timer);
  wsInfo.delete(ws);

  const { session_id, platform } = info;
  const session = sessions.get(session_id);
  if (!session) return;

  session[platform] = null;

  const peer = getPeer(session, platform);
  if (peer && peer.readyState === peer.OPEN) {
    send(peer, { type: 'peer_disconnected' });
  }

  if (!session.mac && !session.windows) {
    sessions.delete(session_id);
    console.log(`[${session_id}] Session deleted`);
  } else {
    console.log(`[${session_id}] ${platform} disconnected, peer notified`);
  }
}

function resetHeartbeatTimer(ws) {
  const info = wsInfo.get(ws);
  if (!info) return;
  clearTimeout(info.timer);
  info.timer = setTimeout(() => {
    console.log(`[${info.session_id}] Heartbeat timeout for ${info.platform}`);
    ws.terminate();
  }, HEARTBEAT_TIMEOUT_MS);
}

// ── HTTP server (health check per Railway/Render) ─────────────────────────
const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      status: 'ok',
      sessions: sessions.size,
      clients: wsInfo.size,
      codes: Array.from(sessions.entries()).map(([id, s]) => ({
        id, mac: !!s.mac, windows: !!s.windows,
      })),
      uptime: process.uptime(),
    }));
  } else {
    res.writeHead(426, { 'Content-Type': 'text/plain' });
    res.end('Upgrade Required — WebSocket only');
  }
});

const wss = new WebSocketServer({ server });

wss.on('connection', (ws, req) => {
  const remoteAddr = req.headers['x-forwarded-for'] || req.socket.remoteAddress;
  console.log(`New connection from ${remoteAddr}`);

  ws.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      send(ws, { type: 'error', message: 'Invalid JSON' });
      return;
    }

    switch (msg.type) {
      case 'register': {
        if (msg.platform !== 'mac') {
          send(ws, { type: 'error', message: 'Only mac can register' });
          return;
        }
        const session_id = generateSessionId();
        sessions.set(session_id, { mac: ws, windows: null });
        wsInfo.set(ws, { session_id, platform: 'mac', last_heartbeat: Date.now(), timer: null });
        resetHeartbeatTimer(ws);
        console.log(`[${session_id}] Mac registered (vin=${msg.vin || 'n/a'})`);
        send(ws, { type: 'session_created', session_id, status: 'waiting' });
        break;
      }

      case 'join': {
        if (msg.platform !== 'windows') {
          send(ws, { type: 'error', message: 'Only windows can join' });
          return;
        }
        const session = sessions.get(msg.session_id);
        if (!session) {
          send(ws, { type: 'error', reason: 'Codice sessione non trovato o scaduto' });
          return;
        }
        if (session.windows) {
          send(ws, { type: 'error', reason: 'Sessione già occupata da un altro tecnico' });
          return;
        }
        session.windows = ws;
        wsInfo.set(ws, { session_id: msg.session_id, platform: 'windows', last_heartbeat: Date.now(), timer: null });
        resetHeartbeatTimer(ws);
        console.log(`[${msg.session_id}] Windows joined`);
        send(ws, { type: 'session_joined', session_id: msg.session_id, status: 'connected' });
        if (session.mac) {
          send(session.mac, { type: 'peer_connected', peer_platform: 'windows', session_id: msg.session_id });
        }
        break;
      }

      case 'heartbeat': {
        const info = wsInfo.get(ws);
        if (!info) return;
        info.last_heartbeat = Date.now();
        resetHeartbeatTimer(ws);
        send(ws, { type: 'heartbeat_ack', session_id: info.session_id });
        break;
      }

      case 'data': {
        const info = wsInfo.get(ws);
        if (!info) return;
        const session = sessions.get(info.session_id);
        if (!session) return;
        const peer = getPeer(session, info.platform);
        if (!peer || peer.readyState !== peer.OPEN) {
          send(ws, { type: 'error', reason: 'Peer non connesso' });
          return;
        }
        // Forward raw payload senza modifiche
        send(peer, { type: 'data', payload: msg.payload });
        break;
      }

      case 'disconnect': {
        const info = wsInfo.get(ws);
        if (info) console.log(`[${info.session_id}] ${info.platform} sent disconnect`);
        cleanupClient(ws);
        ws.close();
        break;
      }

      default:
        send(ws, { type: 'error', message: `Unknown type: ${msg.type}` });
    }
  });

  ws.on('close', () => cleanupClient(ws));
  ws.on('error', (err) => {
    console.error(`WS error: ${err.message}`);
    cleanupClient(ws);
  });
});

server.listen(PORT, () => {
  console.log(`AutoBridge relay listening on port ${PORT}`);
});
