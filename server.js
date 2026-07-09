const { WebSocketServer } = require('ws');
const http = require('http');
const fs = require('fs');
const path = require('path');
const Database = require('better-sqlite3');
const crypto = require('crypto');

const PORT = 3000;

// Setup database
const db = new Database('chat.db');
db.pragma('journal_mode = WAL');

db.exec(`
  CREATE TABLE IF NOT EXISTS rooms (
    id TEXT PRIMARY KEY,
    name TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );
  CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    room_id TEXT NOT NULL,
    username TEXT NOT NULL,
    color TEXT NOT NULL,
    text TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (room_id) REFERENCES rooms(id)
  );
`);

const insertMessage = db.prepare('INSERT INTO messages (room_id, username, color, text) VALUES (?, ?, ?, ?)');
const getMessages = db.prepare('SELECT * FROM messages WHERE room_id = ? ORDER BY id DESC LIMIT 50');
const createRoom = db.prepare('INSERT OR IGNORE INTO rooms (id, name) VALUES (?, ?)');
const getRoom = db.prepare('SELECT * FROM rooms WHERE id = ?');

// Create default room
createRoom.run('general', 'General Chat');

const server = http.createServer((req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  
  if (url.pathname === '/') {
    res.writeHead(200, { 'Content-Type': 'text/html' });
    fs.createReadStream(path.join(__dirname, 'client.html')).pipe(res);
  } else if (url.pathname === '/room') {
    const roomId = url.searchParams.get('id');
    if (roomId) {
      res.writeHead(200, { 'Content-Type': 'text/html' });
      fs.createReadStream(path.join(__dirname, 'client.html')).pipe(res);
    } else {
      res.writeHead(302, { Location: '/?id=general' });
      res.end();
    }
  } else {
    res.writeHead(404);
    res.end('Not found');
  }
});

const wss = new WebSocketServer({ server });

const rooms = new Map(); // roomId -> Set of {ws, username, color}
const colorPalette = ['#e94560', '#0f3460', '#533483', '#2b9348', '#e85d04', '#7209b7', '#4895ef', '#f72585'];
let colorIndex = 0;

function getUserColor(username) {
  return colorPalette[colorIndex++ % colorPalette.length];
}

function generateRoomId() {
  return crypto.randomBytes(4).toString('hex');
}

function broadcastToRoom(roomId, data, excludeWs = null) {
  const room = rooms.get(roomId);
  if (!room) return;
  
  for (const client of room) {
    if (client.ws !== excludeWs && client.ws.readyState === 1) {
      client.ws.send(JSON.stringify(data));
    }
  }
}

function broadcastRoomUsers(roomId) {
  const room = rooms.get(roomId);
  if (!room) return;
  
  const users = Array.from(room).map(c => ({ name: c.username, color: c.color }));
  broadcastToRoom(roomId, { type: 'users', users });
}

wss.on('connection', (ws, req) => {
  let currentRoom = null;

  ws.on('message', (data) => {
    try {
      const msg = JSON.parse(data.toString());

      switch (msg.type) {
        case 'join': {
          const roomId = msg.room || 'general';
          const username = msg.username;
          const color = getUserColor(username);
          
          // Create room if doesn't exist
          const roomName = msg.roomName || `Room ${roomId}`;
          createRoom.run(roomId, roomName);
          
          // Add to in-memory room
          if (!rooms.has(roomId)) rooms.set(roomId, new Set());
          const client = { ws, username, color };
          rooms.get(roomId).add(client);
          currentRoom = roomId;
          
          // Load last 50 messages
          const history = getMessages.all(roomId).reverse();
          ws.send(JSON.stringify({ type: 'history', messages: history, color, roomId }));
          
          // Notify others
          broadcastToRoom(roomId, {
            type: 'system',
            text: `${username} joined the chat`,
            time: new Date().toLocaleTimeString()
          }, ws);
          broadcastRoomUsers(roomId);
          break;
        }

        case 'message': {
          const room = rooms.get(currentRoom);
          if (!room) return;
          
          const client = Array.from(room).find(c => c.ws === ws);
          if (!client) return;
          
          // Save to database
          insertMessage.run(currentRoom, client.username, client.color, msg.text);
          
          // Broadcast to room
          broadcastToRoom(currentRoom, {
            type: 'message',
            username: client.username,
            color: client.color,
            text: msg.text,
            time: new Date().toLocaleTimeString()
          });
          break;
        }

        case 'typing': {
          const room = rooms.get(currentRoom);
          if (!room) return;
          const client = Array.from(room).find(c => c.ws === ws);
          if (client) {
            broadcastToRoom(currentRoom, { type: 'typing', username: client.username }, ws);
          }
          break;
        }

        case 'stopTyping': {
          broadcastToRoom(currentRoom, { type: 'stopTyping' }, ws);
          break;
        }
      }
    } catch (e) {
      console.error('Message error:', e);
    }
  });

  ws.on('close', () => {
    if (currentRoom) {
      const room = rooms.get(currentRoom);
      if (room) {
        const client = Array.from(room).find(c => c.ws === ws);
        if (client) {
          broadcastToRoom(currentRoom, {
            type: 'system',
            text: `${client.username} left the chat`,
            time: new Date().toLocaleTimeString()
          });
          room.delete(client);
          broadcastRoomUsers(currentRoom);
        }
      }
    }
  });
});

server.listen(PORT, () => {
  console.log(`Chat server running at http://localhost:${PORT}`);
  console.log(`Share this link: http://localhost:${PORT}/?id=general`);
});
