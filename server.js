const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = socketIo(server);

app.use(express.static(__dirname));
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'index.html'));
});

// Очередь ожидающих (socket.id)
let waitingQueue = [];
// Пары: { roomId: { user1, user2 } }
let pairs = {};
let roomCounter = 0;

io.on('connection', (socket) => {
  console.log(`[${socket.id}] connected`);

  socket.on('start-search', () => {
    if (waitingQueue.includes(socket.id)) return;
    // Проверим, не в паре ли
    for (let room in pairs) {
      if (pairs[room].user1 === socket.id || pairs[room].user2 === socket.id) return;
    }
    waitingQueue.push(socket.id);
    socket.emit('search-status', { status: 'waiting' });
    tryMatch();
  });

  function tryMatch() {
    while (waitingQueue.length >= 2) {
      const user1 = waitingQueue.shift();
      const user2 = waitingQueue.shift();
      const s1 = io.sockets.sockets.get(user1);
      const s2 = io.sockets.sockets.get(user2);
      if (!s1 || !s2) {
        if (s1) waitingQueue.unshift(user1);
        if (s2) waitingQueue.unshift(user2);
        continue;
      }
      const roomId = `room_${roomCounter++}`;
      pairs[roomId] = { user1, user2 };
      s1.emit('matched', { partnerId: user2, room: roomId });
      s2.emit('matched', { partnerId: user1, room: roomId });
      console.log(`[${user1}] matched with [${user2}] in ${roomId}`);
    }
  }

  // Сигнальные сообщения (только для пары)
  socket.on('offer', ({ to, offer, room }) => {
    if (pairs[room] && (pairs[room].user1 === socket.id || pairs[room].user2 === socket.id)) {
      io.to(to).emit('offer', { from: socket.id, offer, room });
    }
  });

  socket.on('answer', ({ to, answer, room }) => {
    if (pairs[room] && (pairs[room].user1 === socket.id || pairs[room].user2 === socket.id)) {
      io.to(to).emit('answer', { from: socket.id, answer, room });
    }
  });

  socket.on('ice-candidate', ({ to, candidate, room }) => {
    if (pairs[room] && (pairs[room].user1 === socket.id || pairs[room].user2 === socket.id)) {
      io.to(to).emit('ice-candidate', { from: socket.id, candidate, room });
    }
  });

  // Скип
  socket.on('skip', () => handleSkip(socket.id));

  function handleSkip(socketId) {
    let foundRoom = null, partnerId = null;
    for (let room in pairs) {
      if (pairs[room].user1 === socketId) {
        foundRoom = room;
        partnerId = pairs[room].user2;
        break;
      } else if (pairs[room].user2 === socketId) {
        foundRoom = room;
        partnerId = pairs[room].user1;
        break;
      }
    }
    if (!foundRoom) return;
    delete pairs[foundRoom];

    const s1 = io.sockets.sockets.get(socketId);
    const s2 = io.sockets.sockets.get(partnerId);
    if (s1) {
      s1.emit('partner-disconnected');
      waitingQueue.push(socketId);
      s1.emit('search-status', { status: 'waiting' });
    }
    if (s2) {
      s2.emit('partner-disconnected');
      waitingQueue.push(partnerId);
      s2.emit('search-status', { status: 'waiting' });
    }
    tryMatch();
  }

  socket.on('stop-search', () => {
    const idx = waitingQueue.indexOf(socket.id);
    if (idx !== -1) waitingQueue.splice(idx, 1);
    handleSkip(socket.id);
    socket.emit('search-status', { status: 'idle' });
  });

  socket.on('disconnect', () => {
    const idx = waitingQueue.indexOf(socket.id);
    if (idx !== -1) waitingQueue.splice(idx, 1);
    let foundRoom = null, partnerId = null;
    for (let room in pairs) {
      if (pairs[room].user1 === socket.id) {
        foundRoom = room;
        partnerId = pairs[room].user2;
        break;
      } else if (pairs[room].user2 === socket.id) {
        foundRoom = room;
        partnerId = pairs[room].user1;
        break;
      }
    }
    if (foundRoom) {
      delete pairs[foundRoom];
      if (partnerId) {
        const ps = io.sockets.sockets.get(partnerId);
        if (ps) {
          ps.emit('partner-disconnected');
          waitingQueue.push(partnerId);
          ps.emit('search-status', { status: 'waiting' });
          tryMatch();
        }
      }
    }
    console.log(`[${socket.id}] disconnected`);
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Server running on port ${PORT}`));
