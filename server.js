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

// Очередь ожидающих
let waitingQueue = [];
// Группы: { groupId: [socketId1, socketId2, socketId3] }
let groups = {};
let groupCounter = 0;

io.on('connection', (socket) => {
  console.log(`[${socket.id}] connected`);

  socket.on('start-search', () => {
    if (waitingQueue.includes(socket.id)) return;
    // Проверим, не в группе ли уже
    for (let gid in groups) {
      if (groups[gid].includes(socket.id)) return;
    }
    waitingQueue.push(socket.id);
    socket.emit('search-status', { status: 'waiting' });
    tryMatch();
  });

  function tryMatch() {
    while (waitingQueue.length >= 3) {
      const users = waitingQueue.splice(0, 3);
      // Проверим, все ли на связи
      const validUsers = users.filter(id => io.sockets.sockets.get(id));
      if (validUsers.length < 3) {
        validUsers.forEach(id => waitingQueue.push(id));
        continue;
      }
      const groupId = `group_${groupCounter++}`;
      groups[groupId] = validUsers;

      // Для каждого участника определяем, с кем он будет инициатором
      const memberData = validUsers.map(id => {
        // Для каждого другого участника определяем, инициатор ли этот id в паре
        const others = validUsers.filter(other => other !== id);
        const relations = others.map(otherId => {
          // Инициатор — тот, чей id меньше (лексикографически)
          const initiator = id < otherId;
          return { id: otherId, initiator };
        });
        return { id, relations };
      });

      // Отправляем каждому его данные
      memberData.forEach(({ id, relations }) => {
        io.to(id).emit('matched', { groupId, relations });
      });

      console.log(`[Group ${groupId}] matched: ${validUsers.join(', ')}`);
    }
  }

  // Сигнальные сообщения
  socket.on('signal', ({ to, data, groupId }) => {
    if (groups[groupId] && groups[groupId].includes(socket.id) && groups[groupId].includes(to)) {
      io.to(to).emit('signal', { from: socket.id, data, groupId });
    }
  });

  // Скип
  socket.on('skip', () => handleSkip(socket.id));

  function handleSkip(socketId) {
    let groupId = null;
    for (let gid in groups) {
      if (groups[gid].includes(socketId)) {
        groupId = gid;
        break;
      }
    }
    if (!groupId) return;
    const members = groups[groupId];
    delete groups[groupId];

    // Отправляем всем, что кто-то ушёл
    members.forEach(id => {
      const s = io.sockets.sockets.get(id);
      if (s) {
        if (id === socketId) {
          s.emit('partner-disconnected', { reason: 'you-left' });
          waitingQueue.push(id);
          s.emit('search-status', { status: 'waiting' });
        } else {
          s.emit('partner-disconnected', { reason: 'other-left', leftId: socketId });
          waitingQueue.push(id);
          s.emit('search-status', { status: 'waiting' });
        }
      }
    });
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
    let groupId = null;
    for (let gid in groups) {
      if (groups[gid].includes(socket.id)) {
        groupId = gid;
        break;
      }
    }
    if (groupId) {
      const members = groups[groupId];
      delete groups[groupId];
      members.forEach(id => {
        if (id !== socket.id) {
          const s = io.sockets.sockets.get(id);
          if (s) {
            s.emit('partner-disconnected', { reason: 'other-left', leftId: socket.id });
            waitingQueue.push(id);
            s.emit('search-status', { status: 'waiting' });
          }
        }
      });
      tryMatch();
    }
    console.log(`[${socket.id}] disconnected`);
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Server running on port ${PORT}`));
