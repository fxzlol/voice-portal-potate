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
      // Проверим, все ли ещё на связи
      const validUsers = users.filter(id => io.sockets.sockets.get(id));
      if (validUsers.length < 3) {
        // Если кто-то отвалился, возвращаем остальных в очередь
        validUsers.forEach(id => waitingQueue.push(id));
        continue;
      }
      const groupId = `group_${groupCounter++}`;
      groups[groupId] = validUsers;
      // Уведомляем каждого о группе
      validUsers.forEach(id => {
        const otherIds = validUsers.filter(uid => uid !== id);
        io.to(id).emit('matched', { groupId, members: otherIds });
      });
      console.log(`[Group ${groupId}] matched: ${validUsers.join(', ')}`);
    }
  }

  // Сигнальные сообщения (пересылаем только между участниками группы)
  socket.on('signal', ({ to, data, groupId }) => {
    if (groups[groupId] && groups[groupId].includes(socket.id) && groups[groupId].includes(to)) {
      io.to(to).emit('signal', { from: socket.id, data, groupId });
    }
  });

  // Скип (выход из группы)
  socket.on('skip', () => {
    handleSkip(socket.id);
  });

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

    // Отправить всем участникам, что кто-то вышел
    members.forEach(id => {
      const s = io.sockets.sockets.get(id);
      if (s) {
        if (id === socketId) {
          s.emit('partner-disconnected', { reason: 'you-left' });
          // добавим в очередь для поиска нового
          waitingQueue.push(id);
          s.emit('search-status', { status: 'waiting' });
        } else {
          s.emit('partner-disconnected', { reason: 'other-left', leftId: socketId });
          // остальные тоже идут в очередь
          waitingQueue.push(id);
          s.emit('search-status', { status: 'waiting' });
        }
      }
    });
    // Пытаемся создать новые группы
    tryMatch();
  }

  socket.on('stop-search', () => {
    const idx = waitingQueue.indexOf(socket.id);
    if (idx !== -1) waitingQueue.splice(idx, 1);
    handleSkip(socket.id); // также выйдет из группы, если был
    socket.emit('search-status', { status: 'idle' });
  });

  socket.on('disconnect', () => {
    const idx = waitingQueue.indexOf(socket.id);
    if (idx !== -1) waitingQueue.splice(idx, 1);
    // Если был в группе, удаляем его, остальных отправляем обратно в очередь
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
