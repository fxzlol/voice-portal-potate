const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = socketIo(server);

app.use(express.static(path.join(__dirname, 'public')));

// Хранилище комнат: { roomName: { users: { socketId: { username, socketId } } } }
const rooms = {};

io.on('connection', (socket) => {
  console.log(`[${socket.id}] connected`);

  socket.on('join-room', ({ room, username }) => {
    socket.join(room);
    if (!rooms[room]) rooms[room] = { users: {} };
    rooms[room].users[socket.id] = { username, socketId: socket.id };

    // Отправить список всех пользователей в комнате
    io.to(room).emit('room-users', rooms[room].users);
    // Подтверждение для нового пользователя
    socket.emit('room-joined', { room, users: rooms[room].users });
    console.log(`[${socket.id}] joined room ${room}`);
  });

  // Сигнальные события WebRTC
  socket.on('offer', ({ to, offer, room }) => {
    io.to(to).emit('offer', { from: socket.id, offer, room });
  });

  socket.on('answer', ({ to, answer, room }) => {
    io.to(to).emit('answer', { from: socket.id, answer, room });
  });

  socket.on('ice-candidate', ({ to, candidate, room }) => {
    io.to(to).emit('ice-candidate', { from: socket.id, candidate, room });
  });

  socket.on('disconnect', () => {
    // Найти комнату, где был пользователь, и удалить его
    for (const room in rooms) {
      if (rooms[room].users[socket.id]) {
        delete rooms[room].users[socket.id];
        io.to(room).emit('room-users', rooms[room].users);
        if (Object.keys(rooms[room].users).length === 0) {
          delete rooms[room];
        }
        break;
      }
    }
    console.log(`[${socket.id}] disconnected`);
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
