'use strict';

var os = require('os');
var nodeStatic = require('node-static');
const ioRedis = require('socket.io-redis');
const redis = require('redis');
const redisClient = redis.createClient({
    host: process.env.REDIS_HOST,
    port: process.env.REDIS_PORT
})

const cors = require("cors");

//const io = require("socket.io")(httpServer, 


var http = require('http');
var socketIO = require('socket.io');
const port = process.env.PORT;
console.log("Server started on ", port);

let users = {};
var _ = require('underscore');

var fileServer = new(nodeStatic.Server)();
var app = http.createServer(function(req, res) {
  fileServer.serve(req, res);
}).listen(port);

require('events').EventEmitter.defaultMaxListeners = 300;

var io = socketIO.listen(app);
var express = require('express');
app = express();

app.use(express.static('assets'));

/* io = require('socket.io')(http, {
  cors: {
      origins: "*"
  }
});
 */
module.exports.initIO = (httpServer) => {

  io = socket(httpServer, { cors: { origin: '*' } });
  io.adapter(ioRedis({host: process.env.REDIS_HOST, port: process.env.REDIS_PORT}))
    
io.sockets.on('connection', function(socket) {

  let userId = socket.handshake.query.userId; // GET USER ID

    // CHECK IS USER EXHIST 
    if (!users[userId]) users[userId] = [];  

      // PUSH SOCKET ID FOR PARTICULAR USER ID
    users[userId].push(socket.id);
   
  // USER IS ONLINE BROAD CAST TO ALL CONNECTED USERS
  io.sockets.emit("online", userId);
  //io.emit("onlineUser", userId);
  console.log(userId, "Is Online!", socket.id);
  

  // convenience function to log server messages on the client
  function log() {
    console.log("Log Log");
    var array = ['Message from server:'];
    array.push.apply(array, arguments);
    socket.emit('log', array);
  }

  socket.on('message', function(message,roomId) {
    log('Client said: ', message);
    log('Extra: ', roomId);
    if(roomId==null)
    {
      roomId=message.roomId;
      log('null and get it: ', roomId);
    }
    //log('Object: ', roomId.roomId);
    //console.log('Message -roomId: ', roomId);
  //  log('Room Name:' , room);
    // for a real app, would be room-only (not broadcast)
      socket.broadcast.to(roomId).emit('message', message)
      // socket.broadcast.emit('message', message);
  });

  socket.on('create or join', function(room) {
    log('Received request to create or join room ' + room);

    var clientsInRoom = io.sockets.adapter.rooms[room];
    var numClients = clientsInRoom ? Object.keys(clientsInRoom.sockets).length : 0;
    log('Room ' + room + ' now has ' + numClients + ' client(s)');

    if (numClients === 0) {
      socket.join(room);
      log('Client ID ' + socket.id + ' created room ' + room);
      socket.emit('created', room, socket.id);

    } else if (numClients === 1) {
      log('Client ID ' + socket.id + ' joined room ' + room);
      io.sockets.in(room).emit('join', room);
      socket.join(room);
      socket.emit('joined', room, socket.id);
      io.sockets.in(room).emit('ready',room,clientsInRoom.sockets);
    } else { // max two clients
      socket.emit('full', room);
    }
  });

//:JOIN:Client Supplied Room
socket.on('subscribe',function(room){  
  log('subscribe ' + room);
  try{
    console.log('[socket]','join room :',room)
    socket.join(room);
    socket.to(room).emit('user joined', socket.id);
  }catch(e){
    console.log('[error]','join room :',e);
    socket.emit('error','couldnt perform requested action');
  }
})

 //:LEAVE:Client Supplied Room
socket.on('unsubscribe',function(room){  
  log('unsubscribe ' + room);
  try{    
    console.log('[socket]','leave room :', room);
    socket.leave(room);
    socket.to(room).emit('user left', socket.id);
    socket.disconnect();
  }catch(e){
    console.log('[error]','leave room :', e);
    socket.emit('error','couldnt perform requested action');
  }
})

//: RECONNECT:Client Supplied Room
socket.on('reconnect',function(room){  
  log('reconnect ' + room);  
    socket.emit('subscribe', room)
})


  socket.on("send-notification", function (data) {
    log('send-notification ' + data);
    io.emit("new-notification", data);
  });

socket.on('Allow to join',function(room){
  var clientsInRoom = io.sockets.adapter.rooms[room];
  var numClients = clientsInRoom ? Object.keys(clientsInRoom.sockets).length : 0;
  if (numClients === 0) {
    socket.emit('IsActiveStatus', room, 'InActive');
  }
  else
  {
    socket.emit('IsActiveStatus', room, 'Active');
  }
});

  socket.on('ipaddr', function() {
    var ifaces = os.networkInterfaces();
    for (var dev in ifaces) {
      ifaces[dev].forEach(function(details) {
        if (details.family === 'IPv4' && details.address !== '127.0.0.1') {
          socket.emit('ipaddr', details.address);
        }
      });
    }
  });

  socket.on('bye', function(){
    console.log('received bye'); 
  });

  // DISCONNECT EVENT
  socket.on('disconnect', (reason) => {

    // REMOVE FROM SOCKET USERS
  // _.remove(users[userId], (u) => u === socket.id);
    log('disconnect ' + reason);
    if (users[userId].length === 0) {
      // ISER IS OFFLINE BROAD CAST TO ALL CONNECTED USERS
      io.sockets.emit("offline", userId);
      // REMOVE OBJECT
      delete users[userId];
    }
  
    socket.disconnect(); // DISCONNECT SOCKET

    console.log(userId, "Is Offline!", socket.id);

    //   socket.on('disconnect', () => {
  //     const roomID = socketToRoom[socket.id];
  //     let room = users[roomID];
  //     if (room) {
  //         room = room.filter(id => id !== socket.id);
  //         users[roomID] = room;
  //     }
  // });

  });

  

  
});
console.log("Server started on ", port);

  return io;
}

module.exports.getIO = () => {
  if (!io) {
      throw Error("IO not initilized.")
  } else {
      return io;
  }
}
