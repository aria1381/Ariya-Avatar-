import { connect } from 'cloudflare:sockets';

const userID = 'b81329a1-a50d-408d-8a4b-84a56e792e3c';
const proxyIP = ''; 

export default {
  async fetch(request, env, ctx) {
    try {
      const upgradeHeader = request.headers.get('Upgrade');
      if (!upgradeHeader || upgradeHeader !== 'websocket') {
        return new Response('Ariya GitHub Worker Active', { status: 200 });
      }
      return await vlessWebSocketHandler(request);
    } catch (err) {
      return new Response(err.toString(), { status: 500 });
    }
  }
};

async function vlessWebSocketHandler(request) {
  const webSocketPair = new WebSocketPair();
  const [client, server] = Object.values(webSocketPair);
  server.accept();

  let address = '';
  let portWithRandomLog = '';
  let remoteSocket = null;

  async function wsServerReady(chunk) {
    if (chunk.byteLength < 18) {
      throw new Error('invalid data');
    }
    const version = chunk[0];
    const id = chunk.slice(1, 17);
    if (!isValidUUID(id, userID)) {
      throw new Error('invalid uuid');
    }
    const optLength = chunk[17];
    let offset = 18 + optLength;
    const cmd = chunk[offset];
    if (cmd !== 1 && cmd !== 2) {
      throw new Error('command not supported');
    }
    portWithRandomLog = chunk.slice(offset + 1, offset + 3);
    const port = portWithRandomLog[0] * 256 + portWithRandomLog[1];
    offset += 3;
    const addrType = chunk[offset];
    offset += 1;
    let targetAddress = '';
    if (addrType === 1) {
      targetAddress = Array.from(chunk.slice(offset, offset + 4)).join('.');
      offset += 4;
    } else if (addrType === 2) {
      const addrLen = chunk[offset];
      offset += 1;
      targetAddress = new TextDecoder().decode(chunk.slice(offset, offset + addrLen));
      offset += addrLen;
    } else if (addrType === 3) {
      targetAddress = '[' + Array.from(chunk.slice(offset, offset + 16)).map(b => b.toString(16).padStart(2, '0')).reduce((acc, b, i) => i % 2 ? acc + b + ':' : acc + b, '').slice(0, -1) + ']';
      offset += 16;
    }
    address = targetAddress;
    
    const rawClientData = chunk.slice(offset);
    
    remoteSocket = connect({ hostname: proxyIP || address, port: port });
    const writer = remoteSocket.writable.getWriter();
    await writer.write(rawClientData);
    writer.releaseLock();

    remoteSocket.readable.pipeTo(new WritableStream({
      async write(chunk) {
        server.send(chunk);
      },
      close() {
        server.close();
      },
      abort(reason) {
        server.close();
      }
    })).catch(() => {});
  }

  server.addEventListener('message', async (event) => {
    try {
      const data = event.data;
      if (!remoteSocket) {
        await wsServerReady(data);
      } else {
        const writer = remoteSocket.writable.getWriter();
        await writer.write(data);
        writer.releaseLock();
      }
    } catch (err) {
      server.close();
    }
  });

  return new Response(null, {
    status: 101,
    webSocket: client,
  });
}

function isValidUUID(array, uuid) {
  const cleanUUID = uuid.replace(/-/g, '');
  for (let i = 0; i < 16; i++) {
    const b = parseInt(cleanUUID.substr(i * 2, 2), 16);
    if (array[i] !== b) return false;
  }
  return true;
}
