from fastapi import WebSocket, APIRouter

class WebSocketManager:
    def __init__(self):
        self.connections = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.connections.remove(websocket)

    async def send_update(self, message: str):
        for connection in self.connections:
            await connection.send_text(message)


manager = WebSocketManager()

router = APIRouter()

@router.websocket('/ws')
async def websocket_endpoint(websocket:WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    finally:
        manager.disconnect(websocket)