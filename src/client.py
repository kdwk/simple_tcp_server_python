from server import served, readSettings, CommandType, UserCommand, Command, Response, ResponseCode, sever, reintegrate, getsize
from socket import socket, AF_INET, SOCK_STREAM, error as SocketError
from os.path import exists
from threading import Thread, RLock
import sys
from math import ceil
import os
from time import sleep, time_ns

debug = False

downloadLock = RLock()

def error(message: str) -> None:
    global debug
    if debug:
        print('\033[31m' + message + '\033[0m', end="" if message.endswith("\n") else "\n")
    return None

def log(message: str):
    global debug
    if debug:
        print(message, end="" if message.endswith("\n") else "\n")

class PartialDownload:
    filename: str
    size: int # bytes
    chunks: dict[int, str] # Map chunk no. to bytes

    def __init__(self, filename: str, size: int):
        self.filename = filename
        self.size = size
        self.chunks = dict()
    
    def noOfChunks(self) -> int:
        return ceil(float(self.size) / 100.0)

    def isComplete(self) -> bool:
        return all([chunk in self.chunks for chunk in range(self.noOfChunks())])
    
    def save(self, chunk: int, content: str):
        self.chunks[chunk] = content
    
    def writeToDisk(self):
        log("In PartialDownload.writeToDisk() point 0")
        if not self.isComplete():
            return error(f"Upload for {self.filename} is not complete")
        log("In PartialDownload.writeToDisk() point 1")
        if exists(served(self.filename)):
            return error(f"{served(self.filename)} already exists")
        log("In PartialDownload.writeToDisk() point 2")
        log(f"{self.chunks}")
        try:
            with open(served(self.filename), 'x') as f:
                combined = reintegrate([self.chunks[chunk] for chunk in range(self.noOfChunks())])
                noChars = f.write(combined)
                assert noChars == len(combined)
        except Exception as e:
            error(f"{e}")
        log("In PartialDownload.writeToDisk() point 3")

class ClientWorker(Thread):
    clientSocket: socket
    serverAddr: str
    serverPort: int
    userCommand: UserCommand
    serverId: str
    download: PartialDownload | None
    downloadChunkIds: list[int] | None

    def __init__(self, group=None, target=None, name=None, args=..., kwargs=None, *, daemon=None):
        super().__init__(group, target, name, args, kwargs, daemon=daemon)
        self.clientSocket = socket(AF_INET, SOCK_STREAM)
        self.clientSocket.settimeout(0.5)
        self.serverId = kwargs["serverId"]
        self.serverAddr, self.serverPort = kwargs["server"]
        self.download = kwargs["download"]
        self.downloadChunkIds = kwargs["downloadChunkIds"]
        try:
            self.clientSocket.connect((self.serverAddr, self.serverPort))
        except (SocketError, ConnectionRefusedError, TimeoutError):
            print(f"TCP connection to server {self.serverId} failed")
            raise
        self.userCommand = kwargs["userCommand"]
        self._return = None
        log(f"New ClientWorker connected to server {self.serverId}")

    def getResponse(self, command: Command) -> Response | None:
        try:
            print(f"Client ({self.serverId}): {command.toDisplayString()}")
            self.clientSocket.send(command.toSafeString().encode())
        except (SocketError, TimeoutError):
            return error(f"Could not send command to {self.servers[id]}")
        responseStr = ""
        start = time_ns()
        while not responseStr.endswith("\n"):
            now = time_ns()
            if now - start > 1E9:
                return error("Timeout while receiving")
            try:
                buffer = self.clientSocket.recv(1024)
            except (SocketError, TimeoutError):
                return error(f"Could not receive response from {self.servers[id]}")
            if len(buffer) == 0:
                return error(f"Server {self.serverId} closed connection")
            responseStr += buffer.decode()
        response: Response = Response.fromSafeString(responseStr)
        print(f"Server ({self.serverId}): {response.toDisplayString()}")
        return response

    def handleFilelist(self):
        assert self.userCommand.type == CommandType.FILELIST
        response = self.getResponse(
            Command(CommandType.FILELIST, filename=self.userCommand.filename))
        if response is None:
            return error("No response in ClientWorker.handleFilelist")
        if response.code.err():
            return error(response.toString())

    def handleUpload(self):
        assert self.userCommand.type == CommandType.UPLOAD
        assert self.userCommand.filename is not None
        name = self.userCommand.filename
        path = served(name)
        if not exists(path):
            # print(f"Peer {}") # TODO
            return error(f"File {name} does not exist")
        size = getsize(path)
        response = self.getResponse(
            Command(CommandType.UPLOAD, filename=name, bytes=size))
        if response is None or not response.code.ready():
            return error(response.toString() if response is not None else "Response is None in ClientWorker.handleUpload()")
        chunks = sever(path)
        log(f"{len(chunks)} chunks\n\n{chunks}")
        for i, chunk in enumerate(chunks):
            log(f"Send chunk {i}\n{chunk}")
            response = self.getResponse(
                Command(CommandType.UPLOAD, filename=name, chunk=i, content=chunk))
            if response is None or not response.code.ok():
                return error(f"File {name} upload failed")
            if i == len(chunks) - 1 and response.content == f"File {name} received":
                print(f"File {name} upload success")
            

    def handleDownload(self):
        # Download intention declaration should already have been sent
        assert self.userCommand.type == CommandType.DOWNLOAD
        assert self.userCommand.filename is not None
        assert self.download is not None
        if self.downloadChunkIds is None or len(self.downloadChunkIds) == 0:
            log(f"Nothing to download from server {self.serverId}")
            return
        name = self.userCommand.filename
        for chunk in self.downloadChunkIds:
            response = self.getResponse(Command(CommandType.DOWNLOAD, filename=name, chunk=chunk))
            if response is None or not response.code.ok():
                return error(f"File {name} download failed")
            with downloadLock:
                content = response.content
                if content.endswith("\n"):
                    content = content.splitlines()[0]
                log(f"{content.split(" ", 4)}")
                _, receivedFilename, _, receivedChunkNo, content = content.split(" ", 4)
                receivedChunkNo = int(receivedChunkNo)
                if receivedFilename != self.userCommand.filename or receivedChunkNo != chunk:
                    return error("Received wrong file or chunk from server")
                self.download.save(chunk, content)
                if self.download.isComplete():
                    self.download.writeToDisk()
                    print(f"File {self.download.filename} download success")
                    return

    def handleDelete(self):
        assert self.userCommand.type == CommandType.DELETE
        assert self.userCommand.filename is not None
        response = self.getResponse(
            Command(CommandType.DELETE, filename=self.userCommand.filename))
        if response is None or not response.code.ok():
            return error(f"File {self.userCommand.filename} delete failed")
        print(f"File {self.userCommand.filename} delete success")

    def run(self):
        try:
            match self.userCommand.type:
                case CommandType.FILELIST:
                    self.handleFilelist()
                case CommandType.UPLOAD:
                    self.handleUpload()
                case CommandType.DOWNLOAD:
                    self.handleDownload()
                case CommandType.DELETE:
                    self.handleDelete()
                case _:
                    return error("Not implemented")
        except KeyboardInterrupt:
            return

    def join(self, timeout=None):
        super().join(timeout)
        self.clientSocket.close()
        log("Join ClientWorker, closed clientSocket")


class Client:
    servers: dict[str, tuple[str, int]]  # Map server ids to (addr, port)
    download: PartialDownload | None

    def __init__(self):
        global debug
        if len(sys.argv) == 2 and sys.argv[1] == "debug":
            debug = True
        self.servers = readSettings()
        self.download = None

    def getResponse(self, command: Command, clientSocket: socket | None) -> Response | None:
        if clientSocket is None:
            return error("clientSocket is None in Client.getResponse()")
        try:
            log(f"Sending command: {command.toString()}")
            clientSocket.send(command.toSafeString().encode())
        except SocketError:
            return error(f"Could not connect to {self.servers[id]}")
        responseStr = ""
        while not responseStr.endswith("\n"):
            buffer = clientSocket.recv(1024)
            if len(buffer) == 0:
                return error(f"Server closed connection")
            responseStr += buffer.decode()
        return Response.fromSafeString(responseStr)

    def newSocket(self, server: tuple[str, int]) -> socket | None:
        try:
            ret = socket(AF_INET, SOCK_STREAM)
            ret.connect(server)
            return ret
        except (SocketError, ConnectionRefusedError):
            return error(f"Could not connect to {server} in Client.newSocket")

    def run(self):
        while True:
            commandStr = input("Input your command: ")
            if commandStr == "q":
                return
            elif commandStr == "clear":
                os.system('clear' if os.name == 'posix' else 'cls')
                continue
            userCommand: UserCommand = UserCommand.fromString(commandStr)
            if userCommand is None:
                continue
            assert userCommand is not None
            log(f"Confirm userCommand: {userCommand.toString()}")

            validPeers = userCommand.ids
            downloadResponse: Response | None = None
            noOfChunks = 0
            if userCommand.type == CommandType.UPLOAD and userCommand.filename is not None:
                if not exists(served(userCommand.filename)):
                    print(f"Peer self_id does not serve file {userCommand.filename}")
                    continue
                print(f"Uploading {userCommand.filename}")
            if userCommand.type == CommandType.DOWNLOAD and userCommand.filename is not None:
                if exists(served(userCommand.filename)):
                    print(f"File {userCommand.filename} already exists")
                    continue
                print(f"Downloading {userCommand.filename}")
                for id in validPeers:
                    clientSocket = self.newSocket(self.servers[id])
                    downloadResponse = self.getResponse(Command(
                        type=CommandType.DOWNLOAD, filename=userCommand.filename), clientSocket=clientSocket)
                    if downloadResponse is None or not downloadResponse.code.ready():
                        validPeers.remove(id)
                        continue
                    size = int(downloadResponse.content.split("bytes ")[1])
                    self.download = PartialDownload(filename=userCommand.filename, size=size)
                    noOfChunks = ceil(float(size) / 100.0)
                    if clientSocket is not None:
                        clientSocket.close()
                log(f"Confirmed valid peers: {validPeers}")

            workers: list[ClientWorker] = []

            workerConfigs = [
                {
                    "serverId": id,
                    "server": self.servers[id],
                    "userCommand": userCommand,
                    "download": self.download,
                    "downloadChunkIds": [] if userCommand.type == CommandType.DOWNLOAD else None
                } for id in validPeers
            ]
            if userCommand.type == CommandType.DOWNLOAD:
                for chunk in range(noOfChunks):
                    config = workerConfigs[chunk % len(workerConfigs)]
                    assert config["downloadChunkIds"] is not None
                    config["downloadChunkIds"].append(chunk)
            for config in workerConfigs:
                try:
                    worker = ClientWorker(
                        kwargs=config)
                    workers.append(worker)
                    worker.start()
                except (SocketError, ConnectionRefusedError, TimeoutError):
                    continue
            for worker in workers:
                worker.join()
            self.download = None


if __name__ == "__main__":
    try:
        Client().run()
    except KeyboardInterrupt:
        exit()
