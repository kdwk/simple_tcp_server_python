from socket import *
from threading import Thread, RLock
import sys
from os import listdir
from os.path import exists, isfile
from enum import Enum
from pathlib import Path
from math import ceil
from urllib.parse import quote, unquote
from time import sleep, time_ns

debug = False

uploadsLock = RLock()

def error(message: str) -> None:
    global debug
    if debug:
        print("\033[31m" + message + "\033[0m", end="" if message.endswith("\n") else "\n")
    return None

def log(message: str):
    global debug
    if debug:
        print(message, end="" if message.endswith("\n") else "\n")

def path(path: str) -> str:
    return (Path(__file__).parent / path).resolve().as_posix()

def served(filename: str) -> str:
    return path(f"served_files/{filename}")

def getsize(path: str) -> int:
    with open(path, 'r') as f:
        return len(f.read())

# https://severance.wiki/severance_procedure
def sever(filepath: str) -> list[str]:
    chunks: list[str] = []
    with open(filepath, 'r') as f:
        chunk = f.read(100)
        while chunk: #loop until the chunk is empty (the file is exhausted)
            chunks.append(chunk)
            chunk = f.read(100) #read the next chunk
    return chunks

# https://severance.wiki/reintegration
def reintegrate(packets: list[str]) -> str | None:
    return "".join(packets)

def readSettings() -> dict[str, tuple[str, int]]:
    settings = dict()
    try:
        with open(path("../peer_settings.txt")) as settingsFile:
            for line in settingsFile:
                id, addr, port = line.split()
                settings[id] = (addr, int(port))
    except FileNotFoundError:
        return error(f"Could not find {path("../peer_settings.txt")}")
    return settings


class PartialUpload:
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
        if not self.isComplete():
            return error(f"Upload for {self.filename} is not complete")
        if exists(served(self.filename)):
            return error(f"{served(self.filename)} already exists")
        log(f"Writing chunks:\n{self.chunks}")
        try:
            with open(served(self.filename), 'x') as f:
                combined = reintegrate([self.chunks[chunk] for chunk in range(self.noOfChunks())])
                noChars = f.write(combined)
                assert noChars == len(combined)
        except Exception as e:
            error(f"{e}")

class CommandType(Enum):
    FILELIST = 0
    UPLOAD = 1
    DOWNLOAD = 2
    DELETE = 3

    def toString(self) -> str:
        match self:
            case CommandType.FILELIST:
                return "#FILELIST"
            case CommandType.UPLOAD:
                return "#UPLOAD"
            case CommandType.DOWNLOAD:
                return "#DOWNLOAD"
            case CommandType.DELETE:
                return "#DELETE"
            case _:
                raise "Invalid command type"
    
    def toSafeString(self) -> str:
        return quote(self.toString())


class UserCommand:
    type: CommandType
    ids: list[str]
    filename: str | None = None  # Used in upload/ download only

    def __init__(self, type: CommandType, ids: list[str], filename: str | None = None):
        self.type = type
        self.ids = ids
        match self.type:
            case CommandType.FILELIST:
                pass
            case CommandType.UPLOAD:
                if filename == None:
                    raise "Server error: upload command does not have filename"
                self.filename = filename
            case CommandType.DOWNLOAD:
                if filename == None:
                    raise "Server error: download command does not have filename"
                self.filename = filename
            case CommandType.DELETE:
                if filename == None:
                    raise "Server error: delete command does not have filename"
                self.filename = filename
            case _:
                raise "Server error: invalid command type in UserCommand.__init__(...)"

    def fromString(command: str) -> "UserCommand | None":
        components = command.split()
        if len(components) < 1:
            return error("No command")
        match components[0]:
            case "#FILELIST":
                ids = components[1:]
                return UserCommand(CommandType.FILELIST, ids)
            case "#UPLOAD":
                filename = components[1]
                ids = components[2:]
                return UserCommand(CommandType.UPLOAD, ids, filename)
            case "#DOWNLOAD":
                filename = components[1]
                ids = components[2:]
                return UserCommand(CommandType.DOWNLOAD, ids, filename)
            case "#DELETE":
                filename = components[1]
                ids = components[2:]
                return UserCommand(CommandType.DELETE, ids, filename)
            case _:
                return error("Server error: invalid command type in UserCommand.fromString(...)")
            
    def fromSafeString(command: str) -> "UserCommand":
        return UserCommand.fromString(unquote(command))
    
    def toString(self) -> str:
        return f"UserCommand(type={self.type.toString()}, ids={self.ids}{f", {self.filename}" if self.filename is not None else ""})"

class Command:
    type: CommandType
    filename: str | None = None
    bytes: int | None = None
    chunk: int | None = None
    content: str | None = None

    def __init__(self, type: CommandType, filename: str | None = None, bytes: int | None = None, chunk: int | None = None, content: str | None = None):
        self.type = type
        self.filename = filename
        self.bytes = bytes
        self.chunk = chunk
        self.content = content

    def __eq__(self, other: "Command") -> bool:
        return self.type == other.type and self.filename == other.filename and self.bytes == other.bytes and self.chunk == other.chunk and self.content == other.content

    def toString(self) -> str:
        return f"{self.type.toString()}{f" {self.filename}" if self.filename is not None else ""}{f" bytes {self.bytes}" if self.bytes != None else ""}{f" chunk {self.chunk}" if self.chunk != None else ""}{f" {self.content}" if self.content != None else ""}"
    
    def toDisplayString(self) -> str:
        return f"{self.type.toString()}{f" {self.filename}" if self.filename is not None else ""}{f" bytes {self.bytes}" if self.bytes != None else ""}{f" chunk {self.chunk}" if self.chunk != None else ""}"
    
    def toSafeString(self) -> str:
        return quote(self.toString()) + "\n"

    def fromString(command: str) -> "Command":
        if len(command) == 0:
            return error("Empty command!")
        type = ""
        if " " in command:
            type, command = command.split(" ", 1)
        else:
            type = command
        match type:
            case "#FILELIST":
                return Command(type=CommandType.FILELIST)
            case "#UPLOAD":
                filename, command = command.split(" ", 1)
                bytesOrChunk, command = command.split(" ", 1)
                if bytesOrChunk == "bytes":
                    bytes = int(command)
                    return Command(type=CommandType.UPLOAD, filename=filename, bytes=bytes)
                elif bytesOrChunk == "chunk":
                    chunk, content = command.split(" ", 1)
                    chunk = int(chunk)
                    return Command(type=CommandType.UPLOAD, filename=filename, chunk=chunk, content=content)
                else:
                    return error("Invalid command")
            case "#DOWNLOAD":
                filenameAndOptionalChunk = command.split(" ", 1)
                if len(filenameAndOptionalChunk) == 1:
                    filename = filenameAndOptionalChunk[0]
                    return Command(type=CommandType.DOWNLOAD, filename=filename)
                else:
                    filename = filenameAndOptionalChunk[0]
                    _, chunk = filenameAndOptionalChunk[1].split(" ", 1)
                    log("Command.fromString() point 5")
                    chunk = int(chunk)
                    log("Command.fromString() point 6")
                    return Command(type=CommandType.DOWNLOAD, filename=filename, chunk=chunk)
            case "#DELETE":
                filename = command
                return Command(type=CommandType.DELETE, filename=filename)
            case _:
                return error(f"Invalid command type {type}")
            
    def fromSafeString(command: str) -> "Command":
        if command.endswith("\n"):
            command = command.splitlines()[0] # Remove newline
        return Command.fromString(unquote(command))


class ResponseCode(Enum):
    k200 = 200
    k250 = 250
    k330 = 330

    def ok(self) -> bool:
        match self:
            case ResponseCode.k200:
                return True
            case _:
                return False
    
    def err(self) -> bool:
        match self:
            case ResponseCode.k250:
                return True
            case _:
                return False

    def ready(self) -> bool:
        match self:
            case ResponseCode.k330:
                return True
            case _:
                return False


class Response:
    code: ResponseCode
    content: str

    def __init__(self, code: ResponseCode = ResponseCode(200), content: str = ""):
        self.code = code
        self.content = content

    def toString(self) -> str:
        return f"{self.code.value} {self.content}"
    
    def toDisplayString(self) -> str:
        if "#UPLOAD" in self.content and "chunk " in self.content:
            content = " ".join(self.content.split(" ", 4)[:4])
            return f"{self.code.value} {content}"
        return self.toString()
    
    def toSafeString(self) -> str:
        return quote(self.toString()) + "\n"

    def fromString(response: str) -> "Response":
        components = response.split(" ", 1)
        return Response(code=ResponseCode(int(components[0])), content=components[1])
    
    def fromSafeString(response: str) -> "Response":
        response = response.splitlines()[0] # Remove newline
        return Response.fromString(unquote(response))


class ServerWorker(Thread):
    client: tuple[socket, object]
    uploads: dict[str, PartialUpload]  # Map filename to command
    workerUploads: set[str] # Set of uploads handled by this worker
    connectionSocket: socket
    serverId: int

    def __init__(self, client: tuple[socket, object], uploads: dict[str, PartialUpload], serverId: int, group=None, target=None, name=None, args=..., kwargs=None, *, daemon=None):
        super().__init__(group, target, name, args, kwargs, daemon=daemon)
        log("New ServerWorker")
        self.client = client
        self.connectionSocket, addr = self.client
        self.connectionSocket.settimeout(0.5)
        self.uploads = uploads
        self.serverId = serverId
        self.workerUploads = set()

    def receive(self) -> str | None:
        s = ""
        start = time_ns()
        while not s.endswith("\n"):
            now = time_ns()
            if now - start > 1E9:
                return error("Timeout while receiving")
            try:
                s = self.connectionSocket.recv(1024).decode()
            except TimeoutError:
                return error("Timeout while receiving")
            sleep(0.01)
        return s
    
    def send(self, response: str):
        try:
            self.connectionSocket.send(response.encode())
        except TimeoutError:
            return error("Timeout while sending")

    def handle(self, command: Command) -> Response:
        match command.type:
            case CommandType.FILELIST:
                filelist = [filename for filename in listdir(
                    path("served_files/")) if isfile(path(f"served_files/{filename}"))]
                return Response(code=ResponseCode(200), content="Files served: " + " ".join(filelist))
            case CommandType.UPLOAD:
                if command.bytes is not None:  # Declare upload intention
                    with uploadsLock:
                        if command.filename in self.uploads.keys():
                            return Response(code=ResponseCode(250), content=f"Currently receiving file {command.filename}")
                    if exists(served(command.filename)):
                        return Response(code=ResponseCode(250), content=f"Already serving file {command.filename}")
                    # Save upload intention command
                    with uploadsLock:
                        self.uploads[command.filename] = PartialUpload(command.filename, command.bytes)
                    self.workerUploads.add(command.filename)
                    log(f"Upload intention received: {command.filename}, {command.bytes}B, {self.uploads[command.filename].noOfChunks()} chunks")
                    response = Response(code=ResponseCode(330), content=f"Ready to receive file {command.filename}")
                    return response
                elif command.chunk is not None and command.content is not None:  # Actually upload chunks
                    with uploadsLock:
                        self.uploads[command.filename].save(command.chunk, command.content)
                        log(f"Upload {"complete" if self.uploads[command.filename].isComplete() else "not complete"}")
                        if self.uploads[command.filename].isComplete():
                            log("Writing to disk...")
                            self.uploads[command.filename].writeToDisk()
                            log(f"Completely uploaded {command.filename}")
                            self.uploads.pop(command.filename)
                            self.workerUploads.remove(command.filename)
                            return Response(code=ResponseCode(200), content=f"File {command.filename} received")
                    return Response(code=ResponseCode(200), content=f"File {command.filename} chunk {command.chunk} received")
            case CommandType.DOWNLOAD:
                if command.chunk is None:  # Declare download intention
                    log("Declare download intention")
                    with uploadsLock:
                        if not exists(served(command.filename)) or command.filename in self.uploads.keys():
                            return Response(code=ResponseCode(250), content=f"Not serving file {command.filename}")
                        size = getsize(served(command.filename))
                        log(f"Received download intent: {command.filename}, {size}B")
                    return Response(code=ResponseCode(330), content=f"Ready to send file {command.filename} bytes {size}")
                else:  # Actually download chunks
                    content = sever(served(command.filename))[command.chunk]
                    log(f"Return content:\n{content}")
                    return Response(code=ResponseCode(200), content=f"File {command.filename} chunk {command.chunk} {content}")
            case CommandType.DELETE:
                if exists(served(command.filename)) and isfile(served(command.filename)):
                    Path(served(command.filename)).unlink()
                    log(f"Deleted file {command.filename}")
                    return Response(code=ResponseCode(200), content=f"Deleted file {command.filename}")
                else:
                    return Response(code=ResponseCode(250), content=f"Not serving file {command.filename}")
            case _:
                raise "Invalid command type"

    def run(self):
        try:
            try:
                while True:
                    commandStr = self.receive()
                    if commandStr is None:
                        return
                    log(f"Received commandStr: {commandStr}")
                    command: Command = Command.fromSafeString(commandStr)
                    log(f"Converted to command {command.toString()}")
                    if command is None:
                        error(f"Invalid command: {commandStr}")
                        self.send(Response(code=ResponseCode(250), content=f"Invalid command: {commandStr}").toString())
                        return
                    response = self.handle(command)
                    log(response.toString())
                    self.send(response.toSafeString())
            except ConnectionResetError:
                log(f"Connection reset by client")
                raise
        except (KeyboardInterrupt, ConnectionResetError):
            log("Keyboard interrupt or connection reset")
        finally:
            log("Close client connection")
            self.connectionSocket.close()
            with uploadsLock:
                for file in self.workerUploads:
                    if file in self.uploads:
                        self.uploads.pop(file)
            log("Exit ServerWorker")
            return


class Server:
    id: str
    addr: str
    port: int
    uploads: dict[str, PartialUpload]

    def __init__(self):
        global debug
        try:
            self.id = sys.argv[1]
        except IndexError:
            error(f"Did not receive peer id")
            raise
        if len(sys.argv) == 3 and sys.argv[2] == "debug":
            debug = True
        try:
            self.addr, self.port = readSettings()[self.id]
        except KeyError:
            error(f"Could not find setting for peer {self.id}")
            raise
        self.uploads = dict()

    def run(self):
        try:
            serverSocket = socket(AF_INET, SOCK_STREAM)
            serverSocket.bind((self.addr, self.port))
            serverSocket.listen(5)
        except OSError:
            return error(f"Could not bind to {self.addr}:{self.port}")
        log(f"Server {self.id} listening on port {self.addr}:{self.port}")
        while True:
            client = serverSocket.accept()
            ServerWorker(client, self.uploads, self.id).start()


if __name__ == "__main__":
    try:
        Server().run()
    except KeyboardInterrupt:
        exit()
