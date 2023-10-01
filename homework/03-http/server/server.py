import logging
import pathlib
from dataclasses import dataclass
from socketserver import StreamRequestHandler
import typing as t
import click
import socket
import os
from email.parser import Parser
import gzip
import subprocess
import shutil

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


@dataclass
class HTTPServer:
    server_address: t.Tuple[str, int]
    socket: socket.socket
    server_domain: str
    working_directory: pathlib.Path


class HTTPHandler(StreamRequestHandler):
    server: HTTPServer

    # Use self.rfile and self.wfile to interact with the client
    # Access domain and working directory with self.server.{attr}
    def handle(self) -> None:
        first_line = self.rfile.readline()
        logger.info(f"Handle connection from {self.client_address}, first_line {first_line}")

        # TODO: Write your code
        headers = []
        while True:
            line = self.rfile.readline()
            if line in (b'\r\n', b'\n', b''):
                break
            headers.append(line)

        sheaders = b''.join(headers).decode('utf-8')
        all_headers = Parser().parsestr(sheaders)
        host = all_headers.get('Host')
        accept_encoding = all_headers.get('Accept-Encoding')
        create_directory = all_headers.get('Create-Directory')
        remove_directory = all_headers.get('Remove-Directory')
        content_length = all_headers.get('Content-Length')

        body = b''
        response = b'HTTP/1.1 400 Bad Request\n\n'
        if content_length != None:
            body = self.rfile.read(int(content_length))

        if self.server.server_domain and self.server.server_domain != host:
            response = b'HTTP/1.1 400 Bad Request\n'
            response += b'Server: example\n\n'
        else:
            req_line = str(first_line, 'utf-8')
            req_line = req_line.rstrip('\r\n')
            words = req_line.split()           
            # response = b''
            if len(words) == 3:
                command, filename, version = words[0], words[1], words[2]
                if version != 'HTTP/1.1':
                    response = b'HTTP/1.1 505 HTTP Version not supported\n'
                    response += b'Server: example\n\n'
                else:
                    if command == 'GET':
                        path = str(self.server.working_directory) + filename
                        path = pathlib.Path(path)
                        if os.path.exists(path) and os.path.isdir(path):
                            proc = subprocess.Popen(['ls', '-lA', '--time-style=+%Y-%m-%d %H:%M:%S', path], stdout=subprocess.PIPE)
                            with open("data.txt","w") as f:
                                i = 0
                                for line in proc.stdout.readlines():
                                    if i == 0:
                                        i = 1
                                    else:
                                        f.write(str(line))
                            with open('data.txt', 'rb') as f:
                                file_data = f.read()
                            
                            subprocess.run(['rm', 'data.txt'])
                            response = b'HTTP/1.1 200 OK\n'
                            length = len(file_data)
                            response += bytes("Content-Length: {}\n".format(length), 'utf-8')
                            response += b'Content-Type: text/plain\n'
                            response += b'Server: example\n\n'
                            response += file_data
                            f.close()
                        elif os.path.exists(path) and not os.path.isdir(path):
                            try:
                                file_data = b''
                                with open(path, 'rb') as f:
                                    file_data = f.read()
                                f.close()

                                response = b"HTTP/1.1 200 OK\n"
                                if accept_encoding == 'gzip':
                                    compressed_data = b''
                                    with open(path, 'rb') as file:
                                        length = 0
                                        while length != os.path.getsize(path):
                                            cur_data = file.read(8000000)
                                            compressed_chunk = gzip.compress(cur_data)
                                            compressed_data += compressed_chunk
                                            length += len(cur_data)
                                    length = len(compressed_data)
                                    response += bytes("Content-Length: {}\n".format(length), 'utf-8')
                                    response += b'Content-Type: application/octet-stream\n'
                                    response += b'Content-Encoding: gzip\n'
                                    response += b'Server: example\n\n'
                                    response += compressed_data

                                    file.close()
                                elif accept_encoding != None and accept_encoding != 'gzip':
                                    response = b'HTTP/1.1 400 Bad Request\n'
                                    response += b'Server: example\n\n'
                                else:
                                    
                                    length = len(file_data)
                                    response += bytes("Content-Length: {}\n".format(length), 'utf-8')
                                    response += b'Content-Type: application/octet-stream\n'
                                    response += b'Server: example\n\n'
                                    response += file_data
                            except Exception as e:
                                logger.error(e)
                                response = b'HTTP/1.1 400 Bad Request\n'
                                response += b'Server: example\n\n'
                        else:
                            response = b'HTTP/1.1 404 Not Found\n'
                            response += b'Content-Type: application/octet-stream\n'
                            response += b'Server: example\n\n'
                            response += b'File not found.\n'
                    elif command == 'POST':
                        # response = b''
                        path = str(self.server.working_directory) + filename
                        path = pathlib.Path(path)
                        if create_directory:
                            if not os.path.exists(path):
                                logger.info(1)
                                os.makedirs(path)
                                response = b"HTTP/1.1 200 OK\n"
                                response += b'Content-Type: text/plain\n'
                                response += b'Server: example\n\n'
                                response += b'Directory was created\n'
                            else:
                                logger.info(2)
                                response = b'HTTP/1.1 409 Conflict\n'
                                response += b'Content-Type: text/plain\n'
                                response += b'Server: example\n\n'
                                response += b'Directory already exists.\n'
                        else:
                            if os.path.isdir(path):
                                logger.info(3)
                                response = b'HTTP/1.1 409 Conflict\n'
                                response += b'Content-Type: text/plain\n'
                                response += b'Server: example\n\n'
                                response += b'Directory already exists.\n'
                            elif not os.path.exists(path):
                                logger.info(4)
                                try:
                                    cur_path = str(self.server.working_directory) + filename
                                    parsed_path = cur_path.rsplit('/', 1)[0]
                                    subprocess.run(['mkdir', '-p', parsed_path])
                                    subprocess.run(['touch', path])
                                    
                                    with open(path, 'wb') as file:
                                        file.write(body)
                                        
                                    logger.info(44)
                                    
                                    length = len(body)
                                    response = b"HTTP/1.1 200 OK\n"
                                    response += bytes("Content-Length: {}\n".format(length), 'utf-8')
                                    response += b'Content-Type: application/octet-stream\n'
                                    response += b'Server: example\n\n'
                                    response += body

                                    file.close()
                                except Exception as e:
                                    logger.info(6)
                                    logger.error(e)
                                    response = b'HTTP/1.1 400 Bad Request\n'
                                    response += b'Content-Type: application/octet-stream\n'
                                    response += b'Server: example\n\n'
                            else:
                                logger.info(5)
                                response = b'HTTP/1.1 409 Conflict\n'
                                response += b'Content-Type: application/octet-stream\n'
                                response += b'Server: example\n\n'
                                response += b'File exists.\n'
                    elif command == 'PUT':
                        path = str(self.server.working_directory) + filename
                        path = pathlib.Path(path)
                        # response = b''
                        if os.path.isdir(path):
                            response = b'HTTP/1.1 409 Conflict\n'
                            response += b'Content-Type: text/plain\n'
                            response += b'Server: example\n\n'
                            response += b'It is a directory.\n'
                        elif os.path.exists(path) and not os.path.isdir(path):
                            try:
                                data = b''
                                data = body
                                if len(data) != int(content_length):
                                    response = b"HTTP/1.1 400 Bad Request\n"
                                    response += b'Server: example\n\n'
                                else:
                                    try:
                                        with open(path, 'wb') as file:
                                            file.write(data)
                                        
                                        length = len(data)
                                        response = b"HTTP/1.1 200 OK\n"
                                        response += bytes("Content-Length: {}\n".format(length), 'utf-8')
                                        response += b'Content-Type: application/octet-stream\n'
                                        response += b'Server: example\n\n'
                                        response += data

                                        file.close()
                                    except Exception as e:
                                        logger.error(e)
                                        response = b'HTTP/1.1 400 Bad Request\n'
                                        response += b'Server: example\n\n'
                            except Exception as e:
                                logger.error(e)
                                response = b'HTTP/1.1 400 Bad Request\n'
                                response += b'Server: example\n\n'
                        elif not os.path.exists(path) and not os.path.isdir(path):
                            response = b'HTTP/1.1 404 Not Found\n'
                            response += b'Server: example\n\n'
                            response += b'File not found.\n'
                    elif command == 'DELETE':
                        path = str(self.server.working_directory) + filename
                        path = pathlib.Path(path)
                        # response = b''
                        if os.path.isdir(path) and not remove_directory:
                            response = b'HTTP/1.1 406 Not Acceptable\n'
                            response += b'Content-Type: text/plain\n'
                            response += b'Server: example\n\n'
                        elif os.path.isdir(path) and remove_directory:
                            try:
                                shutil.rmtree(path)
                                response = b'HTTP/1.1 200 OK\n'
                                response += b'Content-Type: text/plain\n'
                                response += b'Server: example\n\n'

                            except Exception as e:
                                response = b'HTTP/1.1 400 Bad Request\n'
                                response += b'Server: example\n\n'
                        elif os.path.exists(path):
                            try:
                                os.remove(path)
                                response = b'HTTP/1.1 200 OK\n'
                                response += b'Content-Type: application/octet-stream\n'
                                response += b'Server: example\n\n'
                            except Exception as e:
                                logger.error(e)
                                response = b'HTTP/1.1 400 Bad Request\n'
                                response += b'Server: example\n\n'
                        else:
                            response = b'HTTP/1.1 404 Not Found\n'
                            response += b'Server: example\n\n'
                            response += b'File not found.\n'
            else:
                response = b'HTTP/1.1 400 Bad Request\n'
                response += b'Server: example\n\n'
        try:
            self.wfile.write(response)
            # self.request.send(response)
        except Exception as e:
            logger.error(e)


@click.command()
@click.option("--host", type=str)
@click.option("--port", type=int)
@click.option("--server-domain", type=str)
@click.option("--working-directory", type=str)
def main(host, port, server_domain, working_directory):
    # TODO: Write your code

    if not working_directory:
        exit(1)
    if not host:
        if not os.environ.get("SERVER_HOST"):
            host = "0.0.0.0"
        else:
            host = os.environ["SERVER_HOST"]
    if not port:
        if not os.environ.get("SERVER_PORT"):
            port = 8080
        else:
            port = os.environ["SERVER_PORT"]
    if not server_domain:
        if not os.environ.get("SERVER_DOMAIN"):
            server_domain = None
        else:
            server_domain = os.environ["SERVER_DOMAIN"]
    
    working_directory_path = pathlib.Path(working_directory)

    logger.info(
        f"Starting server on {host}:{port}, domain {server_domain}, working directory {working_directory}"
    )

    # Create a server socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Set SO_REUSEADDR option
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    # Bind the socket object to the address and port
    s.bind((host, port))
    # Start listening for incoming connections
    s.listen()

    logger.info(f"Listening at {s.getsockname()}")
    server = HTTPServer((host, port), s, server_domain, working_directory_path)
    
    while True:
        # Accept any new connection (request, client_address)
        logger.info(f"Listening at {s.getsockname()}")
        
        try:
            conn, addr = s.accept()
        except OSError:
            break

        try:
            # Handle the request
            HTTPHandler(conn, addr, server)

            # Close the connection
            conn.shutdown(socket.SHUT_WR)
            conn.close()
        except Exception as e:
            logger.error(e)
            conn.close()


if __name__ == "__main__":
    main(auto_envvar_prefix="SERVER")
