"""
This module implements a client for the BloomD server.
"""
__all__ = ["BloomdError", "BloomdConnection", "BloomdClient", "BloomdFilter"]
__version__ = "0.4.1"
import os
import logging
import socket
import errno
import time
import hashlib


class BloomdError(Exception):
    "Root of exceptions from the client library"
    pass


class BloomdConnection(object):
    "Provides a convenient interface to server connections"
    def __init__(self, server, timeout, attempts=3, pool=None):
        """
        Creates a new Bloomd Connection.

        :Parameters:
            - server: Provided as a string, either as "host" or "host:port" or "host:port:udpport".
                      Uses the default port of 8673 if none is provided for tcp, and 8674 for udp.
            - timeout: The socket timeout to use.
            - attempts (optional): Maximum retry attempts on errors. Defaults to 3.
        """
        self.pid = os.getpid()

        # Parse the host/port
        parts = server.split(":", 1)
        if len(parts) == 2:
            host, port = parts[0], int(parts[1])
        else:
            host, port = parts[0], 8673

        self.server = (host, port)
        self.timeout = timeout
        self.sock = None
        self.fh = None
        self.attempts = attempts
        self.pool = pool
        self.logger = logging.getLogger("pybloomd.BloomdConnection.%s.%d" % self.server)

    def _create_socket(self):
        "Creates a new socket, tries to connect to the server"
        # Connect the socket
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(self.timeout)
        s.connect(self.server)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        self.fh = None
        return s

    def send(self, cmd):
        "Sends a command with out the newline to the server"
        if self.sock is None:
            self.sock = self._create_socket()
        sent = False
        for attempt in xrange(self.attempts):
            try:
                self.sock.sendall(cmd + "\n")
                sent = True
                break
            except socket.error, e:
                self.logger.exception("Failed to send command to bloomd server! Attempt: %d" % attempt)
                if e[0] in (errno.ECONNRESET, errno.ECONNREFUSED, errno.EAGAIN, errno.EHOSTUNREACH, errno.EPIPE):
                    self.sock = self._create_socket()
                else:
                    raise

        if not sent:
            self.logger.critical("Failed to send command to bloomd server after %d attempts!" % self.attempts)
            raise EnvironmentError("Cannot contact bloomd server!")

    def read(self):
        "Returns a single line from the file"
        if self.sock is None:
            self.sock = self._create_socket()
        if not self.fh:
            self.fh = self.sock.makefile()
        read = self.fh.readline().rstrip("\r\n")
        return read

    def readblock(self, start="START", end="END"):
        """
        Reads a response block from the server. The servers
        responses are between `start` and `end` which can be
        optionally provided. Returns an array of the lines within
        the block.
        """
        lines = []
        first = self.read()
        if first != start:
            raise BloomdError("Did not get block start (%s)! Got '%s'!" % (start, first))
        while True:
            line = self.read()
            if line == end:
                break
            lines.append(line)
        return lines

    def send_and_receive(self, cmd):
        """
        Convenience wrapper around `send` and `read`. Sends a command,
        and reads the response, performing a retry if necessary.
        """
        done = False
        for attempt in xrange(self.attempts):
            try:
                self.send(cmd)
                return self.read()
            except socket.error, e:
                self.logger.exception("Failed to send command to bloomd server! Attempt: %d" % attempt)
                if e[0] in (errno.ECONNRESET, errno.ECONNREFUSED, errno.EAGAIN, errno.EHOSTUNREACH, errno.EPIPE):
                    self.sock = self._create_socket()
                else:
                    raise

        if not done:
            self.logger.critical("Failed to send command to bloomd server after %d attempts!" % self.attempts)
            raise EnvironmentError("Cannot contact bloomd server!")

    def response_block_to_dict(self):
        """
        Convenience wrapper around `readblock` to convert a block
        output into a dictionary by splitting on spaces, and using the
        first column as the key, and the remainder as the value.
        """
        resp_lines = self.readblock()
        return dict(tuple(l.split(" ", 1)) for l in resp_lines)

    def disconnect(self):
        "Disconnects from the Bloomd server"
        if self.sock is None:
            return
        try:
            self.sock.close()
        except socket.error:
            pass
        self.sock = None

    def release(self):
        if self.pool is None:
            return
        self.pool.release(self)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()


class ConnectionPool(object):
    "Generic connection pool"
    def __init__(self, connection_class=BloomdConnection, max_connections=None,
                 **connection_kwargs):
        self.pid = os.getpid()
        self.connection_class = connection_class
        self.connection_kwargs = connection_kwargs
        self.max_connections = max_connections or 2 ** 31
        self._created_connections = 0
        self._available_connections = []
        self._in_use_connections = set()

    def _checkpid(self):
        if self.pid != os.getpid():
            self.disconnect()
            self.__init__(self.connection_class, self.max_connections,
                          **self.connection_kwargs)

    def get_connection(self):
        "Get a connection from the pool"
        self._checkpid()
        try:
            connection = self._available_connections.pop()
        except IndexError:
            connection = self.make_connection()
        self._in_use_connections.add(connection)
        return connection

    def make_connection(self):
        "Create a new connection"
        if self._created_connections >= self.max_connections:
            raise ConnectionError("Too many connections")
        self._created_connections += 1
        return self.connection_class(pool=self, **self.connection_kwargs)

    def release(self, connection):
        "Releases the connection back to the pool"
        self._checkpid()
        if connection.pid == self.pid:
            self._in_use_connections.remove(connection)
            self._available_connections.append(connection)

    def disconnect(self):
        "Disconnects all connections in the pool"
        all_conns = chain(self._available_connections,
                          self._in_use_connections)
        for connection in all_conns:
            connection.disconnect()


class BloomdClient(object):
    "Provides a client abstraction around the BloomD interface."
    def __init__(self, servers, timeout=None, hash_keys=False):
        """
        Creates a new BloomD client.

        :Parameters:
            - servers : A list of servers, which are provided as strings in the "host" or "host:port".
            - timeout: (Optional) A socket timeout to use, defaults to no timeout.
            - hash_keys: (Optional) Should keys be hashed before sending to bloomd. Defaults to False.
        """
        if len(servers) == 0:
            raise ValueError("Must provide at least 1 server!")
        self.servers = servers
        self.timeout = timeout
        self.sever_pools = {}
        self.server_info = None
        self.info_time = 0
        self.hash_keys = hash_keys

    def _server_pool(self, server):
        "Returns a connection to a server, tries to cache connections."
        if server in self.sever_pools:
            return self.sever_pools[server]
        else:
            pool = ConnectionPool(server=server, timeout=self.timeout)
            self.sever_pools[server] = pool
            return pool

    def _get_pool(self, filter, strict=True, explicit_server=None):
        """
        Gets a connection to a server which is able to service a filter.
        Because filters may exist on any server, all servers are queried
        for their filters and the results are cached. This allows us to
        partition data across multiple servers.

        :Parameters:
            - filter : The filter to connect to
            - strict (optional) : If True, an error is raised when a filter
                does not exist.
            - explicit_server (optional) : If provided, when a filter does
                not exist and strict is False, a connection to this server is made.
                Otherwise, the server with the fewest sets is returned.
        """
        # Force checking if we have no info or 5 minutes has elapsed
        if not self.server_info or time.time() - self.info_time > 300:
            self.server_info = self.list_filters(inc_server=True)
            self.info_time = time.time()

        # Check if this filter is in a known location
        if filter in self.server_info:
            serv = self.server_info[filter][0]
            return self._server_pool(serv)

        # Possibly old data? Reload
        self.server_info = self.list_filters(inc_server=True)
        self.info_time = time.time()

        # Recheck
        if filter in self.server_info:
            serv = self.server_info[filter][0]
            return self._server_pool(serv)

        # Check if this is fatal
        if strict:
            raise BloomdError("Filter does not exist!")

        # We have an explicit server provided to us, use that
        if explicit_server:
            return self._server_pool(explicit_server)

        # Does not exist, and is not not strict
        # we can select a server on any criteria then.
        # We will use the server with the minimal set count.
        counts = {}
        for server in self.servers:
            counts[server] = 0
        for filter, (server, info) in self.server_info.items():
            counts[server] += 1

        counts = [(count, srv) for srv, count in counts.items()]
        counts.sort()

        # Select the least used
        serv = counts[0][1]
        return self._server_pool(serv)

    def create_filter(self, name, capacity=None, prob=None, in_memory=False, server=None):
        """
        Creates a new filter on the BloomD server and returns a BloomdFilter
        to interface with it. This will return a BloomdFilter object attached
        to the filter if the filter already exists.

        :Parameters:
            - name : The name of the new filter
            - capacity (optional) : The initial capacity of the filter
            - prob (optional) : The inital probability of false positives.
            - in_memory (optional) : If True, specified that the filter should be created
              in memory only.
            - server (optional) : In a multi-server environment, this forces the
                    filter to be created on a specific server. Should be provided
                    in the same format as initialization "host" or "host:port".
        """
        if prob and not capacity:
            raise ValueError("Must provide size with probability!")

        pool = self._get_pool(name, strict=False, explicit_server=server)

        with pool.get_connection() as conn:
            cmd = "create %s" % name
            if capacity:
                cmd += " capacity=%d" % capacity
            if prob:
                cmd += " prob=%f" % prob
            if in_memory:
                cmd += " in_memory=1"

            conn.send(cmd)
            resp = conn.read()

        if resp == "Done":
            return BloomdFilter(pool, name, self.hash_keys)
        elif resp == "Exists":
            return self[name]
        else:
            raise BloomdError("Got response: %s" % resp)

    def __getitem__(self, name):
        "Gets a BloomdFilter object based on the name."
        pool = self._get_pool(name)
        return BloomdFilter(pool, name, self.hash_keys)

    def list_filters(self, inc_server=False):
        """
        Lists all the available filters across all servers.
        Returns a dictionary of {filter_name : filter_info}.

        :Parameters:
            - inc_server (optional) : If true, the dictionary values
               will be (server, filter_info) instead of filter_info.
        """
        connections = []
        for server in self.servers:
            pool = self._server_pool(server)
            connections.append(pool.get_connection())

        # Send the list to all first
        for conn in connections:
            conn.send("list")

        # Check response from all
        responses = {}
        for conn in connections:
            resp = conn.readblock()
            conn.release()
            for line in resp:
                name, info = line.split(" ", 1)
                if inc_server:
                    responses[name] = server, info
                else:
                    responses[name] = info

        return responses

    def flush(self):
        "Instructs all servers to flush to disk"
        connections = []
        for server in self.servers:
            pool = self._server_pool(server)
            connections.append(pool.get_connection())

        # Send the flush to all first
        for conn in connections:
            conn.send("flush")

        # Check response from all
        error = None
        for conn in connections:
            resp = conn.read()
            conn.release()

            if not error and resp != "Done":
                args = (resp,) + conn.server
                msg = "Got response: '%s' from '%s:%s'" % args
                error = error or BloomdError(msg)

        if error:
            raise error


class BloomdFilter(object):
    "Provides an interface to a single Bloomd filter"
    def __init__(self, pool, name, hash_keys=False):
        """
        Creates a new BloomdFilter object.

        :Parameters:
            - pool : The connection pool to use
            - name : The name of the filter
            - hash_keys : Should the keys be hashed client side
        """
        self.pool = pool
        self.name = name
        self.hash_keys = hash_keys

    def _get_key(self, key):
        """
        Returns the key we should send to the server
        """
        if self.hash_keys:
            return hashlib.sha1(key).hexdigest()
        return key

    def add(self, key):
        """
        Adds a new key to the filter. Returns True/False if the key was added.
        """
        with self.pool.get_connection() as conn:
            resp = conn.send_and_receive("s %s %s" % (self.name, self._get_key(key)))

        if resp in ("Yes", "No"):
            return resp == "Yes"
        raise BloomdError("Got response: %s" % resp)

    def bulk(self, keys):
        "Performs a bulk set command, adds multiple keys in the filter"
        command = ("b %s " % self.name) + " ".join([self._get_key(k) for k in keys])

        with self.pool.get_connection() as conn:
            resp = conn.send_and_receive(command)

        if resp.startswith("Yes") or resp.startswith("No"):
            return [r == "Yes" for r in resp.split(" ")]
        raise BloomdError("Got response: %s" % resp)

    def drop(self):
        "Deletes the filter from the server. This is permanent"
        with self.pool.get_connection() as conn:
            resp = conn.send_and_receive("drop %s" % (self.name))
        if resp != "Done":
            raise BloomdError("Got response: %s" % resp)

    def close(self):
        """
        Closes the filter on the server.
        """
        with self.pool.get_connection() as conn:
            resp = conn.send_and_receive("close %s" % (self.name))
        if resp != "Done":
            raise BloomdError("Got response: %s" % resp)

    def clear(self):
        """
        Clears the filter on the server.
        """
        with self.pool.get_connection() as conn:
            resp = conn.send_and_receive("clear %s" % (self.name))
        if resp != "Done":
            raise BloomdError("Got response: %s" % resp)

    def check(self, key):
        "Checks if the key is contained in the filter."
        return key in self

    def __contains__(self, key):
        "Checks if the key is contained in the filter."
        with self.pool.get_connection() as conn:
            resp = conn.send_and_receive("c %s %s" % (self.name, self._get_key(key)))
        if resp in ("Yes", "No"):
            return resp == "Yes"
        raise BloomdError("Got response: %s" % resp)

    def multi(self, keys):
        "Performs a multi command, checks for multiple keys in the filter"
        command = ("m %s " % self.name) + " ".join([self._get_key(k) for k in keys])

        with self.pool.get_connection() as conn:
            resp = conn.send_and_receive(command)

        if resp.startswith("Yes") or resp.startswith("No"):
            return [r == "Yes" for r in resp.split(" ")]
        raise BloomdError("Got response: %s" % resp)

    def __len__(self):
        "Returns the count of items in the filter."
        info = self.info()
        return int(info["size"])

    def info(self):
        "Returns the info dictionary about the filter."
        with self.pool.get_connection() as conn:
            conn.send("info %s" % (self.name))
            return self.conn.response_block_to_dict()

    def flush(self):
        "Forces the filter to flush to disk"
        with self.pool.get_connection() as conn:
            resp = conn.send_and_receive("flush %s" % (self.name))
        if resp != "Done":
            raise BloomdError("Got response: %s" % resp)

    def pipeline(self):
        "Creates a BloomdPipeline for pipelining multiple queries"
        return BloomdPipeline(self.pool, self.name, self.hash_keys)


class BloomdPipeline(object):
    "Provides an interface to a single Bloomd filter"
    def __init__(self, pool, name, hash_keys=False):
        """
        Creates a new BloomdPipeline object.

        :Parameters:
            - pool : The connection pool to use
            - name : The name of the filter
            - hash_keys : Should the keys be hashed client side
        """
        self.pool = pool
        self.name = name
        self.hash_keys = hash_keys
        self.buf = []

    def _get_key(self, key):
        """
        Returns the key we should send to the server
        """
        if self.hash_keys:
            return hashlib.sha1(key).hexdigest()
        return key

    def add(self, key):
        """
        Adds a new key to the filter. Returns True/False if the key was added.
        """
        self.buf.append(("add", "s %s %s" % (self.name, self._get_key(key))))
        return self

    def bulk(self, keys):
        "Performs a bulk set command, adds multiple keys in the filter"
        command = ("b %s " % self.name) + " ".join([self._get_key(k) for k in keys])
        self.buf.append(("bulk", command))
        return self

    def drop(self):
        "Deletes the filter from the server. This is permanent"
        self.buf.append(("drop", "drop %s" % (self.name)))
        return self

    def close(self):
        """
        Closes the filter on the server.
        """
        self.buf.append(("close", "close %s" % (self.name)))
        return self

    def clear(self):
        """
        Clears the filter on the server.
        """
        self.buf.append(("clear", "clear %s" % (self.name)))
        return self

    def check(self, key):
        "Checks if the key is contained in the filter."
        self.buf.append(("check", "c %s %s" % (self.name, self._get_key(key))))
        return self

    def multi(self, keys):
        "Performs a multi command, checks for multiple keys in the filter"
        command = ("m %s " % self.name) + " ".join([self._get_key(k) for k in keys])
        self.buf.append(("multi", command))
        return self

    def info(self):
        "Returns the info dictionary about the filter."
        self.buf.append(("info", "info %s" % (self.name)))
        return self

    def flush(self):
        "Forces the filter to flush to disk"
        self.buf.append(("flush", "flush %s" % (self.name)))
        return self

    def merge(self, pipeline):
        """
        Merges this pipeline with another pipeline. Commands from the
        other pipeline are appended to the commands of this pipeline.
        """
        self.buf.extend(pipeline.buf)
        return self

    def execute(self):
        """
        Executes the pipelined commands. All commands are sent to
        the server in the order issued, and responses are returned
        in appropriate order.
        """
        with self.pool.get_connection() as conn:
            # Send each command
            buf = self.buf
            self.buf = []
            for name, cmd in buf:
                conn.send(cmd)

            # Get the responses
            all_resp = []
            for name, cmd in buf:
                if name in ("bulk", "multi"):
                    resp = conn.read()
                    if resp.startswith("Yes") or resp.startswith("No"):
                        all_resp.append([r == "Yes" for r in resp.split(" ")])
                    else:
                        all_resp.append(BloomdError("Got response: %s" % resp))

                elif name in ("add", "check"):
                    resp = conn.read()
                    if resp in ("Yes", "No"):
                        all_resp.append(resp == "Yes")
                    else:
                        all_resp.append(BloomdError("Got response: %s" % resp))


                elif name in ("drop", "close", "clear", "flush"):
                    resp = conn.read()
                    if resp == "Done":
                        all_resp.append(True)
                    else:
                        all_resp.append(BloomdError("Got response: %s" % resp))

                elif name == "info":
                    try:
                        resp = conn.response_block_to_dict()
                        all_resp.append(resp)
                    except BloomdError, e:
                        all_resp.append(e)
                else:
                    raise Exception("Unknown command! Command: %s" % name)

            return all_resp

