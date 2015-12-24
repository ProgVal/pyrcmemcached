#!/usr/bin/env python3

import time
import socket
import random
import collections

class Disconnected(Exception):
    pass

def randomnick():
    chars = 'abcdefghijklmnopqrstuvwxyz'
    return ''.join(random.choice(chars) for x in range(9))

def isvalidkey(key):
    chars = frozenset('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.:')
    return all(map(chars.__contains__, key))


Message = collections.namedtuple('Message',
        'tags prefix command params')

SERVER_MAX_KEY_LENGTH = 50 # TODO
SERVER_MAX_VALUE_LENGTH = 500 # TODO

def parse_message(s):
    """Parse a message according to
    http://tools.ietf.org/html/rfc1459#section-2.3.1
    and
    http://ircv3.net/specs/core/message-tags-3.2.html"""
    assert s.endswith('\r\n'), 'Message does not end with CR LF'
    s = s[0:-2]
    if s.startswith('@'):
        (tags, s) = s.split(' ', 1)
    if ' :' in s:
        (other_tokens, trailing_param) = s.split(' :')
        tokens = list(filter(bool, other_tokens.split(' '))) + [trailing_param]
    else:
        tokens = list(filter(bool, s.split(' ')))
    if tokens[0].startswith(':'):
        prefix = tokens.pop(0)[1:]
    else:
        prefix = None
    command = tokens.pop(0)
    params = tokens
    return Message(
            tags=[],
            prefix=prefix,
            command=command,
            params=params,
            )

class IrcClient:
    def __init__(self, name, show_io=False):
        self.name = name
        self.show_io = show_io
        self.inbuffer = []
    def connect(self, hostname, port):
        self.conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.conn.connect((hostname, port))
        self.nick = randomnick()
        if self.show_io:
            print('{:.3f} {}: connects to server.'.format(time.time(), self.name))
    def disconnect(self):
        if not self.conn:
            return
        if self.show_io:
            print('{:.3f} {}: disconnects from server.'.format(time.time(), self.name))
        self.conn.close()
        self.conn = None
    def getMessages(self, assert_get_one=False):
        data = b''
        messages = []
        conn = self.conn
        while True:
            try:
                new_data = conn.recv(4096)
            except socket.timeout:
                if not assert_get_one and data == b'':
                    # Received nothing
                    return []
                if self.show_io:
                    print('{:.3f} waiting…'.format(time.time()))
                time.sleep(0.1)
                break
            else:
                if not new_data:
                    # Connection closed
                    raise ConnectionClosed()
            data += new_data
            if new_data.endswith(b'\r\n'):
                break
            else:
                time.sleep(0.1)
                continue
        for line in data.decode().split('\r\n'):
            if line:
                if self.show_io:
                    print('{:.3f} S -> {}: {}'.format(time.time(), self.name, line.strip()))
                message = parse_message(line + '\r\n')
                messages.append(message)
        return messages
    def getMessage(self, filter_pred=None, synchronize=True):
        while True:
            if not self.inbuffer:
                self.inbuffer = self.getMessages(assert_get_one=True)
            if not self.inbuffer:
                raise NoMessageException()
            message = self.inbuffer.pop(0) # TODO: use dequeue
            if not filter_pred or filter_pred(message):
                return message
    def sendLine(self, line):
        if not self.conn:
            raise Disconnected()
        ret = self.conn.sendall(line.encode())
        assert ret is None
        if not line.endswith('\r\n'):
            ret = self.conn.sendall(b'\r\n')
            assert ret is None
        if self.show_io:
            print('{:.3f} {} -> S: {}'.format(time.time(), self.name, line.strip()))

    def authenticate(self):
        self.sendLine('CAP LS 302')
        self.sendLine('USER pyrcmemcached * * :pyrcmemcached')
        self.sendLine('NICK {}'.format(self.nick))

        capabilities = []
        while True:
            m = self.getMessage()
            if m.command != 'CAP' or m.params[1] != 'LS':
                continue
            if m.params[2] == '*':
                capabilities.extend(m.params[3].split(' '))
            else:
                capabilities.extend(m.params[2].split(' '))
                break
        self.sendLine('CAP END')
        while m.command != '005': # RPL_ISUPPORT
            m = self.getMessage()
        if 'METADATA' not in {x.split('=')[0] for x in m.params[1:-1]}:
            raise Exception('Server does not support METADATA.')

        self.sendLine('PING')
        while m.command != 'PONG':
            m = self.getMessage()

    def join(self, channel):
        self.sendLine('JOIN {}'.format(channel))
        m = self.getMessage()
        while m.command == 'NOTICE':
            m = self.getMessage()
        assert m.command != '366', m
        self.getMessages()


class Client:
    channel = '#foo'
    class MemcachedKeyCharacterError(Exception):
        pass

    def __init__(self, servers, debug=0):
        if not servers:
            raise ValueError('No server.')
        self.servers = servers
        self.irc = IrcClient('irc', show_io=False)
        self._connect()

    def _connect(self):
        (hostname, port) = self.servers[0].rsplit(':', 1)
        self.irc.connect(hostname, int(port))
        self.irc.authenticate()
        self.irc.join(self.channel)

    def disconnect_all(self):
        self.irc.disconnect()

    def mark_dead(self, reason):
        self.irc.disconnect()

    def set(self, key, value, noreply=False):
        if not isvalidkey(key):
            raise self.MemcachedKeyCharacterError(key)
        if isinstance(value, bool):
            type_ = 'bool'
        elif isinstance(value, int):
            type_ = 'int'
        else:
            type_ = 'str'
        self.irc.sendLine('METADATA {chan} SET {key} :{type}:{value}'.format(
            chan=self.channel,
            key=key,
            type=type_,
            value=value,
            ))
        m = self.irc.getMessage()
        assert m.command == '761', m # RPL_KEYVALUE
        m = self.irc.getMessage()
        assert m.command == '762', m # RPL_METADATAEND

    def get(self, key):
        if not isvalidkey(key):
            raise MemcachedKeyCharacterError(key)
        self.irc.sendLine('METADATA {chan} GET {key}'.format(
            chan=self.channel,
            key=key,
            ))
        m = self.irc.getMessage()
        if m.command == '766': # ERR_NOMATCHINGKEY
            return None
        assert m.command == '761', m # RPL_KEYVALUE
        assert m.params[1] == key, m
        (type_, value) = m.params[3].split(':', 1)
        if type_ == 'bool':
            value = bool(value)
        elif type_ == 'int':
            value = int(value)
        return value

    def delete(self, key):
        self.delete_multi([key])
    def delete_multi(self, keys):
        for key in keys:
            self.irc.sendLine('METADATA {chan} SET {key}'.format(
                chan=self.channel,
                key=key,
                ))
        for key in keys:
            m = self.irc.getMessage()
            assert m.command == '761', m # RPL_KEYVALUE
            m = self.irc.getMessage()
            assert m.command == '762', m # RPL_METADATAEND

if __name__ == '__main__':
    c = Client(['localhost:6667'])
    c.set('foo', 'bar')
    assert c.get('foo') == 'bar'
    c.delete('foo')
    assert c.get('foo') is None
    print('All tests passed.')
