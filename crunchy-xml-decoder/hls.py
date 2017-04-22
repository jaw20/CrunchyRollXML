import sys
import errno
import m3u8
import urllib2
from Crypto.Cipher import AES
import StringIO
import socket
import os

import math

blocksize = 16384

class resumable_fetch:
    def __init__(self, uri, cur, total):
        self.uri = uri
        self.cur = cur
        self.total = total
        self.offset = 0
        self._restart()
        self.file_size = int(self.stream.info().get('Content-Length', -1))
        if self.file_size <= 0:
            print "Invalid file size"
            sys.exit()

    def _progress(self):
        sys.stdout.write('\x1b[2K\r%d/%d' % (self.cur, self.total))
        sys.stdout.flush()

    def _restart(self):
        req = urllib2.Request(self.uri)
        if self.offset:
            req.headers['Range'] = 'bytes=%s-' % (self.offset, )

        while True:
            try:
                self.stream = urllib2.urlopen(req, timeout = 180)
                break
            except socket.timeout:
                continue
            except socket.error, e:
                if e.errno != errno.ECONNRESET:
                    raise

    def read(self, n):
        buffer = []
        while self.offset < self.file_size:
            try:
                data = self.stream.read(min(n, self.file_size - self.offset))
                self.offset += len(data)
                n -= len(data)
                buffer.append(data)
                if n == 0 or data:
                        break
            except socket.timeout:
                self._progress()
                self._restart()
            except socket.error as e:
                if e.errno != errno.ECONNRESET:
                    raise
                self._progress()
                self._restart()
        return "".join(buffer)


def copy_with_decrypt(input, output, key, media_sequence):
    if key.iv is not None:
        iv = str(key.iv)[2:]
    else:
        iv = "%032x" % media_sequence
    aes = AES.new(key.key_value, AES.MODE_CBC, iv.decode('hex'))
    while True:
        data = input.read(blocksize)
        if not data:
            break
        output.write(aes.decrypt(data))

def fetch_streams(output, video):
    output = open(output, 'wb')
    for n, seg in enumerate(video.segments):
        percentage = ((n + 1) * 100)/len(video.segments)
        avail_dots = 50
        shaded_dots = int(math.floor(float(n + 1) / len(video.segments) * avail_dots))
        sys.stdout.write("\r" + '[' + '.'*shaded_dots + ' '*(avail_dots-shaded_dots) + '] %'+str(percentage)+' (%d/%d)\x1b[2K' % (n + 1, len(video.segments)))
        sys.stdout.flush()
        raw = resumable_fetch(seg.uri, n+1, len(video.segments))
        if hasattr(video, 'key'):
           copy_with_decrypt(raw, output, video.key, video.media_sequence + n)
        else:
           copy_with_decrypt(raw, output, video.keys[0], video.media_sequence + n)
        size = output.tell()
        if size % 188 != 0:
            size = size // 188 * 188
            output.seek(size)
            output.truncate(size)
    print '\n'

def fetch_encryption_key(video):
    if hasattr(video, 'key'):
        assert video.key.method == 'AES-128'
        video.key.key_value = urllib2.urlopen(url = video.key.uri).read()
    else:
        assert video.keys[0].method == 'AES-128'
        video.keys[0].key_value = urllib2.urlopen(url = video.keys[0].uri).read()

def find_best_video(uri):
    playlist = m3u8.load(uri)
    if not playlist.is_variant:
        return playlist
    best_stream = playlist.playlists[0]
    for stream in playlist.playlists:
        if stream.stream_info.bandwidth == 'max' or stream.stream_info.bandwidth > best_stream.stream_info.bandwidth:
            best_stream = stream
    return find_best_video(best_stream.absolute_uri)

def video_hls(uri, output):
    video = find_best_video(uri)
    fetch_encryption_key(video)
    fetch_streams(output, video)
