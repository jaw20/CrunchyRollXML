import sys
import errno
import m3u8
import urllib2
from Crypto.Cipher import AES
import StringIO
import socket
import os
from threading import Thread
from time import sleep
import math
import time
import subprocess
import requests

blocksize = 16384

class resumable_fetch:
    def __init__(self, uri, cur, total):
        self.uri = uri
        self.cur = cur
        self.total = total
        self.offset = 0
        self._restart()
        try:
            self.file_size = int(self.stream.info().get('Content-Length', -1))
        except:
            self.file_size = int(self.stream.headers['content-length'])
        #print self.file_size
        if self.file_size <= 0:
            print "Invalid file size"
            sys.exit()

    def _progress(self):
        sys.stdout.write('\x1b[2K\r%d/%d' % (self.cur, self.total))
        sys.stdout.flush()

    def _restart(self):
        try:
            #import requstss2
            req = urllib2.Request(self.uri)
            if self.offset:
                req.headers['Range'] = 'bytes=%s-' % (self.offset, )

            while True:
                try:
                    self.stream = urllib2.urlopen(req, timeout = 180)
                    #print self.stream
                    break
                except socket.timeout:
                    continue
                except socket.error, e:
                    if e.errno != errno.ECONNRESET:
                        raise
        except:
            if self.offset:
                headers = {'Range': 'bytes=%s-' % (self.offset, )}
                req = requests.get(self.uri, headers=headers, stream=True, timeout = 180)
            else:
                req = requests.get(self.uri, stream=True, timeout = 180)

            while True:
                try:
                    self.stream = req.raw
                    break
                except socket.timeout:
                    continue
                except socket.error, e:
                    if e.errno != errno.ECONNRESET:
                        raise

    def read(self, n):
        buffer = []
        global download_size_
        if not 'download_size_' in globals():
            download_size_ = [0]*self.total
        download_size_[self.cur-1] = self.offset
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

def compute_total_size(video):
    global total_size
    global total_size_l_
    total_size = 0
    total_size_l_=[]
    for n, seg in enumerate(video.segments):
        try:
            req_1 = urllib2.Request(seg.uri)
            stream_1 = urllib2.urlopen(req_1, timeout = 180)
            total_size += int(stream_1.info().get('Content-Length', -1))
            total_size_l_.append(stream_1.info().get('Content-Length', -1))
        except:
            stream_1r = requests.head(seg.uri, timeout = 180).headers['content-length']
            total_size += int(stream_1r)
            total_size_l_.append(stream_1r)

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

def size_adj(size_, x_):
    if x_ == 'harddisk':
        if size_/1024 > 1:
            if (size_/1024)/1024 > 1:
                if ((size_/1024)/1024)/1024 > 1:
                    size_out_ = str(((size_/1024)/1024)/1024)+'GB'
                else:
                    size_out_ = str((size_/1024)/1024)+'MB'
            else:
                size_out_ = str(size_/1024)+'KB'
        else:
            size_out_ = str(size_)+'bytes'
    if x_ == 'internet':
        if size_/1024 > 1:
            if (size_/1024)/1024 > 1:
                if ((size_/1024)/1024)/1024 > 1:
                    size_out_ = format(((size_/1024)/1024)/1024, '.2f')+'Gb/s'
                else:
                    size_out_ = format((size_/1024)/1024, '.2f')+'Mb/s'
            else:
                size_out_ = format(size_/1024, '.2f')+'Kb/s'
        else:
            size_out_ = format(size_, '.2f')+'b/s'
    return size_out_

def download(video, output, Url, seg_n, connection_n):
    global start_t
    if not 'start_t' in globals():
        start_t = time.clock()
    raw = resumable_fetch(Url, seg_n, len(video.segments))
    progress_.append(seg_n)
    percentage = ((len(progress_)) * 100)/len(video.segments)
    avail_dots = 30
    if total_size_t_.is_alive():
        total_size_s_ = 'C.Size'
    else:
        total_size_s_ = size_adj(total_size, 'harddisk')
    shaded_dots = int(math.floor(float(len(progress_) + 1) / len(video.segments) * avail_dots))
    global max_output_len
    if not 'max_output_len' in globals():
        max_output_len = 0
    for i in range(0,connection_n):
        try:
            os.path.getsize(os.path.join(os.getcwd(), output.name[:-1]+str(i)))
        except:
            pass
    global download_size_
    if not 'download_size_' in globals():
        download_size_ = [0]*len(video.segments)
    download_size_1 = 0
    for i in download_size_:
        download_size_1 += i

    output_len = len("\r" + '[' + '.'*shaded_dots + ' '*(avail_dots-shaded_dots) + '] %'+str(percentage)+' (%d/%d) %s/%s @ %s' % (len(progress_), len(video.segments),size_adj(download_size_1, 'harddisk'), total_size_s_, str(size_adj(download_size_1/(time.clock()-start_t), 'internet'))))
    max_output_len = max(max_output_len, output_len)
    sys.stdout.write("\r" + '[' + '.'*shaded_dots + ' '*(avail_dots-shaded_dots) + '] %'+str(percentage)+' (%d/%d) %s/%s @ %s' % (len(progress_), len(video.segments),size_adj(download_size_1, 'harddisk'), total_size_s_, str(size_adj(download_size_1/(time.clock()-start_t), 'internet'))) + ' '*(max_output_len-output_len))
    sys.stdout.flush()
    if hasattr(video, 'key'):
        copy_with_decrypt(raw, output, video.key, video.media_sequence + seg_n-1)
    else:
        copy_with_decrypt(raw, output, video.keys[0], video.media_sequence + seg_n-1)
    size = output.tell()
    if size % 188 != 0:
        size = size // 188 * 188
        output.seek(size)
        output.truncate(size)
	
def down_thread(video, output, start, end, seg_url, connection_n):
    #this function is for debug
    for i in range(start, end+1):
        download(video, output, seg_url[i-1], i, connection_n)

def fetch_streams(output_dir, video, connection_n):
    global total_size
    global total_size_t_

    total_size_t_ = Thread(target=compute_total_size,args=[video])
    total_size_t_.start()
    if len(video.segments)/connection_n <2:
        connection_n = len(video.segments)/2
    connection_dist = []
    seg_arr_list_=[]
    seg_arr_list_2_=[]
    for i in range(1, len(video.segments)+1):
        seg_arr_list_.append(i)
        seg_arr_list_2_.append(i)
    seg_arr_list_.reverse()
    seg_arr_list_2_.reverse()
    for l in range(0, len(video.segments)):
        for i in range(0,connection_n):
            if not 'seg_arr_list_len_{0}'.format(i) in locals():
                locals()['seg_arr_list_len_{0}'.format(i)]=[]
            try:
                locals()['seg_arr_list_len_{0}'.format(i)].append(seg_arr_list_.pop())
            except:
                pass
    for i in range(0, connection_n):
        for l in range(0, len(locals()['seg_arr_list_len_{0}'.format(i)])):
            if not 'thread_dis_{0}'.format(i) in locals():
                locals()['thread_dis_{0}'.format(i)]=[]
            try:
                locals()['thread_dis_{0}'.format(i)].append(seg_arr_list_2_.pop())
            except:
                pass
    connection_dist = []
    for i in range(0, connection_n):
        connection_dist.append(min(locals()['thread_dis_{0}'.format(i)]))
        connection_dist.append(max(locals()['thread_dis_{0}'.format(i)]))
            
    seg_url = []
    for n, seg in enumerate(video.segments):
        seg_url.append(seg.uri)
    connection_dist.reverse()
    threads = []
    global progress_
    progress_ = []
    for i in range (1, connection_n+1):
        locals()['file_seg_{0}'.format(i)] =  open(output_dir+str(i), 'wb')
        threads.append(Thread(target=down_thread,args=(video, locals()['file_seg_{0}'.format(i)], connection_dist.pop(), connection_dist.pop(), seg_url, connection_n)))
    # Start all threads
    for x in threads:
        x.start()

    # Wait for all of them to finish
    for x in threads:
        x.join()
    #print locals()
    if connection_n==1:
        locals()['file_seg_1'].close()
        os.rename(output_dir+'1',output_dir)
    else:
        #final_file_ = open(output_dir, 'wb')
        cmd_appd = ['copy /b ','cat ']
        for i in range (1, connection_n+1):
            cmd_appd[0] += '"'+output_dir+str(i)+'"+'
            cmd_appd[1] += '"'+output_dir+str(i)+'" '
            #temp_file_ = open(output_dir+str(i), 'rb')
            #final_file_.write(temp_file_.read())
            #temp_file_.close()
            locals()['file_seg_{0}'.format(i)].close()
        #final_file_.close()
        cmd_appd[0] = cmd_appd[0][:-1]
        cmd_appd[0] += ' "'+output_dir+'"'
        cmd_appd[1] += '> "'+output_dir+'"'
        #print cmd_appd
        try:
            subprocess.call(cmd_appd[0], shell=True)
        except:
            subprocess.call(cmd_appd[1], shell=True)
        for i in range (1, connection_n+1):
            os.remove(output_dir+str(i))
    print '\n'


def fetch_encryption_key(video):
    if hasattr(video, 'key'):
        assert video.key.method == 'AES-128'
        try:
            video.key.key_value = urllib2.urlopen(url = video.key.uri).read()
        except:
            video.key.key_value = requests.get(video.keys[0].uri).text.encode('windows-1252')
    else:
        assert video.keys[0].method == 'AES-128'
        try:
            video.keys[0].key_value = urllib2.urlopen(url = video.keys[0].uri).read()
        except:
            video.keys[0].key_value = requests.get(video.keys[0].uri).text.encode('windows-1252')

def find_best_video(uri):
    playlist = m3u8.load(uri)
    if not playlist.is_variant:
        return playlist
    best_stream = playlist.playlists[0]
    for stream in playlist.playlists:
        if stream.stream_info.bandwidth == 'max' or stream.stream_info.bandwidth > best_stream.stream_info.bandwidth:
            best_stream = stream
    return find_best_video(best_stream.absolute_uri)

def video_hls(uri, output, connection_n):
    video = find_best_video(uri)
    connection_n = connection_n
    fetch_encryption_key(video)
    fetch_streams(output, video, connection_n)

if __name__ == '__main__':
    connection_n = 1
    try:
        uri = sys.argv[1]
    except:
        print "invalid url"
    try:
        if int(sys.argv[2]):
            connection_n = int(sys.argv[2])
    except:
        try:
            output = sys.argv[2]
        except:
            output = "download.ts"
    try:
        if int(sys.argv[3]):
            connection_n = int(sys.argv[3])
    except:
        try:
            output = sys.argv[3]

        except:
            if not 'output' in locals():
                output = "download.ts"

    video_hls(uri, output, connection_n)