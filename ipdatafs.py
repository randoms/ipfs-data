#!/usr/bin/env python

from __future__ import with_statement

import os
import sys
import errno
from pymongo import MongoClient
import time
import subprocess
from fuse import FUSE, FuseOSError, Operations
import requests
import thread
import threading
import re
import json
from contextlib import closing

Debug = True
tempPath = "/tmp"
ipfsGatewayPort = 8080

class Command(object):
    def __init__(self, cmd):
        self.cmd = cmd
        self.process = None

    def run(self, timeout=0):
        def target():
            print 'Thread started'
            self.process = subprocess.Popen(self.cmd, shell=True)
            self.process.communicate()
            print 'Thread finished'
        thread = threading.Thread(target=target)
        thread.start()
        if timeout == 0:
            return
        thread.join(timeout)
        if thread.is_alive():
            print 'Terminating process'
            self.process.terminate()
            thread.join()
        print self.process.returncode

class downloadThread(threading.Thread):

    def __init__(self, cache, start):
        super(downloadThread, self).__init__()
        self._stop = threading.Event()
        self.cache = cache
        self.startIndex = start

    def stop(self):
        self._stop.set()

    def run(self):
        # add thread record
        self.cache["lock"].acquire()
        print "download thread start " + self.cache["hash"] + " " + str(self.startIndex)
        if self.cache["download"] != None:
            print "Error download thread error"
        self.cache["download"] = self
        self.cache["lock"].release()
        self.errorFlag = False
        chunkIndex = self.startIndex
        try:
            r = requests.get("http://127.0.0.1:"+ str(ipfsGatewayPort) +"/ipfs/" + self.cache["hash"],
             headers={"range": "bytes="+ str(self.startIndex) +"-"}, stream=True, timeout=10)
            for chunk in r.iter_content(chunk_size=1024*1024*2):
                if chunk: # filter out keep-alive new chunks
                    self.cache["lock"].acquire()
                    if chunkIndex == self.startIndex:
                        self.cache["data"] = chunk
                        self.cache["start"] = self.startIndex
                    else:
                        self.cache["data"] += chunk
                    chunkIndex += len(chunk)
                    self.cache["end"] = chunkIndex
                    self.cache["lock"].release()
                if self.stopped():
                    break
                if self.cache["end"] -  self.cache["start"] > 40*1024*1024:
                    # max cache size 40M
                    print "max cache size"
                    break
            r.close()
        except:
            self.errorFlag = True
            return
        self.cache["lock"].acquire()
        if self.cache["download"] == self:
            # download completed remove thread record
            self.cache["download"] = None
        print "download thread end " + self.cache["hash"]
        self.cache["lock"].release()
    def stopped(self):
        return self._stop.isSet()

class Passthrough(Operations):
    def __init__(self, mountpoint):
        print "init"
        self.root = "/"
        self.mountpoint = mountpoint
        self.saveQueue = []
        self.openFiles = {}
        self.fileCache = {}
        self.fdCache = {}
        self.saveQueueLock = threading.Lock()
        self.openFilesLock = threading.Lock()
        self.fileCacheLock = threading.Lock()
        self.fdCacheLock = threading.Lock()
        self.ipfsLock = threading.Lock()
        client = MongoClient()
        db = client["ipfsdatadb"]
        if len(db.collection_names()) == 0:
            # create database
            if "files" in db.collection_names():
                db["files"].drop()
            filesCollection = db["files"]
            # database cannot be configured
            print "database create success"
            # insert root dir
            filesCollection.insert_one({
                "isdir": True,
                "abs_path": ":/",
                "st_atime": time.time(),
                "st_ctime": time.time(),
                "st_mtime": time.time(),
                "st_size": 4096,
                "st_nlink": 0,
                "st_mode": 0o40777,
                "is_deleted": False
            })
        self.filesCollection = db["files"]


    # Helpers
    # =======

    def _full_path(self, partial):
        if partial.startswith("/"):
            partial = partial[1:]
        path = os.path.join(self.root, partial)
        return path

    def get_info(self, path, remote=False):
        if path[0] == ":":
            # already a abs_path
            full_path = path[1:]
        else:
            full_path = self._full_path(path)
        info = None
        # force update from server
        if remote:
            info = self.filesCollection.find_one({"abs_path": ":" + full_path})
            if info != None:
                self.set_info(info)
            return info

        self.openFilesLock.acquire()
        if self.openFiles.has_key(":" + full_path):
            info = self.openFiles[":" + full_path]
        if info == None:
            info = self.filesCollection.find_one({"abs_path": ":" + full_path})
            if info != None:
                self.openFiles[info["abs_path"]] == info
            if Debug:
                print "path not found"
        self.openFilesLock.release()
        return info

    def set_info(self, info):
        if info != None:
            self.openFilesLock.acquire()
            self.openFiles[info["abs_path"]] = info
            self.openFilesLock.release()

    def delete_info(self, info):
        self.openFilesLock.acquire()
        self.openFiles.pop(info["abs_path"])
        self.openFilesLock.release()

    def get_cache(self, hash, start, end):
        if self.fileCache.has_key(hash):
            self.fileCacheLock.acquire()
            cache = self.fileCache[hash]
            self.fileCacheLock.release()
            cache["lock"].acquire()
            if start >= cache["start"] and end <= cache["end"]:
                data = cache["data"][(start-cache["start"]):(end - cache["start"])]
                cache["lock"].release()
                return data
            else:
                cache["lock"].release()
                if cache["download"] != None:
                    # stop download thread
                    cache["download"].stop()
                    if cache["download"] != None:
                        cache["download"].join()
                # start new download thread
                downloadThread(cache, start).start()
        else:
            cache = {
                "start": start,
                "end": start,
                "data": "",
                "lock": threading.Lock(),
                "download": None,
                "hash": hash,
            }
            self.fileCacheLock.acquire()
            self.fileCache[hash] = cache
            self.fileCacheLock.release()
            downloadThread(cache, start).start()
        # wait for data
        while True:
            time.sleep(0.001)
            cache["lock"].acquire()
            if start >= cache["start"] and end <= cache["end"]:
                data = cache["data"][(start-cache["start"]):(end - cache["start"])]
                cache["lock"].release()
                return data
            cache["lock"].release()
            if cache["download"] != None and cache["download"].errorFlag:
                cache["download"] = None
                raise OSError(errno.ENOENT, os.strerror(errno.ENOENT), hash)


    # Filesystem methods
    # ==================

    def access(self, path, mode):
        info = self.get_info(path, remote=True)
        if info == None:
            raise FuseOSError(errno.EACCES)

    def getattr(self, path, fh=None):
        # get file record from database
        info = self.get_info(path, remote=True)
        if info == None:
            full_path = self._full_path(path)
            raise OSError(errno.ENOENT, os.strerror(errno.ENOENT), full_path)
        return info


    def readdir(self, path, fh):
        full_path = self._full_path(path)
        if full_path == "/":
            subDirs = self.filesCollection.find({"abs_path": {'$regex':'^' + ":" + "/[^\/]+$"}})
        else:
            subDirs = self.filesCollection.find({"abs_path": {'$regex':'^' + ":" + re.escape(full_path) + "/[^\/]+$"}})
        info = self.get_info(path, remote=True)
        dirents = ['.', '..']
        if info != None and info["isdir"] and not info["is_deleted"]:
            for filename in subDirs:
                if not filename["is_deleted"]:
                    dirents.append(filename["abs_path"].split("/")[-1])
                    self.set_info(filename)
        if info == None:
            full_path = self._full_path(path)
            raise OSError(errno.ENOENT, os.strerror(errno.ENOENT), full_path)
        for r in dirents:
            yield r


    def rmdir(self, path):
        info = self.get_info(path)
        if info != None:
            self.filesCollection.delete_one({"_id": info["_id"]})
            self.delete_info(info)
            subDirs = self.filesCollection.find({"abs_path": {'$regex':'^' + re.escape(info["abs_path"]) + "/[^\/]+$"}})
            for subdir in subDirs:
                if subdir["isdir"]:
                    self.rmdir(subdir["abs_path"][1:])
                else:
                    self.unlink(subdir["abs_path"][1:])


    def mkdir(self, path, mode):
        self.rmdir(path)
        info = {
            "abs_path": ":" + self._full_path(path),
            "isdir": True,
            "st_atime": time.time(),
            "st_ctime": time.time(),
            "st_mtime": time.time(),
            "st_size": 4096,
            "st_nlink": 0,
            "st_mode": 0o40777,
            "is_deleted": False,
            "st_uid": os.getuid(),
            "st_gid": os.getgid(),
        }
        self.filesCollection.insert_one(info)
        self.set_info(info)
        return

    def unlink(self, path):
        if Debug:
            print "start unlink"
        info = self.get_info(path)
        if info == None:
            full_path = self._full_path(path)
            raise OSError(errno.ENOENT, os.strerror(errno.ENOENT), full_path)
        # remove temp file if exist
        if os.path.isfile(info["content_temp_path"]):
            os.remove(info["content_temp_path"])
        self.filesCollection.delete_one({"_id": info["_id"]})
        self.delete_info(info)
        if not info["isdir"] and info["content_hash"] != "":
            # check ref
            ref = self.filesCollection.find_one({"content_hash": info["content_hash"]})
            if ref == None:
                # try to get from local
                try:
                    r = requests.get("http://127.0.0.1:"+ str(ipfsGatewayPort) +"/ipfs/" + info["content_hash"], headers={"range": "bytes=0-1"}, timeout=1)
                    # only run the next step when get file success
                    Command('exec curl localhost:5001/api/v0/pin/rm?arg=' + info["content_hash"]).run(5)
                except:
                    pass
        if Debug:
            print "end unlink"


    def rename(self, old, new):
        full_path = self._full_path(new)
        info = self.get_info(new)
        if info != None:
            self.unlink(new)
        full_path = self._full_path(old)
        info = self.get_info(old)
        if info == None:
            full_path = self._full_path(path)
            raise OSError(errno.ENOENT, os.strerror(errno.ENOENT), full_path)
        self.delete_info(info)
        info["abs_path"] = ":" + self._full_path(new)
        self.set_info(info)
        self.filesCollection.update_one({"_id": info["_id"]}, {"$set": {
            "abs_path": info["abs_path"]
        }})

    def utimens(self, path, times=None):
        full_path = self._full_path(path)
        access_time = time.time()
        modification_time = time.time()
        if times != None:
            access_time = times[0]
            modification_time = times[1]
        info = self.get_info(path)
        if info == None:
            full_path = self._full_path(path)
            raise OSError(errno.ENOENT, os.strerror(errno.ENOENT), full_path)
        info["st_atime"] = access_time
        info["st_mtime"] = modification_time
        self.set_info(info)
        self.filesCollection.update_one({"_id": info["_id"]}, {"$set": {
            "st_atime": access_time,
            "st_mtime": modification_time
        }})


    # File methods
    # ============

    def open(self, path, flags):
        if Debug:
            print "*********************open"
        info = self.get_info(path)
        if info == None or info["isdir"]:
            full_path = self._full_path(path)
            raise OSError(errno.ENOENT, os.strerror(errno.ENOENT), full_path)
        if not os.path.isfile(info["content_temp_path"]):
            return os.open(info["content_temp_path"], os.O_RDWR | os.O_CREAT)
        return os.open(info["content_temp_path"], flags)

    def create(self, path, mode, fi=None):
        if Debug:
            print "*********************create"
        info = self.get_info(path)
        if info == None:
            # temp dirs
            full_path = tempPath + "/ipfsdata" + str(int(time.time()*1000))
            # insert file record
            info = {
                "abs_path": ":" + self._full_path(path),
                "isdir": False,
                "st_atime": time.time(),
                "st_ctime": time.time(),
                "st_mtime": time.time(),
                "st_size": 0,
                "st_nlink": 1,
                "st_mode": 0o100777,
                "is_deleted": False,
                "content_temp_path": full_path,
                "content_hash": "",
                "st_uid": os.getuid(),
                "st_gid": os.getgid(),
            }
            self.filesCollection.insert_one(info)
            self.set_info(info)
        return os.open(info["content_temp_path"], os.O_WRONLY | os.O_CREAT, mode)

    def read(self, path, length, offset, fh):
        if Debug:
            print "*********************read"
        start = time.time()
        # find file record in database
        info = self.get_info(path)
        if info == None or info["isdir"]:
            full_path = self._full_path(path)
            raise OSError(errno.ENOENT, os.strerror(errno.ENOENT), full_path)
        # content has been writen into temp file, read from it
        if os.path.isfile(info["content_temp_path"]) and os.stat(info["content_temp_path"]).st_size != 0:
            os.lseek(fh, offset, os.SEEK_SET)
            return os.read(fh, length)
        if info["content_hash"] == "":
            return ""
        print str(time.time() - start)
        # use range
        end = offset + length
        if end > info["st_size"]:
            end = info["st_size"]
        data = self.get_cache(info["content_hash"], offset, end)
        print str(time.time() - start)
        if Debug:
            print "end read " + str(len(data)) + " "  + str(length)
        return data

    def write(self, path, buf, offset, fh):
        full_path = self._full_path(path)
        info = self.get_info(path)
        if info == None or info["isdir"]:
            full_path = self._full_path(path)
            raise OSError(errno.ENOENT, os.strerror(errno.ENOENT), full_path)
        if info["content_hash"] != "" and (not os.path.isfile(info["content_temp_path"]) or os.stat(info["content_temp_path"]).st_size == 0):
            # load data to temp file
            if Debug:
                print "ipfs cat " + info["content_hash"] + " > " + info["content_temp_path"]
            Command("exec curl -s -o " + info["content_temp_path"] + " http://127.0.0.1:"+ str(ipfsGatewayPort) +"/ipfs/" + info["content_hash"]).run(500)
        offsetRecord = 0
        self.fdCacheLock.acquire()
        if self.fdCache.has_key(str(fh)):
            offsetRecord = self.fdCache[str(fh)]
        if offsetRecord != offset or offsetRecord == 0:
            offsetRecord = offset
            os.lseek(fh, offsetRecord, os.SEEK_SET)
        self.fdCache[str(fh)] = offsetRecord + len(buf)
        self.fdCacheLock.release()
        return os.write(fh, buf)

    def flush(self, path, fh):
        if Debug:
            print "*********************flush"
        return os.fsync(fh)

    def save(self, info):
        self.saveQueueLock.acquire()
        if info["_id"] in self.saveQueue:
            self.saveQueueLock.release()
            return
        self.saveQueue.append(info["_id"])
        self.saveQueueLock.release()
        while True:
            time.sleep(60)
            infoId = info["_id"]
            info = self.get_info(info["abs_path"])
            if info == None:
                self.saveQueueLock.acquire()
                self.saveQueue.remove(infoId)
                self.saveQueueLock.release()
                return
            if time.time() - info["st_atime"] > 10:
                # not touch for more than 10s
                self.ipfsLock.acquire()
                if Debug:
                    print "############################################"
                    print "add to ipfs"
                output = subprocess.Popen('curl -s -F "file=@' + info["content_temp_path"] + '" localhost:5001/api/v0/add', shell=True, stdout=subprocess.PIPE).stdout.read()
                try:
                    print output
                    output = json.loads(output)
                except Exception as e:
                    output = {}
                self.ipfsLock.release()
                if not output.has_key("Hash") or output["Hash"] == "":
                    # add failed
                    if Debug:
                        print "add to ipfs failed " + info["abs_path"]
                        print output
                        print info
                        break
                if Debug:
                    print output["Hash"]
                info["content_hash"] = output["Hash"]
                info["st_size"] = os.stat(info["content_temp_path"]).st_size
                info["st_atime"] = time.time()
                self.filesCollection.update_one({"_id": info["_id"]}, {"$set": {
                    "content_hash": info["content_hash"],
                    "st_size": info["st_size"],
                    "st_atime": info["st_atime"]
                }})
                os.remove(info["content_temp_path"])
                self.saveQueueLock.acquire()
                try:
                    self.saveQueue.remove(info["_id"])
                except:
                    pass
                self.saveQueueLock.release()
                self.set_info(info)
                break;

    def release(self, path, fh):

        if Debug:
            print "*********************release"
        full_path = self._full_path(path)
        info = self.get_info(path)
        if info == None or info["isdir"]:
            full_path = self._full_path(path)
            raise OSError(errno.ENOENT, os.strerror(errno.ENOENT), full_path)
        if os.stat(info["content_temp_path"]).st_size != 0:
            info["st_atime"] = time.time()
            info["st_size"] = os.stat(info["content_temp_path"]).st_size
            self.filesCollection.update_one({"_id": info["_id"]}, {"$set": {
                "st_size": info["st_size"],
                "st_atime": info["st_atime"]
            }})
            self.set_info(info)
            thread.start_new(self.save, (info,))
            os.close(fh)
        else:
            os.close(fh)
            os.remove(info["content_temp_path"])
            self.fileCacheLock.acquire()
            self.fileCache.pop(info["content_hash"], None)
            self.fileCacheLock.release()

    def truncate(self, path, length, fh=None):
        info = self.get_info(path)
        if info == None or info["isdir"]:
            full_path = self._full_path(path)
            raise OSError(errno.ENOENT, os.strerror(errno.ENOENT), full_path)
        with open(info["content_temp_path"], 'r+') as f:
            f.truncate(length)



    def fsync(self, path, fdatasync, fh):
        if Debug:
            print "*********************fsync"
        return self.flush(path, fh)


def main(mountpoint):
    FUSE(Passthrough(mountpoint), mountpoint, nothreads=True, foreground=True, allow_other=True)

if __name__ == '__main__':
    print sys.argv
    if len(sys.argv) != 1 and len(sys.argv) != 2:
        print "Usage: python ipdatafs MOUNT_POINT"
    else:
        main(sys.argv[1])
