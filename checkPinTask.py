#!/usr/bin/env python

'''
regularly check pin tasks
'''
import requests
import json
import subprocess, threading
import time

server = "your server address"
pin_target_url = server + "/pin-tasks"
username = "your user name"
password = "your password"

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

while True:
    time.sleep(60)
    try:
        r = requests.get(pin_target_url, auth=(username, password))
        pin_task_list = json.loads(r.text)
        for pin_task in pin_task_list:
            print "pin " + pin_task['hash']
            Command('exec curl localhost:5001/api/v0/pin/add?arg=' + pin_task['hash']).run()
            # send delete requests
            r = requests.delete(pin_target_url + "?hash=" + pin_task['hash'], auth=(username, password))
    except:
        pass
