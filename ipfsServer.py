import web
from pymongo import MongoClient
import json
from bson import json_util
import subprocess, threading
import requests

urls = (
    '/pin-tasks', 'pin_task'
)

client = MongoClient("your db address")
db = client["ipfsdatadb"]
db.authenticate("your db user name", "your db password")
pinTargets = db["pins"]

class Command(object):
    def __init__(self, cmd):
        self.cmd = cmd
        self.process = None

    def run(self, timeout):
        def target():
            print 'Thread started'
            self.process = subprocess.Popen(self.cmd, shell=True)
            self.process.communicate()
            print 'Thread finished'

        thread = threading.Thread(target=target)
        thread.start()

        thread.join(timeout)
        if thread.is_alive():
            print 'Terminating process'
            self.process.terminate()
            thread.join()
        print self.process.returncode

class pin_task:

    def GET(self):
        return json.dumps(list(pinTargets.find()),default=json_util.default,indent=4)

    def DELETE(self):
        target_hash = web.input(name="hash")["hash"]
        pinTargets.delete_many({"hash": target_hash})
        # get file first
        try:
            r = requests.get("http://127.0.0.1:8080/ipfs/" + target_hash,
             headers={"range": "bytes=0-5"}, stream=True, timeout=2)
        except:
            print target_hash + " is not found in local storage"
            return "OK"
        Command('exec curl localhost:5001/api/v0/pin/rm?arg=' + target_hash).run(5)
        print "delete " + target_hash + "success"
        return "OK"


if __name__ == "__main__":
    app = web.application(urls, globals())
    app.run()
