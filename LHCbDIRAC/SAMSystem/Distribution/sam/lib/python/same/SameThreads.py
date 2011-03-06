from threading import Thread

class SameThread(Thread):
    
    def __init__(self,command):
        Thread.__init__(self)
        self.command=command

    def run(self):
        print self.command
