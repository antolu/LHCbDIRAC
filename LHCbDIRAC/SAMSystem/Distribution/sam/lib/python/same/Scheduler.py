from Config import config
import os
import time
import signal

max_processes=int(config.get('scheduler','max_processes'))
default_timeout=int(config.get('scheduler','default_timeout'))
shell=config.get('scheduler','shell')

process_pool={}

def check_processes():
    global process_pool
    t=time.time()
    
    for pid, timeout in process_pool.items():
        if t>timeout:
            try:
                os.kill(-pid, signal.SIGTERM) 
                os.waitpid(pid, 0)
            except:
	        pass
            del process_pool[pid]
        else:
            if os.waitpid(pid,os.WNOHANG)[0]:
                del process_pool[pid]
           
def wait():
    global process_pool
    check_processes()
    while process_pool:
	time.sleep(1)
	check_processes()

def run(cmdline,timeout=default_timeout):
    global process_pool
    check_processes()
    while len(process_pool)>=max_processes:
	time.sleep(1)
	check_processes()

    pid=os.fork()
    if pid:
	process_pool[pid]=time.time()+timeout
    else:
	os.setpgrp()
	args=[shell,'-c',cmdline]
	os.execv(shell,args)

def killall():
    global process_pool
    for pid in process_pool.keys():
        os.kill(-pid,9)
        waitres=os.waitpid(pid,0)
    process_pool={}
    
