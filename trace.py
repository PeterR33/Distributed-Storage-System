# trace.py
import threading
from typing import Any

#to keep track of message and trac what thread they are coming from 

def who() -> str:
    tname = threading.current_thread().name # get the surren thread name 
    return f"thr:{tname}" 

#print the message send 
def log_send(proc: str, mtype: Any, payload: dict, dst: tuple[str, int]) -> None:
    print(f"[{proc}|{who()}] -> {mtype} {payload} to {dst}")    

#print the message for receive 
def log_recv(proc: str, mtype: Any, payload: dict, src: tuple[str, int]) -> None:
    print(f"[{proc}|{who()}] <- {mtype} {payload} from {src}")

#long the information
def log_info(proc: str, msg: str) -> None:
    print(f"[{proc}|{who()}] {msg}")

