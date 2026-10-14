import socket
import sys
from messages import encode, decode, MsgType, SUCCESS
from trace import log_send, log_recv, log_info

#CLI interface to communicate to manager using UDP to tell send message
#to deregister user. 
# how to use: python3 deregister_user.py <manager_ip> <manager_m_port> <user-name>
def main():
	#parse command line input 
    if len(sys.argv) != 4:
        print("usage: python -m dss.deregister_user <manager_ip> <manager_m_port> <user-name>")
        sys.exit(1)

    mgr_ip, mgr_port, name = sys.argv[1], int(sys.argv[2]), sys.argv[3]
    payload = {"user-name": name}
	
	#start UDP connection  
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.settimeout(2.0)
        log_send(f"user:{name}", "DEREGISTER_USER", payload, (mgr_ip, mgr_port))
        #send  message 
        sock.sendto(encode(MsgType.DEREGISTER_USER, **payload), (mgr_ip, mgr_port))
        #get response 
        try:
            data, addr = sock.recvfrom(4096)
        except socket.timeout:
            log_info(f"user:{name}", "!!! no response"); return
        mtype, resp = decode(data)
        log_recv(f"user:{name}", mtype.name, resp, addr)
        if resp.get("status")==SUCCESS:
        	log_info(f"user:{name}", "SUCCESS")
        else: 
        	log_info(f"user:{name}", f"FAILURE: {resp.get('reason')}")

if __name__ == "__main__":
    main()
