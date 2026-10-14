import socket, sys
from messages import encode, decode, MsgType, SUCCESS
from trace import log_send, log_recv, log_info

#CLI interface to communicate to manager over UDP to send message
#to deregister disk. 
# how to use: python3 deregister_disk.py <manager_ip> <manager_m_port> <disk-name>
def main():
	#parse user input 
    if len(sys.argv) != 4:
        print("usage: python -m dss.deregister_disk <manager_ip> <manager_m_port> <disk-name>")
        sys.exit(1)
	
    mgr_ip, mgr_port, name = sys.argv[1], int(sys.argv[2]), sys.argv[3]
    payload = {"disk-name": name}
	# create UDP socket - send message and receive response 
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.settimeout(2.0)
        log_send(f"disk:{name}", "DEREGISTER_DISK", payload, (mgr_ip, mgr_port))
        sock.sendto(encode(MsgType.DEREGISTER_DISK, **payload), (mgr_ip, mgr_port))
        try:
            data, addr = sock.recvfrom(4096)
        except socket.timeout:
           log_info(f"disk:{name}", "!!! no response (timeout)"); return
        mtype, resp = decode(data)
        log_recv(f"disk:{name}", mtype.name, resp, addr)
        if resp.get("status")==SUCCESS:
        	log_info(f"disk:{name}", "SUCCESS")
        else:
        	log_info(f"disk:{name}", f"FAILURE: {resp.get('reason')}")

if __name__ == "__main__":
    main()
