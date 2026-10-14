# dss/user.py
import socket, sys
from messages import encode, decode, MsgType, SUCCESS
# to use our trace messaging. 
from trace import log_send, log_recv, log_info


# usage: python user.py <manager_ip> <manager_m_port> <user-name> <my_ip> <my_m_port> <my_c_port>
# example: python user.py 127.0.0.1 29500 Alice 127.0.0.1 29501 29502

def main():
	# make sure CLI is correct 
    if len(sys.argv) != 7:
        print("usage: python -m dss.user <manager_ip> <manager_m_port> <user-name> <my_ip> <my_m_port> <my_c_port>")
        sys.exit(1)

	# take in the command line argument
    mgr_ip   = sys.argv[1]
    mgr_port = int(sys.argv[2])
    name     = sys.argv[3]
    my_ip    = sys.argv[4]
    my_m     = int(sys.argv[5])
    my_c     = int(sys.argv[6])
	
	
    payload = {
        "user-name": name,
        "IPv4-addr": my_ip,
        "m-port": my_m,
        "c-port": my_c,
    }
	# create a udp socket 
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.settimeout(2.0)
        log_send(f"user:{name}", "REGISTER_USER", payload, (mgr_ip, mgr_port))
        sock.sendto(encode(MsgType.REGISTER_USER, **payload), (mgr_ip, mgr_port))
        try:
            data, addr = sock.recvfrom(4096)
        except socket.timeout:
            log_info(f"user:{name}", "!!! no response (timeout)"); return

        mtype, resp = decode(data)
        log_recv(f"user:{name}", mtype.name, resp, addr)
        # get the managers response and then decode it
        if mtype.name == "REPLY" and resp.get("status") == SUCCESS:
            log_info(f"user:{name}", "SUCCESS: registered")
        else:
            log_info(f"user:{name}", f"FAILURE: {resp.get('reason')}")

if __name__ == "__main__":
    main()
