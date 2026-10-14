# dss/configure.py
import socket, sys
from messages import encode, decode, MsgType, SUCCESS #import our message structure
from trace import log_send, log_recv, log_info


#used so a user can ask the manager to create a DSS"
#Used UDp datagram to manager m-port asking to create DSS with n disks and a striping unit"

def main():
    if len(sys.argv) != 6:
        print("usage: python configure.py <manager_ip> <manager_m_port> <dss-name> <n> <striping-unit>")
        sys.exit(1)
	# build the payload from the command line argument. 
    mgr_ip   = sys.argv[1]
    mgr_port = int(sys.argv[2])
    name     = sys.argv[3]
    n        = int(sys.argv[4])
    su       = int(sys.argv[5])

    payload = {"dss-name": name, "n": n, "striping-unit": su}
	
	# create a UDP socket - so that we can send information to the manager. 
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.settimeout(2.5)
        log_send("user", "CONFIGURE_DSS", payload, (mgr_ip, mgr_port))
        # send the payload to the manager 
        sock.sendto(encode(MsgType.CONFIGURE_DSS, **payload), (mgr_ip, mgr_port))
        try:
            data, addr = sock.recvfrom(4096)
        except socket.timeout:
            log_info("user", "!!! no response - timeout")
            return
		# get the reply from the manager 
        mtype, resp = decode(data)  #decode it 
        # what types of message did we return. 
        log_recv("user", mtype.name, resp, addr)
        if mtype.name == "REPLY" and resp.get("status") == SUCCESS:
            order = resp.get("order")
            log_info("user", f"SUCCESS: DSS {resp.get('dss')} n={resp.get('n')} su={resp.get('striping_unit')} order={order}")
        else:
            log_info("user", f"FAILURE: {resp.get('reason')}")

if __name__ == "__main__":
    main()
