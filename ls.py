import socket, sys, json
from messages import encode, decode, MsgType, SUCCESS
from trace import log_send, log_recv, log_info

#client to test that the ls works
# python3 ls.py <manager_ip> <manager_m_port>
def main():
    if len(sys.argv) != 3:
        print("usage: python3 ls.py <manager_ip> <manager_m_port>"); sys.exit(1)
    mgr_ip, mgr_port = sys.argv[1], int(sys.argv[2])
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.settimeout(2.0)
        payload = {}
        log_send("user", "LS", payload, (mgr_ip, mgr_port))
        #send message types LS
        sock.sendto(encode(MsgType.LS, **payload), (mgr_ip, mgr_port))
        try:
            data, addr = sock.recvfrom(4096)
        except socket.timeout:
            log_info("user", "!!! no response (timeout)"); return
        mtype, resp = decode(data)
        log_recv("user", mtype.name, resp, addr)
        if mtype.name == "REPLY" and resp.get("status") == SUCCESS:
            summary = resp.get("dss_summary", [])
            log_info("user", "SUCCESS: LS")
            # pretty print for your demo
            print(json.dumps(summary, indent=2))
        else:
            log_info("user", f"FAILURE: {resp.get('reason')}")

if __name__ == "__main__":
    main()
