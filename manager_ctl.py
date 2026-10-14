# manager_ctl.py
import argparse, socket, sys, json
from messages import encode, decode, MsgType, SUCCESS
from trace import log_send, log_recv, log_info



#CLI to control functions with the manager, disk failure , signal recovery completion
#DSS decommission
#Do LS in her ? - future note 

#pass in the mgr port and the msg type and payload to be sent 

def send(mgr_ip: str, mgr_port: int, mtype: MsgType, payload: dict, timeout=2.0):
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.settimeout(timeout)
        #send whatever message type was passed in
        log_send("ctl", mtype.value, payload, (mgr_ip, mgr_port))
        sock.sendto(encode(mtype, **payload), (mgr_ip, mgr_port))
        try:
            data, addr = sock.recvfrom(65535)
        except socket.timeout:
            log_info("ctl", "!!! no response (timeout)")
            return None, None
        mtype2, resp = decode(data)
        log_recv("ctl", mtype2.name, resp, addr)
        return mtype2, resp

def main():

	#parse CLI
    ap = argparse.ArgumentParser(description="Manager test controller")
    ap.add_argument("mgr_ip")
    ap.add_argument("mgr_port", type=int)
    sub = ap.add_subparsers(dest="cmd", required=True)

    sub.add_parser("ls")
	#accepted commands #
    p_copy = sub.add_parser("copy")
    p_copy.add_argument("--file-name", required=True)
    p_copy.add_argument("--file-size", type=int, required=True)
    p_copy.add_argument("--owner", required=True)

    p_cc = sub.add_parser("copy-complete")
    p_cc.add_argument("--dss", required=True)
    p_cc.add_argument("--file-name", required=True)
    p_cc.add_argument("--file-size", type=int, required=True)
    p_cc.add_argument("--owner", required=True)

    p_read = sub.add_parser("read")
    p_read.add_argument("--dss", required=True)
    p_read.add_argument("--file-name", required=True)
    p_read.add_argument("--user-name", required=True)

    p_rc = sub.add_parser("read-complete")
    p_rc.add_argument("--dss", required=True)
    p_rc.add_argument("--file-name", required=True)
    p_rc.add_argument("--user-name", required=True)

    p_df = sub.add_parser("disk-failure")
    p_df.add_argument("--dss", required=True)

    p_rec = sub.add_parser("recovery-complete")
    p_rec.add_argument("--dss", required=True)

    p_dec = sub.add_parser("decommission-dss")
    p_dec.add_argument("--dss", required=True)

    args = ap.parse_args()
    ip, port = args.mgr_ip, args.mgr_port

	#send whatever cmomands where parsed 
    if args.cmd == "ls":
        m, r = send(ip, port, MsgType.LS, {})
        if m and r and r.get("status") == SUCCESS:
            print(json.dumps(r.get("dss_summary", []), indent=2))
        return

    if args.cmd == "copy":
        send(ip, port, MsgType.COPY, {
            "file-name": args.file_name,
            "file-size": args.file_size,
            "owner": args.owner
        })
        return

    if args.cmd == "copy-complete":
        send(ip, port, MsgType.COPY_COMPLETE, {
            "dss": args.dss,
            "file-name": args.file_name,
            "file-size": args.file_size,
            "owner": args.owner
        })
        return

    if args.cmd == "read":
        send(ip, port, MsgType.READ, {
            "dss": args.dss,
            "file-name": args.file_name,
            "user-name": args.user_name
        })
        return

    if args.cmd == "read-complete":
        send(ip, port, MsgType.READ_COMPLETE, {
            "dss": args.dss,
            "file-name": args.file_name,
            "user-name": args.user_name
        })
        return

    if args.cmd == "disk-failure":
        send(ip, port, MsgType.DISK_FAILURE, {
            "dss": args.dss
        })
        return

    if args.cmd == "recovery-complete":
        send(ip, port, MsgType.RECOVERY_COMPLETE, {
            "dss": args.dss
        })
        return

    if args.cmd == "decommission-dss":
        send(ip, port, MsgType.DECOMMISSION_DSS, {
            "dss": args.dss
        })
        return

if __name__ == "__main__":
    main()
