# dss/manager.py
import socket, sys, re
import random
#import our message format 
from messages import encode, decode, MsgType, reply_ok, reply_fail, SUCCESS, FAILURE
#import the correct ports using the config.pu file we made to define them 
from config import PORT_MIN, PORT_MAX, valid_port
from trace import log_send, log_recv, log_info
# A single process UDP server. 
# handels request to REGISER _USER to REGISTER_DISK
#CONFIGURE_DSS DEREGISTER_USER DEREGISTER DISK
#PING and PONG for testing 
#gives UDP a UDP response for each request



# To track in memory state 
dss = {}		# to tack memory of the dss 
users = {}   # to track users that are registered 
disks = {}   # to track disks that are registered 
used_ports = set()  # tracks (ip,port) UDP tuples to make sure they are different
#cannot have disk-failure while reads are running
reads_in_progress = {} #dict: dss_name - int: count 

_name_re = re.compile(r"^[A-Za-z]{1,15}$") # names must use these (alphabetical)

#makes sure the port number is valid - in the range 
# (imported from config.py: valid_port(p: int) -> bool)

# can only have one critical operation at a time - handle critical section 
current_critical = {
    "op": None,      # copy, disk-failure, decommission, None
    "dss": None,     # The DSS that the critical opp is currently on (if there is any)
}


#helper functions for the critical section 
#returns if the critical section is busy 
def critical_busy() -> bool:
    return current_critical["op"] is not None

#given the op type and the dss_name put them in the current_criticle
def start_critical(op: str, dss_name: str) -> None:
    current_critical["op"] = op
    current_critical["dss"] = dss_name

#empty out the current_critical when the op is finished 
def end_critical() -> None:
    current_critical["op"] = None
    current_critical["dss"] = None
    
#increment the reads_in_progress
def inc_reads(dss_name: str) -> None:
    reads_in_progress[dss_name] = reads_in_progress.get(dss_name, 0) + 1

def dec_reads(dss_name: str) -> None:
    cur = reads_in_progress.get(dss_name, 0)
    if cur <= 1:
        reads_in_progress.pop(dss_name, None)
    else:
        reads_in_progress[dss_name] = cur - 1

#return if there is anything in reads_in_progress to show that it is busy 
def reads_busy(dss_name: str) -> bool:
    return reads_in_progress.get(dss_name, 0) > 0
    
# Returns a list of disk endpoint triplets (disk_name, ip, c-port) in the DSS order.
def dss_endpoints(name: str):
    cfg = dss.get(name)
    if not cfg: return None
    triplets = []
    for disk_name in cfg["order"]:
        info = disks.get(disk_name)
        if not info:
            return None  # inconsistent state, a disk vanished
        triplets.append((disk_name, info["ip"], info["cport"]))
    return triplets

#Valid
def handle_copy(payload: dict, addr, sock):
    # payload =  { "file-name": str, "file-size": int, "owner": str }
    # Phase 1: choose a DSS, simple - first or round-robin later send params.
    #validate payload is correct 
    try:
        fname = payload["file-name"]
        fsize = int(payload["file-size"])
        owner = payload["owner"]
    except Exception:
        resp = reply_fail("bad COPY payload")
        sock.sendto(resp, addr); log_send("manager", "REPLY", {"status":"FAILURE","reason":"bad COPY payload"}, addr); return

    # pick a DSS – simplest policy, first configured
    
    if not dss:
        resp = reply_fail("no DSS configured")
        sock.sendto(resp, addr); log_send("manager", "REPLY", {"status":"FAILURE","reason":"no DSS configured"}, addr); return
    dss_name = next(iter(dss.keys()))
    cfg = dss[dss_name]

    # enforce critical section, only one critical op at a time
    if critical_busy(): 
        resp = reply_fail(f"busy: {current_critical['op']} in progress on {current_critical['dss']}")
        sock.sendto(resp, addr); log_send("manager", "REPLY", {"status":"FAILURE","reason":f"busy: {current_critical['op']} in progress on {current_critical['dss']}"}, addr); return
    start_critical("copy", dss_name)

    # build endpoints list 
    eps = dss_endpoints(dss_name)
    if eps is None:
        end_critical()
        resp = reply_fail("DSS endpoints unavailable")
        sock.sendto(resp, addr); log_send("manager", "REPLY", {"status":"FAILURE","reason":"DSS endpoints unavailable"}, addr); return

    # reply with DSS params for the user to start P2P writes
    # The user will later send COPY_COMPLETE when done.
    resp = reply_ok(
        dss=dss_name,
        n=cfg["n"],
        striping_unit=cfg["striping_unit"],
        order=cfg["order"],
        endpoints=eps,              # [(disk, ip, cport), ...]
        file_name=fname,
        file_size=fsize,
        owner=owner
    )
    sock.sendto(resp, addr); log_send("manager", "REPLY", {"status":"SUCCESS","dss":dss_name,"n":cfg["n"],"striping_unit":cfg["striping_unit"],"order":cfg["order"],"endpoints":eps,"file_name":fname,"file_size":fsize,"owner":owner}, addr)

#when the users finishes writing blocks this manager will be notified
#Appends the files metadata and clears the critical section 
def handle_copy_complete(payload: dict, addr, sock):
    # payload = { "dss": str, "file-name": str, "file-size": int, "owner": str }
    try:
        dss_name = payload["dss"]
        fname = payload["file-name"]
        fsize = int(payload["file-size"])
        owner = payload["owner"]
    except Exception:
        resp = reply_fail("bad COPY_COMPLETE payload")
        sock.sendto(resp, addr); log_send("manager", "REPLY", {"status":"FAILURE","reason":"bad COPY_COMPLETE payload"}, addr); return

    cfg = dss.get(dss_name)
    if not cfg:
        resp = reply_fail("unknown DSS")
        sock.sendto(resp, addr); log_send("manager", "REPLY", {"status":"FAILURE","reason":"unknown DSS"}, addr); return

    # put record file metadata into DSS directory
    files = cfg.setdefault("files", [])
    files.append({"name": fname, "size": fsize, "owner": owner})

    # release critical
    end_critical()

    resp = reply_ok()
    sock.sendto(resp, addr); log_send("manager", "REPLY", {"status":"SUCCESS"}, addr)

#Hanle disk read: 
#makes sure the user owns the DSS and increments the read count for that DSS so that 
#we know it is being read
def handle_read(payload: dict, addr, sock):
    # payload = { "dss": str, "file-name": str, "user-name": str }
    try:
        dss_name = payload["dss"]
        fname = payload["file-name"]
        uname = payload["user-name"]
    except Exception:  # if the payload is bad 
        resp = reply_fail("bad READ payload")
        sock.sendto(resp, addr); log_send("manager", "REPLY", {"status":"FAILURE","reason":"bad READ payload"}, addr); return

    cfg = dss.get(dss_name)
    if not cfg:
        resp = reply_fail("unknown DSS")
        sock.sendto(resp, addr); log_send("manager", "REPLY", {"status":"FAILURE","reason":"unknown DSS"}, addr); return

    # ownership check (spec examples enforce this)
    meta = next((f for f in cfg.get("files", []) if f["name"] == fname), None)
    if not meta:
        resp = reply_fail("file not found")
        sock.sendto(resp, addr); log_send("manager", "REPLY", {"status":"FAILURE","reason":"file not found"}, addr); return
    if meta["owner"] != uname:
        resp = reply_fail("not owner")
        sock.sendto(resp, addr); log_send("manager", "REPLY", {"status":"FAILURE","reason":"not owner"}, addr); return

    # build endpoints
    eps = dss_endpoints(dss_name)
    if eps is None:
        resp = reply_fail("DSS endpoints unavailable")
        sock.sendto(resp, addr); log_send("manager", "REPLY", {"status":"FAILURE","reason":"DSS endpoints unavailable"}, addr); return

    # increment read to Mark as in progress for this DSS
    inc_reads(dss_name)

    # reply with params so the user can perform parallel reads and parity verification
    resp = reply_ok(
        dss=dss_name,
        file_name=fname,
        file_size=meta["size"],
        n=cfg["n"],
        striping_unit=cfg["striping_unit"],
        order=cfg["order"],
        endpoints=eps
    )
    sock.sendto(resp, addr); log_send("manager", "REPLY", {"status":"SUCCESS","dss":dss_name,"file_name":fname,"file_size":meta["size"],"n":cfg["n"],"striping_unit":cfg["striping_unit"],"order":cfg["order"],"endpoints":eps}, addr)

#decremeants the read count for the DSS - it is no longer being read
def handle_read_complete(payload: dict, addr, sock):
    # payload = { "dss": str, "file-name": str, "user-name": str }
    try:
        dss_name = payload["dss"]
        _ = payload["file-name"]
        _ = payload["user-name"]
    except Exception:
        resp = reply_fail("bad READ_COMPLETE payload")
        sock.sendto(resp, addr); log_send("manager", "REPLY", {"status":"FAILURE","reason":"bad READ_COMPLETE payload"}, addr); return

    # clear the read gate
    dec_reads(dss_name)

    resp = reply_ok()
    sock.sendto(resp, addr); log_send("manager", "REPLY", {"status":"SUCCESS"}, addr)


#this makes sure that there is no active reads
def handle_disk_failure(payload: dict, addr, sock):
    # payload = { "dss": str }
    try:
        dss_name = payload["dss"]
    except Exception:
        resp = reply_fail("bad DISK_FAILURE payload")  #bad payload
        sock.sendto(resp, addr); log_send("manager", "REPLY", {"status":"FAILURE","reason":"bad DISK_FAILURE payload"}, addr); return

    cfg = dss.get(dss_name)
    if not cfg:
        resp = reply_fail("unknown DSS")
        sock.sendto(resp, addr); log_send("manager", "REPLY", {"status":"FAILURE","reason":"unknown DSS"}, addr); return

    # cannot start while READs are in progress for this DSS
    if reads_busy(dss_name):
        resp = reply_fail("cannot start disk-failure: reads in progress")
        sock.sendto(resp, addr); log_send("manager", "REPLY", {"status":"FAILURE","reason":"cannot start disk-failure: reads in progress"}, addr); return

    # also for global critical gate
    if critical_busy():
        resp = reply_fail(f"busy: {current_critical['op']} in progress on {current_critical['dss']}")
        sock.sendto(resp, addr); log_send("manager", "REPLY", {"status":"FAILURE","reason":f"busy: {current_critical['op']} in progress on {current_critical['dss']}"}, addr); return
    start_critical("disk-failure", dss_name)

    eps = dss_endpoints(dss_name)
    if eps is None:
        end_critical()
        resp = reply_fail("DSS endpoints unavailable")
        sock.sendto(resp, addr); log_send("manager", "REPLY", {"status":"FAILURE","reason":"DSS endpoints unavailable"}, addr); return

    # provide params, the user will pick a random disk to fail
    #and then we will rebuild via XOR, then send RECOVERY_COMPLETE
    resp = reply_ok(
        dss=dss_name,
        n=cfg["n"],
        striping_unit=cfg["striping_unit"],
        order=cfg["order"],
        endpoints=eps,
        files=cfg.get("files", [])
    )
    sock.sendto(resp, addr); log_send("manager", "REPLY", {"status":"SUCCESS","dss":dss_name,"n":cfg["n"],"striping_unit":cfg["striping_unit"],"order":cfg["order"],"endpoints":eps,"files":cfg.get("files", [])}, addr)

#clears the critical section when teh users signal they are done reading
def handle_recovery_complete(payload: dict, addr, sock):
    # payload: { "dss": str }
    try:
        dss_name = payload["dss"]
    except Exception:
        resp = reply_fail("bad RECOVERY_COMPLETE payload")
        sock.sendto(resp, addr); log_send("manager", "REPLY", {"status":"FAILURE","reason":"bad RECOVERY_COMPLETE payload"}, addr); return

    # end critical section
    end_critical()

    resp = reply_ok()
    sock.sendto(resp, addr); log_send("manager", "REPLY", {"status":"SUCCESS"}, addr)

#makes sure there are no reads in progress, and there is no other op running in the 
#critical section
def handle_decommission_dss(payload: dict, addr, sock):
    # payload = { "dss": str }
    try:
        dss_name = payload["dss"]
    except Exception:
        resp = reply_fail("bad DECOMMISSION_DSS payload")
        sock.sendto(resp, addr); log_send("manager", "REPLY", {"status":"FAILURE","reason":"bad DECOMMISSION_DSS payload"}, addr); return

    cfg = dss.get(dss_name)
    if not cfg:
        resp = reply_fail("unknown DSS")
        sock.sendto(resp, addr); log_send("manager", "REPLY", {"status":"FAILURE","reason":"unknown DSS"}, addr); return

    # won't be able to decommission if reads are active on this DSS
    if reads_busy(dss_name):
        resp = reply_fail("cannot decommission: reads are in progress")
        sock.sendto(resp, addr); log_send("manager", "REPLY", {"status":"FAILURE","reason":"cannot decommission: reads in progress"}, addr); return

    if critical_busy():
        resp = reply_fail(f"busy: {current_critical['op']} in progress on {current_critical['dss']}")
        sock.sendto(resp, addr); log_send("manager", "REPLY", {"status":"FAILURE","reason":f"busy: {current_critical['op']} in progress on {current_critical['dss']}"}, addr); return
    start_critical("decommission", dss_name)

    #  We'll broadcast a delete-all to each disk c-port.
    #  flip disk states back to Free and clear DSS.
    for disk_name in cfg["order"]:
        if disk_name in disks:
            disks[disk_name]["state"] = "Free" # set the disk states back to free
            disks[disk_name].pop("dss", None)

    # remove the DSS record from the stack 
    dss.pop(dss_name, None)

    # end critical section 
    end_critical()
	
    resp = reply_ok()
    sock.sendto(resp, addr); log_send("manager", "REPLY", {"status":"SUCCESS"}, addr)
    log_info("manager", f"decommissioned DSS {dss_name}")


#Process a REGISTER _USER request that is sent to manager over UDP
#valides the requests, updates manager memory (that we have a new user)
#send a SUCCESS/FAILURE reply 
#expected: user-name, IPv4-addr, m-port, c-port 
def handle_register_user(payload: dict, addr, sock):
   
    #get the information from message payload
    try:
        name = payload["user-name"]
        ip   = payload["IPv4-addr"]
        m    = int(payload["m-port"])
        c    = int(payload["c-port"])
    except Exception:
        resp = reply_fail("did not work register-user")
        sock.sendto(resp, addr); log_send("manager", "REPLY", {"status":"FAILURE","reason":"did not work register-user"}, addr); return

    if not _name_re.match(name): # not a valid user name 
        resp = reply_fail("invalid user-name (alpha, <=15)")
        sock.sendto(resp, addr); log_send("manager", "REPLY", {"status":"FAILURE","reason":"invalid user-name (alpha, <=15)"}, addr); return
    if name in users: # user name exists 
        resp = reply_fail("duplicate user-name")
        sock.sendto(resp, addr); log_send("manager", "REPLY", {"status":"FAILURE","reason":"duplicate user-name"}, addr); return
    if not (valid_port(m) and valid_port(c)): # port not valid 
        resp = reply_fail("port(s) out of group range")
        sock.sendto(resp, addr); log_send("manager", "REPLY", {"status":"FAILURE","reason":"port(s) out of group range"}, addr); return

    # each process needs to go to a unique port 
    #  enforce global uniqueness on (ip,port) to be safe.
    if (ip, m) in used_ports or (ip, c) in used_ports:
        resp = reply_fail("m-port or c-port already in use")
        sock.sendto(resp, addr); log_send("manager", "REPLY", {"status":"FAILURE","reason":"m-port or c-port already in use"}, addr); return
	
    # update thhe state - so the ports are remembered
    users[name] = {"ip": ip, "mport": m, "cport": c}
    used_ports.add((ip, m)); used_ports.add((ip, c))

    #send reply 
    resp = reply_ok()
    sock.sendto(resp, addr); log_send("manager", "REPLY", {"status":"SUCCESS"}, addr)
    log_info("manager", f"users: {users}")  # print out the users

# PROCESS register disk request. Update manager memory that there is a new disk
# Replies with SUCCESS/FAILURE message
def handle_register_disk(payload: dict, addr, sock):
    # disk-name, IPv4-addr, m-port, c-port
    try:
        name = payload["disk-name"]
        ip   = payload["IPv4-addr"]
        m    = int(payload["m-port"])
        c    = int(payload["c-port"])
    except Exception:
        resp = reply_fail("did not work register-disk")
        sock.sendto(resp, addr); log_send("manager", "REPLY", {"status":"FAILURE","reason":"did not work register-disk"}, addr); return

    if not _name_re.match(name):
        resp = reply_fail("invalid disk-name (alpha, <=15)")
        sock.sendto(resp, addr); log_send("manager", "REPLY", {"status":"FAILURE","reason":"invalid disk-name (alpha, <=15)"}, addr); return
    if name in disks:
        resp = reply_fail("duplicate disk-name")
        sock.sendto(resp, addr); log_send("manager", "REPLY", {"status":"FAILURE","reason":"duplicate disk-name"}, addr); return
    if not (valid_port(m) and valid_port(c)):
        resp = reply_fail("port(s) out of group range")
        sock.sendto(resp, addr); log_send("manager", "REPLY", {"status":"FAILURE","reason":"port(s) out of group range"}, addr); return
    if (ip, m) in used_ports or (ip, c) in used_ports:
        resp = reply_fail("m-port or c-port already in use")
        sock.sendto(resp, addr); log_send("manager", "REPLY", {"status":"FAILURE","reason":"m-port or c-port already in use"}, addr); return

    disks[name] = {"ip": ip, "mport": m, "cport": c, "state": "Free"}
    used_ports.add((ip, m)); used_ports.add((ip, c))

    resp = reply_ok()
    sock.sendto(resp, addr); log_send("manager", "REPLY", {"status":"SUCCESS"}, addr)
    log_info("manager", f"disks: {disks}")

#Validate user input. Chooses disks and update the stae to "InDSS to show that 
# the disks are no longer free. 
# Replies with either success of fail. 
def handle_configure_dss(payload: dict, addr, sock):
    #  dss-name, n, striping-unit
    try:
        name = payload["dss-name"]
        n    = int(payload["n"])
        su   = int(payload["striping-unit"])
    except Exception:
        resp = reply_fail("did  not work - configure-dss")
        sock.sendto(resp, addr); log_send("manager", "REPLY", {"status":"FAILURE","reason":"did  not work - configure-dss"}, addr); return

    # spec validations
    if not _name_re.match(name):
        resp = reply_fail("invalid dss-name (alpha, <=15)")
        sock.sendto(resp, addr); log_send("manager", "REPLY", {"status":"FAILURE","reason":"invalid dss-name (alpha, <=15)"}, addr); return
    if name in dss:
        resp = reply_fail("dss-name already exists")
        sock.sendto(resp, addr); log_send("manager", "REPLY", {"status":"FAILURE","reason":"dss-name already exists"}, addr); return
    if n < 3:
        resp = reply_fail("n must be >= 3")
        sock.sendto(resp, addr); log_send("manager", "REPLY", {"status":"FAILURE","reason":"n must be >= 3"}, addr); return
    if not (STRIPE_MIN <= su <= STRIPE_MAX and is_power_of_two(su)):
        resp = reply_fail("striping unit must be a power of two in [128,1MB]")
        sock.sendto(resp, addr); log_send("manager", "REPLY", {"status":"FAILURE","reason":"striping unit must be a power of two in [128,1MB]"}, addr); return

    # gather Free disks
    free_disks = [dn for dn, info in disks.items() if info.get("state") == "Free"]
    if len(free_disks) < n:
        resp = reply_fail("not enough Free disks")
        sock.sendto(resp, addr); log_send("manager", "REPLY", {"status":"FAILURE","reason":"not enough Free disks"}, addr); return

    # pick n at random and lock them in an order
    chosen = random.sample(free_disks, n)  # random order returned by sample is fine

    # update disk state and tag with DSS name (handy later)
    for dn in chosen:
        disks[dn]["state"] = "InDSS"
        disks[dn]["dss"] = name

    # record DSS config
    dss[name] = {"n": n, "striping_unit": su, "order": chosen, "files": []}
    log_info("manager", f"configured DSS {name}: n={n}, su={su}, order={chosen}")

    # include config in the reply payload (useful for your demo trace)
    resp = reply_ok(dss=name, n=n, striping_unit=su, order=chosen)
    sock.sendto(resp, addr); log_send("manager", "REPLY", {"status":"SUCCESS","dss":name,"n":n,"striping_unit":su,"order":chosen}, addr)
 
#handle ls message 
#payload = data sent by the client, addr - clients IP and port so we know where to reply
#sock the UDP socket that the manager is listening on. 
def handle_ls(payload: dict, addr, sock):

    # A list of dictionaries 
    #remember that DSS holds all the configured DSS's
    summary = []
    for name, cfg in dss.items():
    	#append all the info to the dict to print nice summary
        summary.append({
            "dss": name,
            "n": cfg.get("n"),
            "striping_unit": cfg.get("striping_unit"),
            "order": cfg.get("order"),
            "files": cfg.get("files", []),   # list of {name,size,owner}
        })
    resp = reply_ok(dss_summary=summary)  #successs reply 
    sock.sendto(resp, addr); log_send("manager", "REPLY", {"status":"SUCCESS","dss_summary":summary}, addr)
   
#to deregister a user  - removes from user ds   
def handle_deregister_user(payload: dict, addr, sock):
    try:
        name = payload["user-name"]
    except Exception:
        resp = reply_fail("not right try again, deregister-user")
        sock.sendto(resp, addr); log_send("manager", "REPLY", {"status":"FAILURE","reason":"not right try again, deregister-user"}, addr); return

    info = users.pop(name, None) # take it out of the users 
    if not info:
        resp = reply_fail("that user-name is no in here")
        sock.sendto(resp, addr); log_send("manager", "REPLY", {"status":"FAILURE","reason":"that user-name is no in here"}, addr); return
    # release the ports that that user was using. 
    release_ports(info["ip"], info["mport"], info["cport"])
    resp = reply_ok() # update that ports are released 
    sock.sendto(resp, addr); log_send("manager", "REPLY", {"status":"SUCCESS"}, addr)
    log_info("manager", f"users: {users}")

#To deregister a disk  - removes from disk ds - state must be FREE
def handle_deregister_disk(payload: dict, addr, sock):
    try:
        name = payload["disk-name"]
    except Exception:
        resp = reply_fail("not correct deregister-disk")
        sock.sendto(resp, addr); log_send("manager", "REPLY", {"status":"FAILURE","reason":"not correct deregister-disk"}, addr); return
	
    info = disks.get(name)
    if not info:
        resp = reply_fail("unknown disk-name")
        sock.sendto(resp, addr); log_send("manager", "REPLY", {"status":"FAILURE","reason":"unknown disk-name"}, addr); return
    if info.get("state") != "Free": #if the disk is has storage on it you cannot just 
    #deregister it 
        resp = reply_fail("disk is in DSS; cannot deregister")
        sock.sendto(resp, addr); log_send("manager", "REPLY", {"status":"FAILURE","reason":"disk is in DSS; cannot deregister"}, addr); return
    #remove it from data structure
    disks.pop(name)
    release_ports(info["ip"], info["mport"], info["cport"])
    resp = reply_ok()
    sock.sendto(resp, addr); log_send("manager", "REPLY", {"status":"SUCCESS"}, addr)
    log_info("manager", f"disks: {disks}")


def main():
    #parse out the command line arguments
    if len(sys.argv) != 2:
        print("usage: python3 manager.py <manager_m_port>")
        sys.exit(1)

    try:
        PORT = int(sys.argv[1])
    except ValueError:
        print("manager port must be an integer")
        sys.exit(1)

    if not valid_port(PORT):
        print(f"manager port must be in [{PORT_MIN},{PORT_MAX}] for Group 57")
        sys.exit(1)

    # start up the UDP server 
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("0.0.0.0", PORT))
        log_info("manager", f"listening on 0.0.0.0:{PORT} (UDP)")

        #keep listenting 
        while True:
            data, addr = sock.recvfrom(65535)
            mtype, payload = decode(data)
            log_recv("manager", mtype.name, payload, addr)
			
            #handle the requests for each message type
            if mtype == MsgType.PING:
                resp = encode(MsgType.PONG, seq=payload.get("seq", 0))
                sock.sendto(resp, addr); log_send("manager", "PONG", {"seq": payload.get("seq", 0)}, addr)
            elif mtype == MsgType.REGISTER_USER:  # if we are registering a user
                handle_register_user(payload, addr, sock)
            elif mtype == MsgType.REGISTER_DISK:  # if we are registering a disk
                handle_register_disk(payload, addr, sock) # if we are configuring a DSS
            elif mtype == MsgType.CONFIGURE_DSS:
                handle_configure_dss(payload, addr, sock)
            elif mtype == MsgType.DEREGISTER_USER:
                handle_deregister_user(payload, addr, sock)
            elif mtype == MsgType.DEREGISTER_DISK:
                handle_deregister_disk(payload, addr, sock)
            elif mtype == MsgType.LS:
            	handle_ls(payload, addr, sock)
            elif mtype == MsgType.COPY:
            	handle_copy(payload, addr, sock)
            elif mtype == MsgType.COPY_COMPLETE:
            	handle_copy_complete(payload, addr, sock)
            elif mtype == MsgType.READ:
            	handle_read(payload, addr, sock)
            elif mtype == MsgType.READ_COMPLETE:
            	handle_read_complete(payload, addr, sock)
            elif mtype == MsgType.DISK_FAILURE:
            	handle_disk_failure(payload, addr, sock)
            elif mtype == MsgType.RECOVERY_COMPLETE:
            	handle_recovery_complete(payload, addr, sock)
            elif mtype == MsgType.DECOMMISSION_DSS:
            	handle_decommission_dss(payload, addr, sock) 
            else:
                resp = reply_fail("unsupported command")
                sock.sendto(resp, addr); log_send("manager", "REPLY", {"status":"FAILURE","reason":"unsupported command"}, addr)

#[128, 1MB]
STRIPE_MIN = 128
STRIPE_MAX = 1024 * 1024 # 1 MB of data

def is_power_of_two(x: int) -> bool:
    return x > 0 and (x & (x - 1)) == 0

#helper function to release ports 
def release_ports(ip: str, m: int, c: int) -> None:
    used_ports.discard((ip,m)) # free the used ports
    used_ports.discard((ip,c))

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log_info("manager", "goodbye")
