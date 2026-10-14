# dss/disk.py
import socket, sys
import threading 
from messages import encode, decode, MsgType, SUCCESS
# to use our defined trace messages system - keep track of where the threads are coming from
from trace import log_send, log_recv, log_info
import base64
# usage: python3 disk.py <manager_ip> <manager_m_port> <disk-name> <my_ip> <my_m_port> <my_c_port>
# example: python3 disk.py 127.0.0.1 29500 D1 127.0.0.1 29511 29512



# we will now add a UDP server for the data plane, that it is listning on the C ports
#to move the actual  file data in the peer to peer communication between the user and the disk 
# this happen on UDP server on the C-Ports

#to keep track of the in memory blocks
# how this DSS will work: (dss_name, file_name, stripe_idx:int, disk_idx:int) ->> bytes
# need an index for the bloks so that the disks knows where to access them. 
store = {}
failed = False


#This defines the cport listener - user to disk communication - data plane message are :
#  STORE_BLOCK / STORE_BLOCK_ACK
# READ_BLOCK / READ_BLOCK_REPLY
# FAIL_DISK
# DELETE_ALL
# - WRITE_RECOVERED_BLOCK
# - for testing maybe PING -> PONG
# the socket will loop for ever 
def cport_listener(my_ip: str, my_c: int, disk_name: str):

    proc = f"disk:{disk_name}-cport"
    #create the udp socket for the Cport
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.bind((my_ip, my_c)) # bind to address and port 
        log_info(proc, f"listening on {my_ip}:{my_c} (UDP)") # log the nec info. 
        # keep looping while getting blocks 
        while True:
            data, addr = sock.recvfrom(65535)
            mtype, payload = decode(data)
            log_recv(proc, mtype.name, payload, addr)

            #  helper to send a failure reply with logging
            def send_fail(reason: str):
                resp = encode(MsgType.REPLY, status="FAILURE", reason=reason)
                sock.sendto(resp, addr)
                log_send(proc, "REPLY", {"status": "FAILURE", "reason": reason}, addr)

            #  helper to send a success reply with logging taht will help in debug 
            def send_ok(extra: dict | None = None):
                body = {"status": "SUCCESS"}
                if extra:
                    body.update(extra)
                resp = encode(MsgType.REPLY, **body)
                sock.sendto(resp, addr)
                log_send(proc, "REPLY", body, addr)

            if mtype == MsgType.PING:
                # test with a ping - can we even reach ?
                resp = encode(MsgType.PONG, **payload)
                sock.sendto(resp, addr)
                log_send(proc, "PONG", payload, addr)

			# message to write a block 
            elif mtype == MsgType.STORE_BLOCK:
                # payload =  { dss, file, stripe, disk_idx, data }
                global failed
                if failed:
                    send_fail("disk has failed"); continue
                try:
                    dss = payload["dss"]
                    fn = payload["file"]
                    s = int(payload["stripe"]) #srtipe 
                    di = int(payload["disk_idx"])  # the disk index 
                    # We encode raw bytes with baes 64 to travel inside JSON safely
                    raw = payload.get("data", "")
                    blk = base64.b64decode(raw.encode("ascii"))
                except Exception:
                    send_fail("wrong STORE_BLOCK payload"); continue

                store[(dss, fn, s, di)] = blk
                # Acknowledge with an ACK type for easier 
                ack = encode(MsgType.STORE_BLOCK_ACK, ok=1) 
                sock.sendto(ack, addr)
                log_send(proc, "STORE_BLOCK_ACK", {"ok": 1}, addr)
			#Message to read a block of data 
            elif mtype == MsgType.READ_BLOCK: 
                # payload =  { dss, file, stripe, disk_idx }
                if failed:
                    send_fail("disk failed"); continue
                try:
                    dss = payload["dss"]
                    fn = payload["file"]
                    s = int(payload["stripe"])
                    di = int(payload["disk_idx"])
                except Exception:
                    send_fail("bad READ_BLOCK payload"); continue
				#get the block from store data structure 
                blk = store.get((dss, fn, s, di))
                if blk is None:
                    #The block has not been found 
                    nbr = encode(MsgType.READ_BLOCK_REPLY, found=0)
                    sock.sendto(nbr, addr)
                    log_send(proc, "READ_BLOCK_REPLY", {"found": 0}, addr)
                else:
                    # send bytes back as base64  string
                    r = encode(MsgType.READ_BLOCK_REPLY, found=1, data=base64.b64encode(blk).decode("ascii"))
                    sock.sendto(r, addr)
                    log_send(proc, "READ_BLOCK_REPLY", {"found": 1, "len": len(blk)}, addr)
			# if there is a failed disk we need to wipte the block and set it as failed
            elif mtype == MsgType.FAIL_DISK:
                store.clear()
                failed = True
                send_ok()
			#Delete everything from the storgage 
            elif mtype == MsgType.DELETE_ALL:
                # wipe everything but remain healthy
                store.clear()
                # we are still healthy - user chose to delete 
                failed = False
                send_ok()
			# IF there is a missing block rebuild it 
            elif mtype == MsgType.WRITE_RECOVERED_BLOCK:
                # payload = { dss, file, stripe, disk_idx, data }
                try:
                    dss = payload["dss"]
                    fn = payload["file"]
                    s = int(payload["stripe"])
                    di = int(payload["disk_idx"])
                    raw = payload.get("data", "")
                    blk = base64.b64decode(raw.encode("ascii"))
                except Exception:
                    send_fail("bad WRITE_RECOVERED_BLOCK payload"); continue

                if failed:
                    # Policy choice, reject writes
                    # while failed, user should rebuild then optionally unfail.
                    send_fail("disk failed"); continue

                store[(dss, fn, s, di)] = blk #store the block
                send_ok()  # send ack 

            else:
                send_fail("unsupported on c-port")


def main():
    if len(sys.argv) != 7:
        print("usage: python -m dss.disk <manager_ip> <manager_m_port> <disk-name> <my_ip> <my_m_port> <my_c_port>")
        sys.exit(1)

	# parseses the CLI input 
    mgr_ip   = sys.argv[1]   # ip address for manager 
    mgr_port = int(sys.argv[2])  # m port for manager
    name     = sys.argv[3]  # the name of the disk 
    my_ip    = sys.argv[4] # the ip address of the machine where peer is running 
    my_m     = int(sys.argv[5]) # used for manger to peer communication 
    my_c     = int(sys.argv[6]) # used for peet to peer communication 
    t = threading.Thread(target=cport_listener, args=(my_ip, my_c, name), name=f"{name}-cport", 
    daemon=True)
    t.start()

    payload = {
        "disk-name": name,
        "IPv4-addr": my_ip,
        "m-port": my_m,
        "c-port": my_c,
    }
	# create the udp socket 
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.settimeout(2.0)
        log_send(f"disk:{name}", "REGISTER_DISK", payload, (mgr_ip, mgr_port))
        sock.sendto(encode(MsgType.REGISTER_DISK, **payload), (mgr_ip, mgr_port))
        try:
            data, addr = sock.recvfrom(4096)  # receive data from 
        except socket.timeout:
            log_info(f"disk:{name}", "!!! There has been no response -timeout"); return

        mtype, resp = decode(data)
        #print out messages
        log_recv(f"disk:{name}", mtype.name, resp, addr)
        if mtype.name == "REPLY" and resp.get("status") == SUCCESS:
            log_info(f"disk:{name}", "SUCCESS: registered (state Free)")
        else:
            log_info(f"disk:{name}", f"FAILURE: {resp.get('reason')}")

    # Need Keep the disk process alive to continue responding to cport requests
    # Join the c port thread.
    t.join()


if __name__ == "__main__":
    main()

