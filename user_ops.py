# user_operations.py
import argparse, os, socket, sys, math, random
from concurrent.futures import ThreadPoolExecutor, as_completed
from messages import encode, decode, MsgType, SUCCESS
from trace import log_send, log_recv, log_info
import socket, errno, time
import base64


#This talks to the manager to get parameters and performs the P2P comms 
#to disks over the c-port
#It will notify the manager when the ops are complete 
#computes parity and injects error for demo

MAX_UDP = 65507
#Max size that a block can be 
MAX_BLOCK = 60000 #less for safety 

#Helper method to perform parity - computes the bitwise XOR across a length of bytes blocks
#If you loose a block - recover it XORing all the other blocks  
def xorb(*chunks: bytes) -> bytes:
    if not chunks: return b""
    out = bytearray(chunks[0])
    for c in chunks[1:]:
        for i in range(len(out)):
            out[i] ^= c[i]
    return bytes(out)
    
#helper method to shorten the data message so it is more readable in the CLI
def _shorten(val, limit=64):
    s = str(val)
    return (s[:limit] + '…') if len(s) > limit else s
    
#Opens a UDP socket that logs the messages to the managers m-port
#wait for a reply 
def send_mgr(ip, port, mtype, payload, timeout=2.5):
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.settimeout(timeout) #we don't want this open too long 
        log_send("user_ops", mtype.value, payload, (ip, port)) #user ups is the thread
        s.sendto(encode(mtype, **payload), (ip, port))
        data, addr = s.recvfrom(65535)
        m2, resp = decode(data)
        log_recv("user_ops", m2.name, resp, addr)  #the response payload
        return m2, resp

#Opens a UDP socket to communicate with the disks c-port 
#The data plane call - P@P transer between user and disks 
def send_disk(ip, cport, mtype, payload, timeout=2.0, retries=1):
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.settimeout(timeout)
        data = encode(mtype, **payload)
        for attempt in range(retries + 1):
            try:
                # log (truncated) to avoid printing 4KB of random bytes
                log_send("userops", mtype.name, {**payload, "data": _shorten(payload.get("data", ""))}, (ip, cport))
                s.sendto(data, (ip, cport))
                resp, addr = s.recvfrom(65535)
                m2, r = decode(resp)
                log_recv("userops", m2.name, r, addr)
                return m2, r
            except OSError as e:
                if e.errno == errno.EMSGSIZE and attempt < retries:
                    # Fallback: re-encode even smaller (optional), or split (not needed with 4KB blocks),
                    # or just retry once (some stacks fail on first try sporadically).
                    time.sleep(0.01)
                    continue
                raise
            except socket.timeout:
                if attempt < retries:
                    time.sleep(0.05)
                    continue
                raise
#coordinate with the manager that we want to a copy file (name, size, owner)
#manager will make sure critical section is not busy, picks a DSS
#respond with the layout of that dss 
#this is so we know which disks (the endpoints of the disk) that we can use 
#After data is finished being copied we must sent COPY_COMPLETE to indicate this 
def do_copy(mgr_ip, mgr_port, file_path, owner):

	#Tell the manager that we want to do a copy message type 
    size = os.path.getsize(file_path)
    #sent over the m ports (the control plane)
    m, r = send_mgr(mgr_ip, mgr_port, MsgType.COPY, {
        "file-name": os.path.basename(file_path), #name of file 
        "file-size": size, # size of files
        "owner": owner 
    })
    if r.get("status") != SUCCESS: return

	#su (striping unit) is the block size
	#our dss block we will be using and it's endpoints 
    dss = r["dss"]; n = r["n"]; su = r["striping_unit"]; order = r["order"]; eps = r["endpoints"]
	#size of the block - keep it <60kb 
    b = min(int(su), MAX_BLOCK) # make sure it is not more than the max block 

    # read from the file and slice into stripes of n-1 data blocks 
    #these will be of size b, compute above
    with open(file_path, "rb") as f:
        stripe_idx = 0
        while True:
            data_blocks = []
            bytes_read_this_stripe = 0  # track real bytes read this stripe before padding

            # read up to n-1 data blocks for this stripe
            for _ in range(n-1):
                chunk = f.read(b)

                # if EOF and nothing read yet for this stripe then done
                if not chunk and _ == 0:
                    bytes_read_this_stripe = 0
                    break

                # after some data has been read in this stripe, empty reads become padding
                if not chunk:
                    chunk = b""

                data_blocks.append(chunk)
                bytes_read_this_stripe += len(chunk)

                #if the data chunk is less then out block size meaning ,the file has ended 
                if len(chunk) < b:
                    # pad the last data block to fill up to b with A's
                    data_blocks[-1] = data_blocks[-1] + b"\x00" * (b - len(chunk))
                    # and pad the lasts data blocks if any zeros
                    for __ in range(_+1, n-1):
                        data_blocks.append(b"\x00"*b)
                    break

            # stop if this stripe read 0 *real* bytes (don’t send all-zero stripes forever)
            if bytes_read_this_stripe == 0:
                break

            # ensure there are exactly n-1 blocks, each length b
            while len(data_blocks) < n-1:
                data_blocks.append(b"\x00"*b)

            # compute the parity 
            parity = xorb(*data_blocks)
            # rotate the parity's position
            # moves from disk 0 to disk n-1 as stripe_idx increases, this is where we 
            #are distributing the parity evenly across the disks
            parity_pos = stripe_idx % n

            # build block list length n where position parity_pos is parity the
            # others are data in order
            blocks = []
            di_data = 0
            for di in range(n):
                if di == parity_pos:
                    blocks.append(parity)
                else:
                    blocks.append(data_blocks[di_data]) 
                    di_data += 1

            #This is so all blocks sneds for this stripe happen concurrently 
            #this is the peer to peer data movements through the disks c-port
            #Need to send all n blocks in parallel
            with ThreadPoolExecutor(max_workers=n) as ex:
                futs = []
                #for each disk (di) we will build a STORE_BLOCK payload
                #note eps are the disks endpoints that we got from the DSS from the manager
                #it comes from the manager reply payload
                #can think of it is the set of network endpoint for all of the disks that are going to be in this 
                #stripe 
                #di is disk index - send to every disks c -port 
                for di, (disk_name, ip, cport) in enumerate(eps):
                    payload = {
                        "dss": dss,
                        "file": os.path.basename(file_path),
                        "stripe": stripe_idx,
                        "disk_idx": di,
                        # base64 encode block bytes so JSON stays small and UDP - had issues
                        "data": base64.b64encode(blocks[di]).decode("ascii") if blocks[di] else ""
                    }
                    #send this payload to the disk 
                    futs.append(ex.submit(send_disk, ip, cport, MsgType.STORE_BLOCK, payload))
                for f_ in as_completed(futs):
                    m2, rr = f_.result()
                    if m2.name not in ("STORE_BLOCK_ACK","REPLY") or rr.get("ok")==0:
                        log_info("userops", f"STORE failed: {rr}")
            stripe_idx += 1

    # Tell the managert we are done with the copy 
    send_mgr(mgr_ip, mgr_port, MsgType.COPY_COMPLETE, {
        "dss": dss,
        "file-name": os.path.basename(file_path),
        "file-size": size,
        "owner": owner
    })


#Do read message from the disk 
def do_read(mgr_ip, mgr_port, dss, file_name, user_name, out_path, p_error=0):

	#ask the manager for READ message type - give th enumber of disks the striping unit
	#the endpoints of the DSS for P2P comms 
    m, r = send_mgr(mgr_ip, mgr_port, MsgType.READ, {
        "dss": dss, "file-name": file_name, "user-name": user_name
    })
    if r.get("status") != SUCCESS: return
    n = r["n"]; b = min(int(r["striping_unit"]), MAX_BLOCK); eps = r["endpoints"]; fsize = int(r["file_size"])
	#calculate how many stripes to read from 
	#each stripe should have n-1 data blocks that are size b and then 1 parity block
    stripes = math.ceil(fsize / ( (n-1) * b )) if fsize>0 else 0
    #open the output file to stream so that we can reconstruct all the data from the disks 

    out = open(out_path, "wb") # out path is where we will write the data blocks to 
    try:
    	#for each stripe we need to read n blocks in parallel from the data plane(that is the disks)
    	#n concurrent READ_BLOCK requres - one per disk for to get the current stripe and the disks_idx
        for s in range(stripes):
            #  in parallel
            blocks = [None]*n
            with ThreadPoolExecutor(max_workers=n) as ex:
                # FIX: keep a mapping from Future to disk index (di) so we store results
                # in the correct slot regardless of completion order.
                fut_to_di = {}
                for di, (disk_name, ip, cport) in enumerate(eps):
                    payload = {"dss": dss, "file": file_name, "stripe": s, "disk_idx": di}
                    fut = ex.submit(send_disk, ip, cport, MsgType.READ_BLOCK, payload)
                    fut_to_di[fut] = di
                # as_completed yields futures as they finish
                for fut in as_completed(fut_to_di):
                    di = fut_to_di[fut]  # original disk index for this future
                    m2, rr = fut.result()
                    if rr.get("found")==1:
                        blk = base64.b64decode(rr["data"].encode("ascii"))
                        blocks[di] = blk
                    else:
                        blocks[di] = None

            # with probability p error flip one random bit in a random non None block
            if p_error>0 and random.randint(1,100) <= p_error:
                candidates = [i for i,bk in enumerate(blocks) if bk]
                if candidates:
                    i = random.choice(candidates)
                    ba = bytearray(blocks[i])
                    ba[0] ^= 0x01  # flip bit , the first 0 in byte 
                    blocks[i] = bytes(ba)

            # get the parity positions s mod n 
            parity_pos = s % n
            data_blks = [blocks[i] for i in range(n) if i != parity_pos] # this has the n-1 data blocks
            # if we are missing a blok, reconstruct it with our XOR method
            if None in data_blks:
                # reconstruct
                missing_idx = blocks.index(None) # get missing block 
                present = [bk for bk in blocks if bk is not None]
                rec = xorb(*present)
                # if missing is the parity that is fine?
                # if missing is data at di, assign rec to that slot
                blocks[missing_idx] = rec
                # reassemble data_blks after reconstruction
                data_blks = [blocks[i] for i in range(n) if i != parity_pos]

            # check that the parity matches
            parity = blocks[parity_pos]
            if parity != xorb(*data_blks):
                # parity mismatch: re read once
    			#computes xor(*data_blks ) - compared with partiyf - if they are differeent 
    			#there mush have been something wrong and we re read the while stripe once
                data_blks = []
                with ThreadPoolExecutor(max_workers=n) as ex:
                    # PCH (again): use fut -> di mapping during the re read branch too.
                    fut_to_di = {}
                    for di, (disk_name, ip, cport) in enumerate(eps):
                        payload = {"dss": dss, "file": file_name, "stripe": s, "disk_idx": di}
                        fut = ex.submit(send_disk, ip, cport, MsgType.READ_BLOCK, payload)
                        fut_to_di[fut] = di
                    blocks = [None]*n
                    for fut in as_completed(fut_to_di):
                        di = fut_to_di[fut]
                        m2, rr = fut.result()
                        if rr.get("found")==1:
                            blocks[di] = base64.b64decode(rr["data"].encode("ascii"))
                parity_pos = s % n
                data_blks = [blocks[i] for i in range(n) if i != parity_pos]

            # now lets write the blocks
            #write only the n-1 data blocks trim the very last padding
            for db in data_blks:
                out.write(db)

        # trim  the file to original size remove padding
        out.flush()
        out.seek(0, os.SEEK_END)
        if out.tell() > fsize:
            out.truncate(fsize)
    #SEnd completion signal to the manager 
    #two phase READ protocol 
    finally:
        out.close()
        send_mgr(mgr_ip, mgr_port, MsgType.READ_COMPLETE, {
            "dss": dss, "file-name": file_name, "user-name": user_name
        })


#Handle disk failure 
def do_disk_failure(mgr_ip, mgr_port, dss):
	#establish connection with the manager that you are doing a disk failure 
    m, r = send_mgr(mgr_ip, mgr_port, MsgType.DISK_FAILURE, {"dss": dss})
    if r.get("status") != SUCCESS: return #comm is good 
    #return the DSS with end points and use b as the striping unit 
    n = r["n"]; b = min(int(r["striping_unit"]), MAX_BLOCK); eps = r["endpoints"]; files = r.get("files", [])
    if not files:
        # there is nothing to rebuild the 
        send_mgr(mgr_ip, mgr_port, MsgType.RECOVERY_COMPLETE, {"dss": dss}); return

    # chooses and random disk to fail 
    fail_i = random.randrange(n)
    fail_name, fail_ip, fail_cport = eps[fail_i]
    # tell the managet to fail the disk 
    send_disk(fail_ip, fail_cport, MsgType.FAIL_DISK, {})

    # rebuiild teh file 
    for meta in files:  #metedata
        fname = meta["name"]; fsize = int(meta["size"])
        stripes = math.ceil(fsize / ((n-1)*b)) if fsize>0 else 0 #comute stripes (n-1)
        for s in range(stripes): # read n-1 blocks 
            # read the other n-1 blocks
            present = []
            for di, (disk_name, ip, cport) in enumerate(eps):
                if di == fail_i: continue
                m2, rr = send_disk(ip, cport, MsgType.READ_BLOCK, {
                    "dss": dss, "file": fname, "stripe": s, "disk_idx": di
                })
                if rr.get("found")==1:
                    present.append(base64.b64decode(rr["data"].encode("ascii")))
            # use  XOR to reconstruct the missing block
            rec = xorb(*present)
            # use WRITE_RECOVERED_BLOCK messat to write back to failed disk
            send_disk(fail_ip, fail_cport, MsgType.WRITE_RECOVERED_BLOCK, {
                "dss": dss, "file": fname, "stripe": s, "disk_idx": fail_i,
                "data": base64.b64encode(rec).decode("ascii")
            })

    # send that the recovery has been complete
    send_mgr(mgr_ip, mgr_port, MsgType.RECOVERY_COMPLETE, {"dss": dss})
  
 
 #builds the CLI for user P2P operatsion 
 #needs mgr_ip where the manager listens)and port    
def main():
    ap = argparse.ArgumentParser(description="User P2P operations (copy/read/disk-failure)")
    ap.add_argument("mgr_ip"); ap.add_argument("mgr_port", type=int)
    sub = ap.add_subparsers(dest="cmd", required=True)
	
	#list of commands that can be accepted 
    p_copy = sub.add_parser("copy")
    p_copy.add_argument("--file", required=True) #give path to the local file 
    p_copy.add_argument("--owner", required=True) # give the username of the person who owns the file
    #ownership of the file gibven 

    p_read = sub.add_parser("read")
    p_read.add_argument("--dss", required=True) # the DSS to read from 
    p_read.add_argument("--file-name", required=True) # file to be read from 
    p_read.add_argument("--user-name", required=True) # has to have write file owner 
    p_read.add_argument("--out", required=True) #where should reconstructed file be written 
    p_read.add_argument("--p-error", type=int, default=0) # probablity of error single bit error
    
	#for disk failure command - simulating a failure 
    p_df = sub.add_parser("disk-failure") 
    p_df.add_argument("--dss", required=True) # what dss should have the failed disk 

	#what functions to send the disk  
    args = ap.parse_args()
    if args.cmd == "copy":
        do_copy(args.mgr_ip, args.mgr_port, args.file, args.owner)
    elif args.cmd == "read":
        do_read(args.mgr_ip, args.mgr_port, args.dss, args.file_name, args.user_name, args.out, args.p_error)
    elif args.cmd == "disk-failure":
        do_disk_failure(args.mgr_ip, args.mgr_port, args.dss)

if __name__ == "__main__":
    main()
