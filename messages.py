# messages.py
# can put helper function to help with encoding and decoding 
# messages.py

from __future__ import annotations
from dataclasses import dataclass
from enum import Enum # for pint and pong 
import json
from typing import Any, Dict, Tuple

# Max payload for UDP 
MAX_UDP_PAYLOAD = 65507   


#define our message types, they wil
class MsgType(str, Enum):
    # Ping, Pong Error - basic message types 
    PING = "PING"
    PONG = "PONG"
    ERROR = "ERROR"
    
    # All message types to communicate with the manager
    REGISTER_USER = "REGISTER_USER"
    REGISTER_DISK = "REGISTER_DISK"
    DEREGISTER_USER = "DEREGISTER_USER"
    DEREGISTER_DISK = "DEREGISTER_DISK"
    CONFIGURE_DSS = "CONFIGURE_DSS"
    REPLY = "REPLY"   # default reply with status/extra 
    LS = "LS" 
    COPY = "COPY"
    COPY_COMPLETE = "COPY_COMPLETE"  #metadata finalize
    READ = "READ"
    READ_COMPLETE = "READ_COMPLETE" 
    DISK_FAILURE = "DISK_FAILURE"
    RECOVERY_COMPLETE = "RECOVERY_COMPLETE"
    DECOMMISSION_DSS = "DECOMMISSION_DSS"
    #add Peer to Peer message types
    STORE_BLOCK = "STORE_BLOCK"          # writes one block
    STORE_BLOCK_ACK = "STORE_BLOCK_ACK"  # ack the write
    READ_BLOCK = "READ_BLOCK"            # request a block
    READ_BLOCK_REPLY = "READ_BLOCK_REPLY" # returns a block or if a block is not found
    FAIL_DISK = "FAIL_DISK"              # wipe the disk - set it as failed 
    DELETE_ALL = "DELETE_ALL"            # wipe the disk but it is healthy?
    WRITE_RECOVERED_BLOCK = "WRITE_RECOVERED_BLOCK"  # rebuild a block that went wrong. 
    
SUCCESS= "SUCCESS"
FAILURE= "FAILURE"

	
@dataclass
#define how we want out message to look - use JSON formatting. 
#{“type”: <”MSG_TYPE>”, “payload”: {dict} }
class Message:
    """ Using JSON encoded message for UDP."""
    type: MsgType 
    payload: Dict[str, Any] | None = None
	
	
	#need convert the object to bytes to send 
    def to_bytes(self) -> bytes:
        obj = {
            "type": self.type.value,
            "payload": self.payload or {}
        }
        #convert it to a JSON string 
        b = json.dumps(obj, separators=(",", ":")).encode("utf-8")
        if len(b) > MAX_UDP_PAYLOAD:
            # just in case the message is too big 
            raise ValueError("Message is too big for a single UDP datagram")
        return b

	# takes the bytes from the socket and formats them back into JSON
    @staticmethod
    def from_bytes(data: bytes) -> "Message":
        obj = json.loads(data.decode("utf-8"))
        mtype = MsgType(obj["type"])
        payload = obj.get("payload") or {}
        return Message(type=mtype, payload=payload)

# Helper method to encode and to decode 
def encode(mtype: MsgType, **payload: Any) -> bytes:
    return Message(type=mtype, payload=payload).to_bytes()

def decode(data: bytes) -> Tuple[MsgType, Dict[str, Any]]:
    msg = Message.from_bytes(data)
    return msg.type, (msg.payload or {})

#added helper methods
def reply_ok(**payload: Any) -> bytes:
    return encode(MsgType.REPLY, status=SUCCESS, **payload)

def reply_fail(reason: str, **payload: Any) -> bytes:
    return encode(MsgType.REPLY, status=FAILURE, reason=reason, **payload)
