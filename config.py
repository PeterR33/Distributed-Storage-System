# config.py
#standardize what port we can choose from
#because I am in an odd group (57) it must be port between 29500 and 29999
from typing import Tuple

GROUP = 57  #group nm 

def port_window_for_group(g: int) -> Tuple[int, int]:
    """  For odd groups, the window is [ceil(g/2)*1000+500, ceil(g/2)*1000+999]."""
    # ceil(57/2)=29 → [29500, 29999]
    upper_thousand = (g + 1) // 2  # ceil for ints
    lo = upper_thousand * 1000 + 500
    hi = upper_thousand * 1000 + 999
    return lo, hi

PORT_MIN, PORT_MAX = port_window_for_group(GROUP)


# get the valid ports 
def valid_port(p: int) -> bool:
    return PORT_MIN <= p <= PORT_MAX
