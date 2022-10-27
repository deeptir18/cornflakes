import scapy
from scapy.all import *

client_mac = "1c:34:da:41:ca:c4"
server_mac = "1c:34:da:41:c7:4c"
client_ip = "128.110.218.245"
server_ip = "128.110.218.241"

p = Ether(src=client_mac, dst=server_mac)/IP(src=client_ip, dst=server_ip)/UDP(sport=5000, dport=50001)/Raw(load='123456789012345678901234')
sendp(p, iface="enp65s0f0")
