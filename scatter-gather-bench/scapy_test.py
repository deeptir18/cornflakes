import scapy
from scapy.all import *

client_mac = "0c:42:a1:dd:5f:e4"
server_mac = "0c:42:a1:dd:5e:a4"
client_ip = "193.168.1.2"
server_ip = "193.168.1.1"

p = Ether(src=client_mac, dst=server_mac)/IP(src=client_ip, dst=server_ip)/UDP(sport=50000, dport=50000)/Raw(load='123456789012345678901234')
sendp(p, iface="ens1f0np0")
