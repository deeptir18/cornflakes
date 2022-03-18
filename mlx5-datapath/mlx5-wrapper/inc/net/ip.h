/*-
 * Copyright (c) 1982, 1986, 1993
 *	The Regents of the University of California.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 4. Neither the name of the University nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 *
 *	@(#)ip.h	8.2 (Berkeley) 6/1/94
 * $FreeBSD$
 */

#pragma once

#include <base/types.h>
#include <base/byteorder.h>
#include <netinet/in.h>
#include <base/debug.h>

/*
 * Definitions for internet protocol version 4.
 *
 * Per RFC 791, September 1981.
 */
#define IHL_NO_OPTIONS 5
#define	IPVERSION	4
#define VERSION_IHL (IPVERSION << 4) | IHL_NO_OPTIONS

#define MAKE_IP_ADDR(a, b, c, d)			\
	(((uint32_t) a << 24) | ((uint32_t) b << 16) |	\
	 ((uint32_t) c << 8) | (uint32_t) d)

#define IP_ADDR_STR_LEN	16

extern char *ip_addr_to_str(uint32_t addr, char *str);

/*
 * Structure of an internet header, naked of options.
 */
struct ip_hdr {
    uint8_t version_ihl; /* version and ihl */
    uint8_t tos;        /* type of service */
    uint16_t len;       /* total packet length */
	uint16_t id;			/* identification */
	uint16_t off;			/* fragment offset field */
	uint8_t ttl;			/* time to live */
	uint8_t proto;			/* protocol */
	uint16_t chksum;		/* checksum */
	uint32_t saddr;			/* source address */
	uint32_t daddr;			/* dest address */
} __packed __aligned(4);

#define	IP_RF 0x8000			/* reserved fragment flag */
#define	IP_DF 0x4000			/* dont fragment flag */
#define	IP_MF 0x2000			/* more fragments flag */
#define	IP_OFFMASK 0x1fff		/* mask for fragmenting bits */
#define	IP_MAXPACKET	65535		/* maximum packet size */

/*
 * Definitions for IP type of service (ip_tos).
 */
#define	IPTOS_LOWDELAY		0x10
#define	IPTOS_THROUGHPUT	0x08
#define	IPTOS_RELIABILITY	0x04
#define	IPTOS_MINCOST		0x02

/*
 * Definitions for IP precedence (also in ip_tos) (hopefully unused).
 */
#define	IPTOS_PREC_NETCONTROL		0xe0
#define	IPTOS_PREC_INTERNETCONTROL	0xc0
#define	IPTOS_PREC_CRITIC_ECP		0xa0
#define	IPTOS_PREC_FLASHOVERRIDE	0x80
#define	IPTOS_PREC_FLASH		0x60
#define	IPTOS_PREC_IMMEDIATE		0x40
#define	IPTOS_PREC_PRIORITY		0x20
#define	IPTOS_PREC_ROUTINE		0x00

/*
 * Definitions for DiffServ Codepoints as per RFC2474
 */
#define	IPTOS_DSCP_CS0		0x00
#define	IPTOS_DSCP_CS1		0x20
#define	IPTOS_DSCP_AF11		0x28
#define	IPTOS_DSCP_AF12		0x30
#define	IPTOS_DSCP_AF13		0x38
#define	IPTOS_DSCP_CS2		0x40
#define	IPTOS_DSCP_AF21		0x48
#define	IPTOS_DSCP_AF22		0x50
#define	IPTOS_DSCP_AF23		0x58
#define	IPTOS_DSCP_CS3		0x60
#define	IPTOS_DSCP_AF31		0x68
#define	IPTOS_DSCP_AF32		0x70
#define	IPTOS_DSCP_AF33		0x78
#define	IPTOS_DSCP_CS4		0x80
#define	IPTOS_DSCP_AF41		0x88
#define	IPTOS_DSCP_AF42		0x90
#define	IPTOS_DSCP_AF43		0x98
#define	IPTOS_DSCP_CS5		0xa0
#define	IPTOS_DSCP_EF		0xb8
#define	IPTOS_DSCP_CS6		0xc0
#define	IPTOS_DSCP_CS7		0xe0

/*
 * ECN (Explicit Congestion Notification) codepoints in RFC3168 mapped to the
 * lower 2 bits of the TOS field.
 */
#define	IPTOS_ECN_NOTECT	0x00	/* not-ECT */
#define	IPTOS_ECN_ECT1		0x01	/* ECN-capable transport (1) */
#define	IPTOS_ECN_ECT0		0x02	/* ECN-capable transport (0) */
#define	IPTOS_ECN_CE		0x03	/* congestion experienced */
#define	IPTOS_ECN_MASK		0x03	/* ECN field mask */

/*
 * Definitions for options.
 */
#define	IPOPT_COPIED(o)		((o)&0x80)
#define	IPOPT_CLASS(o)		((o)&0x60)
#define	IPOPT_NUMBER(o)		((o)&0x1f)

#define	IPOPT_CONTROL		0x00
#define	IPOPT_RESERVED1		0x20
#define	IPOPT_DEBMEAS		0x40
#define	IPOPT_RESERVED2		0x60

#define	IPOPT_EOL		0		/* end of option list */
#define	IPOPT_NOP		1		/* no operation */

#define	IPOPT_RR		7		/* record packet route */
#define	IPOPT_TS		68		/* timestamp */
#define	IPOPT_SECURITY		130		/* provide s,c,h,tcc */
#define	IPOPT_LSRR		131		/* loose source route */
#define	IPOPT_ESO		133		/* extended security */
#define	IPOPT_CIPSO		134		/* commerical security */
#define	IPOPT_SATID		136		/* satnet id */
#define	IPOPT_SSRR		137		/* strict source route */
#define	IPOPT_RA		148		/* router alert */

/*
 * Offsets to fields in options other than EOL and NOP.
 */
#define	IPOPT_OPTVAL		0		/* option ID */
#define	IPOPT_OLEN		1		/* option length */
#define	IPOPT_OFFSET		2		/* offset within option */
#define	IPOPT_MINOFF		4		/* min value of above */

/*
 * Time stamp option structure.
 */
struct	ip_timestamp {
	uint8_t code;			/* IPOPT_TS */
	uint8_t len;			/* size of structure (variable) */
	uint8_t ptr;			/* index of current entry */
#if __BYTE_ORDER == __LITTLE_ENDIAN
	uint8_t flags:4,		/* flags, see below */
		overflow:4;		/* overflow counter */
#endif
#if __BYTE_ORDER == __BIG_ENDIAN
	uint8_t overflow:4,		/* overflow counter */
		flags:4;		/* flags, see below */
#endif
	union  {
		uint32_t time[1];	/* network format */
		struct {
			uint32_t addr;
			uint32_t ipt_time;	/* network format */
		} ta[1];
	} u;
};

/* Flag bits for ipt_flg. */
#define	IPOPT_TS_TSONLY		0		/* timestamps only */
#define	IPOPT_TS_TSANDADDR	1		/* timestamps and addresses */
#define	IPOPT_TS_PRESPEC	3		/* specified modules only */

/* Bits for security (not byte swapped). */
#define	IPOPT_SECUR_UNCLASS	0x0000
#define	IPOPT_SECUR_CONFID	0xf135
#define	IPOPT_SECUR_EFTO	0x789a
#define	IPOPT_SECUR_MMMM	0xbc4d
#define	IPOPT_SECUR_RESTR	0xaf13
#define	IPOPT_SECUR_SECRET	0xd788
#define	IPOPT_SECUR_TOPSECRET	0x6bc5

/*
 * Internet implementation parameters.
 */
#define	MAXTTL		255		/* maximum time to live (seconds) */
#define	IPDEFTTL	64		/* default ttl, from RFC 1340 */
#define	IPFRAGTTL	60		/* time to live for frags, slowhz */
#define	IPTTLDEC	1		/* subtracted when forwarding */
#define	IP_MSS		576		/* default maximum segment size */

/*
 * This is the real IPv4 pseudo header, used for computing the TCP and UDP
 * checksums. For the Internet checksum, struct ipovly can be used instead.
 * For stronger checksums, the real thing must be used.
 */
struct ip_pseudo {
	uint32_t	saddr;		/* source internet address */
	uint32_t	daddr;		/* destination internet address */
	uint8_t		pad;		/* pad, must be zero */
	uint8_t		proto;		/* protocol */
	uint16_t	len;		/* protocol length */
};

// taken from DPDK
 static inline uint32_t
 __rte_raw_cksum(const void *buf, size_t len, uint32_t sum)
 {
     /* workaround gcc strict-aliasing warning */
     uintptr_t ptr = (uintptr_t)buf;
     typedef uint16_t __attribute__((__may_alias__)) u16_p;
     const u16_p *u16_buf = (const u16_p *)ptr;
 
     while (len >= (sizeof(*u16_buf) * 4)) {
         sum += u16_buf[0];
         sum += u16_buf[1];
         sum += u16_buf[2];
         sum += u16_buf[3];
         len -= sizeof(*u16_buf) * 4;
         u16_buf += 4;
     }
     while (len >= sizeof(*u16_buf)) {
         sum += *u16_buf;
         len -= sizeof(*u16_buf);
         u16_buf += 1;
     }
 
     /* if length is in odd bytes */
     if (len == 1) {
         uint16_t left = 0;
         *(uint8_t *)&left = *(const uint8_t *)u16_buf;
         sum += left;
     }
 
     NETPERF_DEBUG("Returning %u as sum", sum);
     return sum;
 }
 
 static inline uint16_t
 __rte_raw_cksum_reduce(uint32_t sum)
 {
     sum = ((sum & 0xffff0000) >> 16) + (sum & 0xffff);
     sum = ((sum & 0xffff0000) >> 16) + (sum & 0xffff);
     NETPERF_DEBUG("Returning %u as reduced sum, %u as u16, htons %u", sum, (uint16_t)sum, htons(sum));
     return (uint16_t)sum;
 }
 
 static inline uint16_t
 rte_raw_cksum(const void *buf, size_t len)
 {
     uint32_t sum;
 
     sum = __rte_raw_cksum(buf, len, 0);
     return __rte_raw_cksum_reduce(sum);
 }

#define get_chksum(hdr) \
    htons(rte_raw_cksum((void *)hdr, sizeof(hdr)))
