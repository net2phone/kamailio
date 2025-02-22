/*
 * PIKE module
 *
 * This file is part of Kamailio, a free SIP server.
 *
 * SPDX-License-Identifier: GPL-2.0-or-later
 *
 * Kamailio is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version
 *
 * Kamailio is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 *
 */
#include "pike_top.h"

#include "../../core/dprint.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <arpa/inet.h>
#include "ip_tree.h"


static struct TopListItem_t *top_list_root = 0;
static struct TopListItem_t *top_list_iter = 0;

static char _pike_top_buff[PIKE_BUFF_SIZE];

struct TopListItem_t *pike_top_get_root()
{
	return top_list_root;
}

char *pike_top_print_addr(
		unsigned char *ip, int iplen, char *obuff, int obuffsize)
{
	unsigned short *ipv6_ptr = (unsigned short *)ip;
	int blen;

	memset(obuff, 0, obuffsize);

	DBG("address iplen: %d, buffsize: %d", iplen, obuffsize);

	if(iplen == 4) {
		inet_ntop(AF_INET, ip, obuff, obuffsize);
	} else if(iplen == 16) {
		inet_ntop(AF_INET6, ip, obuff, obuffsize);
	} else {
		blen = snprintf(obuff, obuffsize,
				"%04x:%04x:%04x:%04x:%04x:%04x:%04x:%04x", htons(ipv6_ptr[0]),
				htons(ipv6_ptr[1]), htons(ipv6_ptr[2]), htons(ipv6_ptr[3]),
				htons(ipv6_ptr[4]), htons(ipv6_ptr[5]), htons(ipv6_ptr[6]),
				htons(ipv6_ptr[7]));
		if(blen < 0 || blen >= obuffsize) {
			LM_ERR("failed to print the address - reset it\n");
			memset(obuff, 0, obuffsize);
		}
	}

	return obuff;
}

/* if you do not need global buffer, you can use this simpler call */
static char *print_addr(unsigned char *ip, int iplen)
{
	return pike_top_print_addr(
			ip, iplen, _pike_top_buff, sizeof(_pike_top_buff));
}

int pike_top_add_entry(unsigned char *ip_addr, int addr_len,
		unsigned short leaf_hits[2], unsigned short hits[2],
		unsigned int expires, pike_node_status_t status)
{
	struct TopListItem_t *new_item =
			(struct TopListItem_t *)malloc(sizeof(struct TopListItem_t));
	if(!new_item) {
		SYS_MEM_ERROR;
		return -1;
	}
	print_addr(ip_addr, addr_len);
	DBG("pike_top_add_enrty(ip: %s, leaf_hits[%d,%d], hits[%d,%d],"
		" expires: %d, status: %d)",
			_pike_top_buff, leaf_hits[0], leaf_hits[1], hits[0], hits[1],
			expires, status);
	assert(new_item != 0);

	memset((void *)new_item, 0, sizeof(struct TopListItem_t));

	new_item->status = status;
	new_item->expires = expires;
	new_item->hits[0] = hits[0];
	new_item->hits[1] = hits[1];
	new_item->leaf_hits[0] = leaf_hits[0];
	new_item->leaf_hits[1] = leaf_hits[1];

	assert(addr_len <= 16);

	new_item->addr_len = addr_len;
	memcpy(new_item->ip_addr, ip_addr, addr_len);

	new_item->next = top_list_root;
	top_list_root = new_item;

	return 1;
}

void pike_top_list_clear()
{
	struct TopListItem_t *ptr;

	top_list_iter = top_list_root;
	while(top_list_iter) {
		ptr = top_list_iter->next;
		free(top_list_iter);
		top_list_iter = ptr;
	}
	top_list_root = 0;
	memset(_pike_top_buff, 0, sizeof(_pike_top_buff));
}
