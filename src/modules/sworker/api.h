/**
 *
 * Copyright (C) 2025
 *
 * This file is part of kamailio, a free SIP server.
 *
 * Kamailio is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version
 *
 * Kamailio is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */

#ifndef _SWORKER_MOD_API_H_
#define _SWORKER_MOD_API_H_

#include "../../core/sr_module.h"
#include "../../core/usr_avp.h"

typedef int (*sworker_api_is_active_f)();

typedef struct sworker_api
{
	sworker_api_is_active_f is_active;
} sworker_api_t;

typedef int (*bind_sworker_f)(sworker_api_t *api);
int bind_sworker(sworker_api_t *api);

/**
 * @brief Load the Sanity API
 */
static inline int sworker_load_api(sworker_api_t *api)
{
	bind_sworker_f bindsworker;

	bindsworker = (bind_sworker_f)find_export("bind_sworker", 0, 0);
	if(bindsworker == 0) {
		LM_ERR("cannot find bind_sworker\n");
		return -1;
	}
	if(bindsworker(api) < 0) {
		LM_ERR("cannot bind sworker api\n");
		return -1;
	}
	return 0;
}

#endif
