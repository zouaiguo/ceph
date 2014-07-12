// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/types.h"
#include "Messenger.h"

#include "msg/simple/SimpleMessenger.h"
#include "msg/async/AsyncMessenger.h"
#ifdef HAVE_XIO
#include "msg/xio/XioMessenger.h"
#endif

Messenger *Messenger::create(CephContext *cct, const string &type,
			     entity_name_t name, string lname,
			     uint64_t nonce, uint64_t features)
{
  int r = -1;
  if (type == "random")
    r = rand() % 2; // random does not include xio
  if (r == 0 || type == "simple")
    return new SimpleMessenger(cct, name, lname, nonce, features);
  else if ((r == 1 || type == "async") &&
	   cct->check_experimental_feature_enabled("ms-type-async"))
    return new AsyncMessenger(cct, name, lname, nonce, features);
#ifdef HAVE_XIO
  else if ((type == "xio") &&
	   cct->check_experimental_feature_enabled("ms-type-xio"))
    return new XioMessenger(cct, name, lname, nonce, features);
#endif
  lderr(cct) << "unrecognized ms_type '" << type << "'" << dendl;
  return NULL;
}

/*
 * Pre-calculate desired software CRC settings.  CRC computation may
 * be disabled by default for some transports (e.g., those with strong
 * hardware checksum support).
 */
int Messenger::get_default_crc_flags(md_config_t * conf)
{
  int r = 0;
  if (conf->ms_crc_data)
    r |= MSG_CRC_DATA;
  if (conf->ms_crc_header)
    r |= MSG_CRC_HEADER;
  return r;
}

/*
 * I. return former behavior with stock ceph.conf + tcp (+ no rdma)
 * II. default to no crc when rdma in config (even if doing tcp)
 * III. lots of silly configuration possible, precompute to make cheap.
 *
 * Note that crc is silly.  tcp always does checksumming too, + retries
 *  if the data is wrong.  Ethernet does checksumming also, so does PPP.
 *  Authentication (should) be doing cryptographic checksums.  All
 *  crc does is slow the code down (and maybe catch programming errors.)
 *
 * behavior:
 *	ms_datacrc = false ms_restcrc = false	=> 0		force crc no
 *	ms_datacrc = false ms_restcrc = true	=> MS_CRC_REST	force former "no crc"
 *	ms_datacrc = true ms_restcrc = true	=> MS_CRC_REST|MS_CRC_DATA force crc yes
 *	otherwise, if rdma_local is specified	=> 0		no crc
 *	otherwise, if "ms nocrc" == true	=> MS_CRC_REST	former "no crc"
 *	otherwise,				=> MS_CRC_REST|MS_CRC_DATA crc yes
 */
int Messenger::get_default_crc_flags(md_config_t * conf)
{
	int r = 0;
	if (conf->ms_datacrc)
		r |= MSG_CRC_DATA;
	if (conf->ms_restcrc)
		r |= MSG_CRC_REST;
	switch(r) {
	case MSG_CRC_DATA:
		std::vector <std::string> my_sections;
		g_conf->get_my_sections(my_sections);
		std::string val;
		r = MSG_CRC_REST | MSG_CRC_DATA;
		if (g_conf->get_val_from_conf_file(my_sections, "rdma local",
			val, true) == 0) {
			r = 0;
		}
		if (g_conf->get_val_from_conf_file(my_sections, "ms nocrc",
			val, true) == 0) {
			int v = -1;
			if (strcasecmp(val.c_str(), "false") == 0)
				v = 0;
			else if (strcasecmp(val.c_str(), "true") == 0)
				v = 1;
			else {
				std::string err;
				int b = strict_strtol(val.c_str(), 10, &err);
					if (!err.empty()) {
						v = -1;
// XXX should emit an error?
					}
				v = !!b;
			}
			switch(v) {
			case 1:
				r &= ~MSG_CRC_DATA;
			}
		}
		break;
	/* default: break; */
	}
	return r;
}
