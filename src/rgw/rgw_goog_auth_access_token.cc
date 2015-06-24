
// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include <errno.h>
#include <common/ceph_json.h>
#include <common/errno.h>
#include <include/types.h>
#include "rgw_goog_auth_access_token.h"

#define dout_subsys ceph_subsys_rgw
int OAuthAcessToken::parse(CephContext *cct, bufferlist& bl)
{
  JSONParser parser;
  if (!parser.parse(bl.c_str(), bl.length())) {
    ldout(cct, 0) << "OAuthAcessToken token parse error: malformed json" << dendl;
    return -EINVAL;
  }

  try {
    decode_json(&parser);
  } catch (JSONDecoder::err& err) {
    ldout(cct, 0) << "OAuthAcessToken token parse error: " << err.message << dendl;
    return -EINVAL;
  }

  return 0;
}

