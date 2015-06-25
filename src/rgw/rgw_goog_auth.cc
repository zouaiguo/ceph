
// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_goog_auth.h"
#include "rgw_rest.h"

#include "common/ceph_crypto.h"
#include "common/Clock.h"

#include "auth/Crypto.h"

#include "rgw_client_io.h"
#include <uuid/uuid.h>

#define dout_subsys ceph_subsys_rgw

#define DEFAULT_GOOG_PREFIX "goog"

using namespace ceph::crypto;


void RGW_GOOG_Auth_Get::execute()
{
  int ret = -EPERM;

  bufferlist bl;
  char uuid_str[36];
  uuid_t state_id;
  uuid_generate(state_id);
  uuid_unparse(state_id, uuid_str);
  ldout(store->ctx(),0) << "oauth state id: " << uuid_str<< dendl;
  memcpy(s->goog_oauth_state_id,uuid_str,sizeof(uuid_str));
  ret = ERR_FOUND_REDIRECT;   
  set_req_state_err(s, ret);

}

void RGW_GOOG_Auth_Get::send_response()
{
  dump_errno(s);
  std::string redirect = "https://oauth.io/auth/google?k=lQ8NFm40FMCSc1FOK0leMixH0Jk&opts={\"state\":";
  string state(s->goog_oauth_state_id, 36); 
  redirect += "\"" + state; 
  redirect += "\"}&redirect_type=server&redirect_uri=http://localhost:8000/oauth/redirect";
  dump_redirect(s,redirect);
  end_header(s,this,"text/html");
  cout<< redirect;
  ldout(store->ctx(),0) << "redirect: " << redirect << dendl;
  s->formatter->open_object_section_with_attrs("a",FormatterAttrs("href",redirect.c_str(),NULL));
  s->formatter->close_section();
  rgw_flush_formatter_and_reset(s,s->formatter);
}
int RGWHandler_GOOG_Auth::init(RGWRados *store, struct req_state *state, RGWClientIO *cio)
{
  state->dialect = "goog-auth";
  state->formatter = new XMLFormatter(false);
  state->format = RGW_FORMAT_XML;

  return RGWHandler::init(store, state, cio);
}

int RGWHandler_GOOG_Auth::authorize()
{
  return 0;
}

RGWOp *RGWHandler_GOOG_Auth::op_get()
{
  return new RGW_GOOG_Auth_Get;
}

