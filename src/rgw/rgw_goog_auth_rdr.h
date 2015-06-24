
// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_GOOG_AUTH_RDR_H
#define CEPH_RGW_GOOG_AUTH_RDR_H

#include "rgw_op.h"
#include "rgw_rest.h"
#include "rgw_goog_auth_access_token.h"

class RGW_GOOG_Auth_Rdr_Get : public RGWOp {
 OAuthAcessToken ac; 
 bufferlist bl;
public:
  RGW_GOOG_Auth_Rdr_Get() {}
  ~RGW_GOOG_Auth_Rdr_Get() {}

  int verify_permission() { return 0; }
  void execute();
  void send_response(); 
  virtual const string name() { return "goog_auth_get"; }
};
class RGWHandler_GOOG_Auth_Rdr : public RGWHandler {
public:
  RGWHandler_GOOG_Auth_Rdr() {}
  ~RGWHandler_GOOG_Auth_Rdr() {}
  RGWOp *op_get();

  int init(RGWRados *store, struct req_state *state, RGWClientIO *cio);
  int authorize();
  int read_permissions(RGWOp *op) { return 0; }
  virtual RGWAccessControlPolicy *alloc_policy() { return NULL; }
  virtual void free_policy(RGWAccessControlPolicy *policy) {}
};

class RGWRESTMgr_GOOG_Auth_Rdr : public RGWRESTMgr {
public:
  RGWRESTMgr_GOOG_Auth_Rdr() {}
  virtual ~RGWRESTMgr_GOOG_Auth_Rdr() {}

  virtual RGWRESTMgr *get_resource_mgr(struct req_state *s, const string& uri, string *out_uri) {
    return this;
  }
  virtual RGWHandler *get_handler(struct req_state *s) {

    return new RGWHandler_GOOG_Auth_Rdr;
  }
};


#endif
