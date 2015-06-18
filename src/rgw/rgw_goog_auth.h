
// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_GOOG_AUTH_H
#define CEPH_RGW_GOOG_AUTH_H

#include "rgw_op.h"
#include "rgw_rest.h"

class RGW_GOOG_Auth_Get : public RGWOp {
public:
  RGW_GOOG_Auth_Get() {}
  ~RGW_GOOG_Auth_Get() {}

  int verify_permission() { return 0; }
  void execute();
  void send_response(); 
  virtual const string name() { return "goog_auth_get"; }
};
class RGWHandler_GOOG_Auth : public RGWHandler {
public:
  RGWHandler_GOOG_Auth() {}
  ~RGWHandler_GOOG_Auth() {}
  RGWOp *op_get();

  int init(RGWRados *store, struct req_state *state, RGWClientIO *cio);
  int authorize();
  int read_permissions(RGWOp *op) { return 0; }
  virtual RGWAccessControlPolicy *alloc_policy() { return NULL; }
  virtual void free_policy(RGWAccessControlPolicy *policy) {}
};

class RGWRESTMgr_GOOG_Auth : public RGWRESTMgr {
public:
  RGWRESTMgr_GOOG_Auth() {}
  virtual ~RGWRESTMgr_GOOG_Auth() {}

  virtual RGWRESTMgr *get_resource_mgr(struct req_state *s, const string& uri, string *out_uri) {
    return this;
  }
  virtual RGWHandler *get_handler(struct req_state *s) {
    return new RGWHandler_GOOG_Auth;
  }
};


#endif
