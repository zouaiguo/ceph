// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_REST_Goog_H
#define CEPH_RGW_REST_Goog_H
#define TIME_BUF_SIZE 128

#include "rgw_op.h"
#include "rgw_http_errors.h"
#include "rgw_acl_goog.h"
#include "rgw_policy_goog.h"

#define RGW_AUTH_GRACE_MINS 15

class RGWCreateBucket_ObjStore_Goog : public RGWCreateBucket_ObjStore {
public:
  RGWCreateBucket_ObjStore_Goog() {}
  ~RGWCreateBucket_ObjStore_Goog() {}

  int get_params();
  void send_response();
};

class RGWDeleteBucket_ObjStore_Goog : public RGWDeleteBucket_ObjStore {
public:
  RGWDeleteBucket_ObjStore_Goog() {}
  ~RGWDeleteBucket_ObjStore_Goog() {}

  int get_params();
  void send_response();
};

class RGWHandler_ObjStore_Goog : public RGWHandler_ObjStore {
  friend class RGWRESTMgr_Goog;
public:
  static int init_from_header(struct req_state *s, int default_formatter, bool configurable_format);

  RGWHandler_ObjStore_Goog() : RGWHandler_ObjStore() {}
  virtual ~RGWHandler_ObjStore_Goog() {}

  int validate_bucket_name(const string& bucket, bool relaxed_names);

  virtual int init(RGWRados *store, struct req_state *state, RGWClientIO *cio);
  virtual int authorize() {
  }
};

class RGWHandler_ObjStore_Service_Goog : public RGWHandler_ObjStore_Goog {
protected:
  RGWOp *op_get();
  RGWOp *op_head();
public:
  RGWHandler_ObjStore_Service_Goog() {}
  virtual ~RGWHandler_ObjStore_Service_Goog() {}
};

class RGWHandler_ObjStore_Bucket_Goog : public RGWHandler_ObjStore_Goog {
protected:
  bool is_acl_op() {
    return s->info.args.exists("acl");
  }
  bool is_cors_op() {
      return s->info.args.exists("cors");
  }
  bool is_obj_update_op() {
    return is_acl_op() || is_cors_op();
  }
  RGWOp *get_obj_op(bool get_data);

  RGWOp *op_get(){};
  RGWOp *op_head(){};
  RGWOp *op_put(){};
  RGWOp *op_delete();
  RGWOp *op_post();
  RGWOp *op_options(){};
public:
  RGWHandler_ObjStore_Bucket_Goog() {}
  virtual ~RGWHandler_ObjStore_Bucket_Goog() {}
};

class RGWHandler_ObjStore_Obj_Goog : public RGWHandler_ObjStore_Goog {
protected:
  bool is_acl_op() {
    return s->info.args.exists("acl");
  }
  bool is_cors_op() {
      return s->info.args.exists("cors");
  }
  bool is_obj_update_op() {
    return is_acl_op();
  }
  RGWOp *get_obj_op(bool get_data);

  RGWOp *op_get();
  RGWOp *op_head();
  RGWOp *op_put();
  RGWOp *op_delete();
  RGWOp *op_post();
  RGWOp *op_options();
public:
  RGWHandler_ObjStore_Obj_Goog() {}
  virtual ~RGWHandler_ObjStore_Obj_Goog() {}
};

class RGWRESTMgr_Goog : public RGWRESTMgr {
public:
  RGWRESTMgr_Goog() {}
  virtual ~RGWRESTMgr_Goog() {}

  virtual RGWRESTMgr *get_resource_mgr(struct req_state *s, const string& uri) {
    return this;
  }
  virtual RGWHandler *get_handler(struct req_state *s);
};


#endif
