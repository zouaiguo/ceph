
// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_GOOG_AUTH_ACESS_TOKEN_H
#define CEPH_RGW_GOOG_AUTH_ACESS_TOKEN_H

#include "rgw_common.h"

class OAuthAcessToken {
public:
  class request {
    public:
      class headers {
        public:
          string Authorization;
          void decode_json(JSONObj *obj);
      };
      string url;
      headers hdr;
      void decode_json(JSONObj *obj);
  };	
  string token;
  string token_type;
  int expire;
  request req;
  string state;
  string provider;
  int parse(CephContext *cct, bufferlist& bl); 
  void decode_json(JSONObj *obj);
};
#endif
