// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_ACL_Goog_H
#define CEPH_RGW_ACL_Goog_H

#include <map>
#include <string>
#include <iostream>
#include <include/types.h>

#include <expat.h>

#include "include/str_list.h"
#include "rgw_xml.h"
#include "rgw_acl.h"


using namespace std;

class RGWRados;

class ACLPermission_Goog : public ACLPermission, public XMLObj
{
public:
  ACLPermission_Goog() {}
  ~ACLPermission_Goog() {}

  bool xml_end(const char *el);
  void to_xml(ostream& out);
};

class ACLGrantee_Goog : public ACLGrantee, public XMLObj
{
public:
  ACLGrantee_Goog() {}
  ~ACLGrantee_Goog() {}

  bool xml_start(const char *el, const char **attr);
};


class ACLGrant_Goog : public ACLGrant, public XMLObj
{
public:
  ACLGrant_Goog() {}
  ~ACLGrant_Goog() {}

  void to_xml(CephContext *cct, ostream& out);
  bool xml_end(const char *el);
  bool xml_start(const char *el, const char **attr);

  static ACLGroupTypeEnum uri_to_group(string& uri);
  static bool group_to_uri(ACLGroupTypeEnum group, string& uri);
};

class RGWAccessControlList_Goog : public RGWAccessControlList, public XMLObj
{
public:
  RGWAccessControlList_Goog(CephContext *_cct) : RGWAccessControlList(_cct) {}
  ~RGWAccessControlList_Goog() {}

  bool xml_end(const char *el);
  void to_xml(ostream& out) {
    multimap<string, ACLGrant>::iterator iter;
    out << "<AccessControlList>";
    for (iter = grant_map.begin(); iter != grant_map.end(); ++iter) {
      ACLGrant_Goog& grant = static_cast<ACLGrant_Goog &>(iter->second);
      grant.to_xml(cct, out);
    }
    out << "</AccessControlList>";
  }

  int create_canned(ACLOwner& owner, ACLOwner& bucket_owner, const string& canned_acl);
  int create_from_grants(std::list<ACLGrant>& grants);
};

class ACLOwner_Goog : public ACLOwner, public XMLObj
{
public:
  ACLOwner_Goog() {}
  ~ACLOwner_Goog() {}

  bool xml_end(const char *el);
  void to_xml(ostream& out) {
    if (id.empty())
      return;
    out << "<Owner>" << "<ID>" << id << "</ID>";
    if (!display_name.empty())
      out << "<DisplayName>" << display_name << "</DisplayName>";
    out << "</Owner>";
  }
};

class RGWEnv;

class RGWAccessControlPolicy_Goog : public RGWAccessControlPolicy, public XMLObj
{
public:
  RGWAccessControlPolicy_Goog(CephContext *_cct) : RGWAccessControlPolicy(_cct) {}
  ~RGWAccessControlPolicy_Goog() {}

  bool xml_end(const char *el);

  void to_xml(ostream& out) {
    out << "<AccessControlPolicy xmlns=\"http://goog.amazonaws.com/doc/2006-03-01/\">";
    ACLOwner_Goog& _owner = static_cast<ACLOwner_Goog &>(owner);
    RGWAccessControlList_Goog& _acl = static_cast<RGWAccessControlList_Goog &>(acl);
    _owner.to_xml(out);
    _acl.to_xml(out);
    out << "</AccessControlPolicy>";
  }
  int rebuild(RGWRados *store, ACLOwner *owner, RGWAccessControlPolicy& dest);
  bool compare_group_name(string& id, ACLGroupTypeEnum group);

  virtual int create_canned(ACLOwner& _owner, ACLOwner& bucket_owner, string canned_acl) {
    RGWAccessControlList_Goog& _acl = static_cast<RGWAccessControlList_Goog &>(acl);
    int ret = _acl.create_canned(_owner, bucket_owner, canned_acl);
    owner = _owner;
    return ret;
  }
  int create_from_headers(RGWRados *store, RGWEnv *env, ACLOwner& _owner);
};

/**
 * Interfaces with the webserver's XML handling code
 * to parse it in a way that makes sense for the rgw.
 */
class RGWACLXMLParser_Goog : public RGWXMLParser
{
  CephContext *cct;

  XMLObj *alloc_obj(const char *el);
public:
  RGWACLXMLParser_Goog(CephContext *_cct) : cct(_cct) {}
};

#endif
