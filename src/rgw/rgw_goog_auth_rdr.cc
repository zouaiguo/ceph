
// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_goog_auth_rdr.h"
#include "rgw_rest.h"

#include "common/ceph_crypto.h"
#include "common/Clock.h"

#include "auth/Crypto.h"

#include "rgw_client_io.h"
#include <uuid/uuid.h>
#include <curl/curl.h>
#include <curl/easy.h>
#include "rgw_http_client.h"

#define dout_subsys ceph_subsys_rgw

#define DEFAULT_GOOG_PREFIX "goog"

using namespace ceph::crypto;

static size_t receive_http_header(void *ptr, size_t size, size_t nmemb, void *_info)
{
  RGWHTTPClient *client = static_cast<RGWHTTPClient *>(_info);
  size_t len = size * nmemb;
  int ret = client->receive_header(ptr, size * nmemb);
  if (ret < 0) {
    dout(0) << "WARNING: client->receive_header() returned ret=" << ret << dendl;
  }

  return len;
}

static size_t receive_http_data(void *ptr, size_t size, size_t nmemb, void *_info)
{
  RGWHTTPClient *client = static_cast<RGWHTTPClient *>(_info);
  size_t len = size * nmemb;
  int ret = client->receive_data(ptr, size * nmemb);
  if (ret < 0) {
    dout(0) << "WARNING: client->receive_data() returned ret=" << ret << dendl;
  }

  return len;
}

static size_t send_http_data(void *ptr, size_t size, size_t nmemb, void *_info)
{
  RGWHTTPClient *client = static_cast<RGWHTTPClient *>(_info);
  int ret = client->send_data(ptr, size * nmemb);
  if (ret < 0) {
    dout(0) << "WARNING: client->receive_data() returned ret=" << ret << dendl;
  }

  return ret;
}

class RGWPostOAuthToken: public RGWHTTPClient {
  bufferlist *bl;
  std::string post_data;
  size_t post_data_index;
public:
  RGWPostOAuthToken(CephContext *_cct, bufferlist *_bl) : RGWHTTPClient(_cct), bl(_bl), post_data_index(0) {}

  void set_post_data(const std::string& _post_data) {
    this->post_data = _post_data;
  }

  int send_data(void* ptr, size_t len) {
    int length_to_copy = 0;
    if (post_data_index < post_data.length()) {
      length_to_copy = min(post_data.length() - post_data_index, len);
      memcpy(ptr, post_data.data() + post_data_index, length_to_copy);
      post_data_index += length_to_copy;
    }
    return length_to_copy;
  }

  int receive_data(void *ptr, size_t len) {
    bl->append((char *)ptr, len);
    return 0;
  }

  int receive_header(void *ptr, size_t len) {
    return 0;
  }
  
int process(const char *method, const char *url, const char *data)
{
  int ret = 0;
  CURL *curl_handle;

  char error_buf[CURL_ERROR_SIZE];

  curl_handle = curl_easy_init();

  dout(20) << "sending request to " << url << dendl;


  curl_easy_setopt(curl_handle, CURLOPT_CUSTOMREQUEST, method);
  curl_easy_setopt(curl_handle, CURLOPT_URL, url);
  curl_easy_setopt(curl_handle, CURLOPT_NOPROGRESS, 1L);
  curl_easy_setopt(curl_handle, CURLOPT_NOSIGNAL, 1L);
  curl_easy_setopt(curl_handle, CURLOPT_HEADERFUNCTION, receive_http_header);
  curl_easy_setopt(curl_handle, CURLOPT_WRITEHEADER, (void *)this);
  curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, receive_http_data);
  curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, (void *)this);
  curl_easy_setopt(curl_handle, CURLOPT_ERRORBUFFER, (void *)error_buf);
  curl_easy_setopt(curl_handle, CURLOPT_POSTFIELDS, data); 
  CURLcode status = curl_easy_perform(curl_handle);
  if (status) {
    dout(0) << "curl_easy_performed returned error: " << error_buf << dendl;
    ret = -EINVAL;
  }
  curl_easy_cleanup(curl_handle);

  return ret;
}
};
class RGWGetOAuthToken : public RGWHTTPClient {
  bufferlist *bl;
public:
  RGWGetOAuthToken(CephContext *_cct, bufferlist *_bl) : RGWHTTPClient(_cct), bl(_bl) {}

  int receive_data(void *ptr, size_t len) {
    bl->append((char *)ptr, len);
    return 0;
  }
  int receive_header(void *ptr, size_t len) {
    return 0;
  }
  int send_data(void *ptr, size_t len) {
    return 0;
  }

};
void RGW_GOOG_Auth_Rdr_Get::execute()
{
  int ret = -EPERM;

  bufferlist bl;
  bool exists;
  string OAuthURL = "https://oauth.io";
  string secretKey = "8Se8ut2Oi-thC0Fs5AwXZQSMnBI";
  string appKey = "lQ8NFm40FMCSc1FOK0leMixH0Jk";
  RGWPostOAuthToken getToken(g_ceph_context,&bl);
  //RGWGetOAuthToken getToken(g_ceph_context,&bl);
  string code = s->info.args.get("code",&exists);
  string code_removed;
  if(exists == true){
    ldout(s->cct,0) << "access code: " << code << dendl;
  }
  else{

    ldout(s->cct,0) << "access code does not exist"  << dendl;
    return;
  }
  string url = OAuthURL + "/auth/access_token";
  //getToken.append_header("Conent-Type", "application/x-www-form-urlencoded");
  string post_data = "code="+code+"&key=" +appKey+ "&secret="+secretKey; 
  getToken.set_post_data(post_data);
  getToken.set_send_length(post_data.size());
  ldout(s->cct,0) << "http post: " << post_data << dendl;
  int r = getToken.process("POST",url.c_str(), post_data.c_str());
  //int r = getToken.process("POST",post_data.c_str());
  if(r < 0){
    ldout(s->cct,0) << "http resp error: " << r << dendl;
    return;
  }
  
  bl.append((char)0);
  ldout(s->cct,0) << "http resp from oauth: " << bl.c_str()<< dendl;
  set_req_state_err(s, ret);

}

void RGW_GOOG_Auth_Rdr_Get::send_response()
{
  dump_errno(s);
  end_header(s,this,"text/html");
  dump_start(s);
  //std::string redirect = "https://oauth.io/auth/google?k=lQ8NFm40FMCSc1FOK0leMixH0Jk&opts="+"{\"stat\":\""+s->goog_oauth_state_id+ "\"}&redirect_type=server&redirect_uri=http://localhost:3000/oauth/redirect";
  std::string redirect = "https://oauth.io/auth/google?k=lQ8NFm40FMCSc1FOK0leMixH0Jk&opts={\"stat\":";
  std::string state = std::string(reinterpret_cast<const char*>(s->goog_oauth_state_id), sizeof(s->goog_oauth_state_id)/sizeof(s->goog_oauth_state_id[0]));
  redirect += state;
  redirect += "\"}&redirect_type=server&redirect_uri=http://localhost:3000/oauth/redirect\"";
  cout<< redirect;
  rgw_flush_formatter_and_reset(s,s->formatter);
}
int RGWHandler_GOOG_Auth_Rdr::init(RGWRados *store, struct req_state *state, RGWClientIO *cio)
{
  ldout(store->ctx(),0) << "relative uri: " << state->relative_uri.c_str() << dendl;
  ldout(store->ctx(),0) << "decoded uri: " << state->decoded_uri.c_str() << dendl;

  const char *req_name = state->relative_uri.c_str();
  const char *p;
  string code_str;

  if (*req_name == '?') {
    p = req_name;
  } else {
   string req_param = state->info.request_params;
  ldout(store->ctx(),0) << "req_param: " << req_param << dendl;
  size_t pos = req_param.find("code");
  if(pos != string::npos){
    code_str = req_param.substr(pos,4)+"="+req_param.substr(pos+13,27) ;
    ldout(store->ctx(),0) << "code_str: " << code_str << dendl;
  }else{
    ldout(store->ctx(),0) << "code_str not found " << dendl;
  }
    //req_param.erase(std::remove(req_param.begin(), req_param.end(), '\"'), req_param.end());
    //req_param.erase(std::remove(req_param.begin(), req_param.end(), '\{}'), req_param.end());
    p = code_str.c_str();
    //p = req_param.c_str(); 
  //ldout(store->ctx(),0) << "req_param_removed: " << req_param << "p: " << p <<dendl;
  }

  ldout(store->ctx(),0) << "req para: " << state->info.request_params.c_str() << dendl;
  state->info.args.set(p);
  state->info.args.parse();
  map<string, string> para_map = state->info.args.get_params();
  map<string, string>::iterator iter = para_map.begin();
  while(iter != para_map.end()){
    ldout(store->ctx(),0) << "req para: " << iter->first << " " << iter->second << dendl;
    iter++;

  }
  state->dialect = "goog-auth";
  state->formatter = new XMLFormatter(false);
  state->format = RGW_FORMAT_XML;

  return RGWHandler::init(store, state, cio);
}

int RGWHandler_GOOG_Auth_Rdr::authorize()
{
  return 0;
}

RGWOp *RGWHandler_GOOG_Auth_Rdr::op_get()
{
  return new RGW_GOOG_Auth_Rdr_Get;
}

