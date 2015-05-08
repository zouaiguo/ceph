#ifndef RGW_REQUEST_H
#define RGW_REQUEST_H

struct RGWRequest
{
  uint64_t id;
  struct req_state *s;
  string req_str;
  RGWOp *op;
  utime_t ts;

  RGWRequest(uint64_t id) : id(id), s(NULL), op(NULL) {}

  virtual ~RGWRequest() {}

  void init_state(req_state *_s) {
    s = _s;
  }

  void log_format(struct req_state *s, const char *fmt, ...);
  void log_init();

  void log(struct req_state *s, const char *msg);
};

#endif
