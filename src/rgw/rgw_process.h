#ifndef RGW_FRONTEND_H
#define RGW_FRONTEND_H

struct RGWProcessEnv {
  RGWRados *store;
  RGWREST *rest;
  OpsLogSocket *olog;
  int port;
};

class RGWFrontendConfig {
  string config;
  map<string, string> config_map;
  int parse_config(const string& config, map<string, string>& config_map);
  string framework;
public:
  RGWFrontendConfig(const string& _conf) : config(_conf) {}
  int init() {
    int ret = parse_config(config, config_map);
    if (ret < 0)
      return ret;
    return 0;
  }
  bool get_val(const string& key, const string& def_val, string *out);
  bool get_val(const string& key, int def_val, int *out);

  map<string, string>& get_config_map() { return config_map; }

  string get_framework() { return framework; }
};

class RGWProcess {
  deque<RGWRequest *> m_req_queue;
protected:
  RGWRados *store;
  OpsLogSocket *olog;
  ThreadPool m_tp;
  Throttle req_throttle;
  RGWREST *rest;
  RGWFrontendConfig *conf;
  int sock_fd;

  struct RGWWQ : public ThreadPool::WorkQueue<RGWRequest> {
    RGWProcess *process;
    RGWWQ(RGWProcess *p, time_t timeout, time_t suicide_timeout, ThreadPool *tp)
      : ThreadPool::WorkQueue<RGWRequest>("RGWWQ", timeout, suicide_timeout, tp), process(p) {}

    bool _enqueue(RGWRequest *req) ;

    void _dequeue(RGWRequest *req) {
      assert(0);
    }
    bool _empty() {
      return process->m_req_queue.empty();
    }
    RGWRequest *_dequeue();

    void _process(RGWRequest *req);

    void _dump_queue();

    void _clear() {
      assert(process->m_req_queue.empty());
    }
  } req_wq;

public:
  RGWProcess(CephContext *cct, RGWProcessEnv *pe, int num_threads, RGWFrontendConfig *_conf)
    : store(pe->store), olog(pe->olog), m_tp(cct, "RGWProcess::m_tp", num_threads),
      req_throttle(cct, "rgw_ops", num_threads * 2),
      rest(pe->rest),
      conf(_conf),
      sock_fd(-1),
      req_wq(this, g_conf->rgw_op_thread_timeout,
	     g_conf->rgw_op_thread_suicide_timeout, &m_tp) {}
  virtual ~RGWProcess() {}
  virtual void run() = 0;
  virtual void handle_request(RGWRequest *req) = 0;

  void close_fd() {
    if (sock_fd >= 0) {
      ::close(sock_fd);
      sock_fd = -1;
    }
  }
};

class RGWFrontend {
public:
  virtual ~RGWFrontend() {}

  virtual int init() = 0;

  virtual int run() = 0;
  virtual void stop() = 0;
  virtual void join() = 0;
};


class RGWProcessControlThread : public Thread {
  RGWProcess *pprocess;
public:
  RGWProcessControlThread(RGWProcess *_pprocess) : pprocess(_pprocess) {}

  void *entry() {
    pprocess->run();
    return NULL;
  }
};

class RGWProcessFrontend : public RGWFrontend {
protected:
  RGWFrontendConfig *conf;
  RGWProcess *pprocess;
  RGWProcessEnv env;
  RGWProcessControlThread *thread;

public:
  RGWProcessFrontend(RGWProcessEnv& pe, RGWFrontendConfig *_conf) : conf(_conf), pprocess(NULL), env(pe), thread(NULL) {
  }

  ~RGWProcessFrontend() {
    delete thread;
    delete pprocess;
  }

  int run() {
    assert(pprocess); /* should have initialized by init() */
    thread = new RGWProcessControlThread(pprocess);
    thread->create();
    return 0;
  }

  void stop() {
    pprocess->close_fd();
    thread->kill(SIGUSR1);
  }

  void join() {
    thread->join();
  }
};

int process_request(RGWRados *store, RGWREST *rest, RGWRequest *req, RGWClientIO *client_io, OpsLogSocket *olog);

#endif
