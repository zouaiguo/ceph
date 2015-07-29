// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef PRIORITY_QUEUE_H
#define PRIORITY_QUEUE_H

#include "common/Mutex.h"
#include "common/Formatter.h"
#include "common/ceph_context.h"
#include "common/config.h"

#include <map>
#include <utility>
#include <list>
#include <algorithm>
#include <time.h>
#include <cstdlib>
#include <sstream>

/**
 * Manages queue for normal and strict priority items
 *
 * On dequeue, the queue will select the lowest priority queue
 * such that the q has bucket > cost of front queue item.
 *
 * If there is no such queue, we choose the next queue item for
 * the highest priority queue.
 *
 * Before returning a dequeued item, we place into each bucket
 * cost * (priority/total_priority) tokens.
 *
 * enqueue_strict and enqueue_strict_front queue items into queues
 * which are serviced in strict priority order before items queued
 * with enqueue and enqueue_front
 *
 * Within a priority class, we schedule round robin based on the class
 * of type K used to enqueue items.  e.g. you could use entity_inst_t
 * to provide fairness for different clients.
 */


struct SLO {
  unsigned reserve;
  unsigned prop;
  unsigned limit;
  SLO() :
    reserve(0), prop(0), limit(0) {
    }

  SLO(unsigned r, unsigned p, unsigned l) {
    reserve = r;
    prop = p;
    limit = l;
  }
};

class CephContext;

template<typename T, typename K>
class PrioritizedQueue {
  int64_t total_priority;
  int64_t max_tokens_per_subqueue;
  int64_t min_cost;

  //tags that dmClock uses
  enum tag_types_t {
    T_NONE = -1, T_RESERVE = 0, T_PROP, T_LIMIT
  };

  typedef std::list<std::pair<double, T> > ListPairs;
  template<class F>
    static unsigned filter_list_pairs(ListPairs *l, F f, std::list<T> *out) {
      unsigned ret = 0;
      if (out) {
	for (typename ListPairs::reverse_iterator i = l->rbegin();
	    i != l->rend(); ++i) {
	  if (f(i->second)) {
	    out->push_front(i->second);
	  }
	}
      }
      for (typename ListPairs::iterator i = l->begin(); i != l->end();) {
	if (f(i->second)) {
	  l->erase(i++);
	  ++ret;
	} else {
	  ++i;
	}
      }
      return ret;
    }

  struct SubQueue {
    private:
      typedef std::map<K, ListPairs> Classes;
      Classes q;
      unsigned tokens, max_tokens;
      int64_t size;
      typename Classes::iterator cur;
    public:
      SubQueue(const SubQueue &other) :
	q(other.q), tokens(other.tokens), max_tokens(other.max_tokens), size(
	    other.size), cur(q.begin()) {
	}
      SubQueue() :
	tokens(0), max_tokens(0), size(0), cur(q.begin()) {
	}
      void set_max_tokens(unsigned mt) {
	max_tokens = mt;
      }
      unsigned get_max_tokens() const {
	return max_tokens;
      }
      unsigned num_tokens() const {
	return tokens;
      }
      void put_tokens(unsigned t) {
	tokens += t;
	if (tokens > max_tokens)
	  tokens = max_tokens;
      }
      void take_tokens(unsigned t) {
	if (tokens > t)
	  tokens -= t;
	else
	  tokens = 0;
      }
      void enqueue(K cl, unsigned cost, T item) {
	q[cl].push_back(std::make_pair(cost, item));
	if (cur == q.end())
	  cur = q.begin();
	size++;
      }
      void enqueue_front(K cl, unsigned cost, T item) {
	q[cl].push_front(std::make_pair(cost, item));
	if (cur == q.end())
	  cur = q.begin();
	size++;
      }
      std::pair<unsigned, T> front() const {
	assert(!(q.empty()));
	assert(cur != q.end());
	return cur->second.front();
      }
      void pop_front() {
	assert(!(q.empty()));
	assert(cur != q.end());
	cur->second.pop_front();
	if (cur->second.empty())
	  q.erase(cur++);
	else
	  ++cur;
	if (cur == q.end())
	  cur = q.begin();
	size--;
      }
      unsigned length() const {
	assert(size >= 0);
	return (unsigned) size;
      }
      bool empty() const {
	return q.empty();
      }
      template<class F>
	void remove_by_filter(F f, std::list<T> *out) {
	  for (typename Classes::iterator i = q.begin(); i != q.end();) {
	    size -= filter_list_pairs(&(i->second), f, out);
	    if (i->second.empty()) {
	      if (cur == i)
		++cur;
	      q.erase(i++);
	    } else {
	      ++i;
	    }
	  }
	  if (cur == q.end())
	    cur = q.begin();
	}
      void remove_by_class(K k, std::list<T> *out) {
	typename Classes::iterator i = q.find(k);
	if (i == q.end())
	  return;
	size -= i->second.size();
	if (i == cur)
	  ++cur;
	if (out) {
	  for (typename ListPairs::reverse_iterator j =
	      i->second.rbegin(); j != i->second.rend(); ++j) {
	    out->push_front(j->second);
	  }
	}
	q.erase(i);
	if (cur == q.end())
	  cur = q.begin();
      }

      void dump(Formatter *f) const {
	f->dump_int("tokens", tokens);
	f->dump_int("max_tokens", max_tokens);
	f->dump_int("size", size);
	f->dump_int("num_keys", q.size());
	if (!empty())
	  f->dump_int("first_item_cost", front().first);
      }
  };

  //implementation of dmclock algorithm
  struct SubQueueDMClock {
    private:
      typedef std::map<K, ListPairs > Requests;
      Requests requests;
      unsigned throughput_available, aggregated_p_throughput, osd_max_throughput;
      int64_t size;
      bool purge_required;
      int64_t virtual_clock;
      utime_t when_idled_oldest, when_purged_oldest;

      // structure for deadline-tag for each client
      struct Tag {
	double_t r_deadline, r_spacing;
	double_t p_deadline, p_spacing;
	double_t l_deadline, l_spacing;
	bool active;
	tag_types_t selected_tag;
	K cl;
	SLO slo;
	utime_t when_idled;
	int64_t delta; //total IO done in-between two consecutive request to this cl
	int64_t rho; //total r-IO done in-between two consecutive request to this cl

	Tag(K _cl, SLO _slo) :
	  r_deadline(0), r_spacing(0), p_deadline(0), p_spacing(0), l_deadline(
	      0), l_spacing(0), active(true), selected_tag(
		T_NONE), cl(_cl), slo(_slo), when_idled(ceph_clock_now(NULL)), delta(0), rho(0) {
	      }
	Tag(utime_t t) :
	  r_deadline(t), r_spacing(0), p_deadline(t), p_spacing(0), l_deadline(
	      t), l_spacing(0), active(true), selected_tag(
		T_NONE), when_idled(ceph_clock_now(NULL)), delta(0), rho(0) {
	      }

	Tag(int64_t t) :
	  r_deadline(t), r_spacing(0), p_deadline(t), p_spacing(0), l_deadline(
	      t), l_spacing(0), active(true), selected_tag(
		T_NONE), when_idled(ceph_clock_now(NULL)), delta(0), rho(0) {
	      }
      };
      Tag * oldest_idle_tag;
      typedef std::vector<Tag> Schedule;
      Schedule schedule;

      struct Deadline {
	size_t cl_index;
	double_t deadline;
	bool valid;
	Deadline() :
	  cl_index(0), deadline(0), valid(false) {
	  }
	void set_values(size_t ci, double_t d, bool v = true) {
	  cl_index = ci;
	  deadline = d;
	  valid = v;
	}
      };
      Deadline min_tag_r, min_tag_p;
      std::map<K, size_t> client_map;

      void create_new_tag(K cl, SLO slo) {
	Tag tag(cl, slo);
	if (slo.reserve) {
	  tag.r_deadline = get_current_clock();
	  tag.r_spacing = (double_t) get_osd_max_throughput()
	    / slo.reserve;
	  reserve_r_throughput(slo.reserve);
	}
	if (slo.limit) {
	  assert(slo.limit > slo.reserve);
	  tag.l_deadline = get_current_clock();
	  tag.l_spacing = (double_t) get_osd_max_throughput() / slo.limit;
	}
	if (slo.prop) {
	  reserve_p_throughput(slo.prop);
	  tag.p_deadline =
	    min_tag_p.deadline ?
	    min_tag_p.deadline : get_current_clock();
	}
	client_map[cl] = schedule.size(); //index in vector
	schedule.push_back(tag);
	recalc_prop_spacing();
	find_min_deadlines();
      }

      //straight forward update-rule from paper
      void update_current_tag(size_t cl_index) {
	Tag *tag = &schedule[cl_index];

	if (tag->selected_tag == T_RESERVE) {
	  if (tag->r_deadline)
	    tag->r_deadline = tag->r_deadline + tag->r_spacing;
	}
	if (tag->p_deadline) {
	  tag->p_deadline = tag->p_deadline + tag->p_spacing;
	}
	if (tag->l_deadline) {
	  tag->l_deadline = tag->l_deadline + tag->l_spacing;
	}
	find_min_deadlines();
      }

      // a separate function to update idle tags for better performance.
      //note: the mclock paper makes an error in updating idle client.
      void update_idle_tag(size_t cl_index) {
	double_t now = get_current_clock();
	Tag *tag = &schedule[cl_index];
	tag->active = true;
	if(tag == oldest_idle_tag){ // reset purge_required flag
	    purge_required = false;
	    oldest_idle_tag = NULL;
	}
	if (tag->r_deadline)
	  tag->r_deadline = MAX(tag->r_deadline , now);
	if (tag->p_deadline)
	  tag->p_deadline = MAX(tag->p_deadline , now);
	if (tag->l_deadline)
	  tag->l_deadline = MAX(tag->l_deadline , now);

	find_min_deadlines();
      }

      // the heart of dm_clock algorithm. it sweeps through the schedule vector
      // and identifies minimum R and P tags at current time-stamp.
      // note: this function will be called every time (before dequeue,
      // after client addition, idle client becomes active, etc.). so keep it fast.
      void find_min_deadlines() {
	min_tag_r.valid = min_tag_p.valid = false;
	size_t index = 0;
	for (typename Schedule::iterator it = schedule.begin();
	    it != schedule.end(); ++it, index++) {
	  Tag tag = *it;
	  if (!tag.active)
	    continue;

	  if (tag.r_deadline
	      && ((tag.r_deadline >= tag.l_deadline)
		|| (tag.l_deadline <= get_current_clock()))) {
	    if (min_tag_r.valid) {
	      if (min_tag_r.deadline >= tag.r_deadline)
		min_tag_r.set_values(index, tag.r_deadline);
	    } else {//min_tag_r was not set before
	      min_tag_r.set_values(index, tag.r_deadline);
	    }
	  }

	  if (tag.p_deadline && (tag.l_deadline <= get_current_clock())) {
	    if (min_tag_p.valid) {
	      if (min_tag_p.deadline >= tag.p_deadline)
		min_tag_p.set_values(index, tag.p_deadline);
	    } else {//min_tag_p was not set before
	      min_tag_p.set_values(index, tag.p_deadline);
	    }
	  }
	}
      }

      // if no tag is eligible to pick at current time-stamp
      void issue_idle_cycle() {
	increment_clock();
	find_min_deadlines();
      }

      double_t calc_prop_throughput(double_t prop) const {
	//assert(aggregated_p_throughput !=0 );
	if(throughput_available <= 0)
	  return 0;
	if (aggregated_p_throughput && prop) {
	  if (prop < aggregated_p_throughput)
	    return throughput_available * (prop / aggregated_p_throughput);
	  else
	    return throughput_available;
	}
	return 0;
      }

      void recalc_prop_spacing() {
	double_t prop;
	for (typename Schedule::iterator it = schedule.begin();
	    it != schedule.end(); ++it) {
	  if (it->slo.prop ) { //it->active
	    prop = calc_prop_throughput(it->slo.prop);
	    if(prop > 0)
	      it->p_spacing = (double_t) get_osd_max_throughput() / prop;
	    else // no available throughput
	      it->p_deadline = 0; //disable current P-tag
	  }
	}
      }

      //helper function
      std::string print_iops() {
	std::ostringstream data;
	data << "throughput at " << get_current_clock() << ":" << std::endl;
	for (size_t i = 0; i < schedule.size(); i++)
	  data << "\t cl " << schedule[i].cl << " IOPS :" << schedule[i].delta << std::endl;
	return data.str();
      }

      // helper function
      std::string print_current_tag(tag_types_t tt, int index = -1) {
	std::ostringstream data;
	data << "current time: "<< get_current_clock() << " deadlines:"<<std::endl;
	for (typename Schedule::iterator it = schedule.begin();
	    it != schedule.end(); ++it) {
	  Tag _tag = *it;
	  if (index == (it - schedule.begin())) {
	    if (tt == T_RESERVE)
	      data << "*";
	    if (tt == T_PROP)
	      data << "~";
	    if (tt == T_LIMIT)
	      data << "_";
	  }
	  data <<"\t["<< _tag.r_deadline << " , " << _tag.p_deadline << " , "
	    << _tag.l_deadline << " ]" << std::endl;
	}
	return data.str();
      }

    public:
      SubQueueDMClock(const SubQueueDMClock &other) :
	requests(other.requests), throughput_available(
	other.throughput_available), aggregated_p_throughput(
	other.aggregated_p_throughput), osd_max_throughput(
	other.osd_max_throughput), size(other.size), schedule(
	other.schedule), purge_required(other.purge_required),
	virtual_clock(other.virtual_clock),
	when_idled_oldest(other.when_purged_oldest),
	when_purged_oldest(other.when_purged_oldest),
	oldest_idle_tag(other.oldest_idle_tag){
      }

      SubQueueDMClock() :
	throughput_available(0),
	aggregated_p_throughput(0),
	osd_max_throughput( 0),
	size(0), purge_required(false),
	virtual_clock(1),
	when_idled_oldest(ceph_clock_now(NULL)),
	when_purged_oldest(utime_t(0,0)),
	oldest_idle_tag(NULL){
      }

      int64_t get_current_clock() const{
	//should call ceph_time_now()
	return virtual_clock;
      }

      int64_t increment_clock() {
	return ++virtual_clock;
      }

      void set_osd_max_throughput(unsigned mt) {
	osd_max_throughput = mt;
	throughput_available = mt;
      }

      unsigned get_osd_max_throughput() const {
	return osd_max_throughput;
      }

      unsigned get_available_throughput() const {
	return throughput_available;
      }

      void release_r_throughput(unsigned t) {
	throughput_available += t;
	//if (throughput_available > osd_max_throughput)
	  //throughput_available = osd_max_throughput;
      }

      // need further work, i.e. admission control
      void reserve_r_throughput(unsigned t) {
	// throughput_avaailable could be -ve
	throughput_available -= t;

      }

      void release_p_throughput(unsigned t) {
	aggregated_p_throughput -= t;
	if (aggregated_p_throughput < 0)
	  aggregated_p_throughput = 0;
      }

      void reserve_p_throughput(unsigned t) {
	aggregated_p_throughput += t;
      }

      //if a tag is idle for last >5 sec, it's eligible for purging
      bool is_purge_required() {
	if(purge_required && (oldest_idle_tag != NULL) ){
	  utime_t now = ceph_clock_now(NULL);
	  now -= when_idled_oldest;
	  if(now > utime_t(5,0) )
	    return true;
	  else return false;
	}
	return false;
      }


      void purge_idle_clients(bool has_grace_time = true) {
	typename Schedule::iterator it = schedule.begin();
	utime_t now = ceph_clock_now(NULL);
	utime_t cut_off = utime_t(5,0);
	now -= when_idled_oldest;
	for (size_t index = 0; it != schedule.end();) {
	  if (it->active) {
	    client_map[it->cl] = index++;
	    ++it;
	    continue;
	  }
	  if(has_grace_time && (now - it->when_idled) <= cut_off ){
	    client_map[it->cl] = index++;
	    ++it;
	    continue;
	  }
	  if (it->slo.reserve)
	    release_r_throughput(it->slo.reserve);
	  if (it->slo.prop)
	    release_p_throughput(it->slo.prop);
	  //clean-up
	  requests.erase(it->cl);
	  client_map.erase(it->cl);
	  it = schedule.erase(it);
	}
	oldest_idle_tag = NULL;
	purge_required = false;
	when_purged_oldest = ceph_clock_now(NULL);
	recalc_prop_spacing();
      }

      template<class F>
      void remove_by_filter(F f, std::list<T> *out) {
	  bool _purge_required = false;
	  for (typename Requests::iterator i = requests.begin(); i != requests.end(); ++i) {
	    size -= filter_list_pairs(&(i->second), f, out);
	    if (i->second.empty()) {
	      Tag * tag = &schedule[client_map[i->first]];
	      tag->active = false;
	      tag->when_idled = ceph_clock_now(NULL);
	      _purge_required = true;
	    }
	  }
	  if(_purge_required){
	    purge_idle_clients(false);
	  }
	}

      void remove_by_class(K k, std::list<T> *out) {
	typename Requests::iterator i = requests.find(k);
	if (i == requests.end())
	  return;
	size -= i->second.size();
	if (out) {
	  for (typename ListPairs::reverse_iterator j =
	      i->second.rbegin(); j != i->second.rend(); ++j) {
	    out->push_front(j->second);
	  }
	}
	Tag *tag = schedule[client_map[i->first]];
	tag->active = false;
	tag->when_idled = ceph_clock_now(NULL);
	purge_idle_clients(false);
      }

      Tag* front(size_t &out) {
	assert((size != 0));
	int64_t t = get_current_clock();

	if (min_tag_r.valid) {
	  Tag *tag = &schedule[min_tag_r.cl_index];
	  if (tag->r_deadline <= t) {
	    tag->selected_tag = T_RESERVE;
	    out = min_tag_r.cl_index;
	    return tag;
	  }
	}
	if (min_tag_p.valid) {
	  Tag *tag = &schedule[min_tag_p.cl_index];
	  if (tag->p_deadline) {
	    tag->selected_tag = T_PROP;
	    out = min_tag_p.cl_index;
	    return tag;
	  }
	}
	return NULL;
      }

      //the dm_clock dequeue process
      std::pair<T, tag_types_t> pop_front() {
	assert((size != 0));
	size_t cl_index;
	Tag *tag = front(cl_index);

	//TODO: issue idle cycle
	while (size && tag == NULL) {
	  issue_idle_cycle();
	  tag = front(cl_index);
	}
	//update delta, rho
	tag->delta++;
	if(tag->selected_tag == T_RESERVE)
	  tag->rho++;

	T ret = requests[tag->cl].front().second;
	requests[tag->cl].pop_front();
	if (requests[tag->cl].empty()){
	  tag->active = false;
	  if(!purge_required){
	    purge_required = true;
	    when_idled_oldest = ceph_clock_now(NULL);
	    oldest_idle_tag = tag;
	  }
	}
	// management routine
	increment_clock();
	update_current_tag(cl_index);
	size--;
	return std::make_pair(ret, tag->selected_tag);
      }

      void enqueue(K cl, SLO slo, double cost, T item) {
	bool new_cl = (requests.find(cl) == requests.end());
	if (new_cl) {//new cl
	  create_new_tag(cl, slo);
	} else {//idle or active
	  if (requests[cl].empty()) {//idle
	    update_idle_tag(client_map[cl]);
	  }
	}
	requests[cl].push_back(std::make_pair(cost, item));
	size++;
      }

      void enqueue_front(K cl, SLO slo, double cost, T item) {
	bool new_cl = (requests.find(cl) == requests.end());
	if (new_cl) {
	  create_new_tag(cl, slo);
	} else {
	  if (requests[cl].empty()) {
	    update_idle_tag(client_map[cl]);
	  }
	}
	requests[cl].push_front(std::make_pair(cost, item));
	size++;
      }

      unsigned length() const {
	assert(size >= 0);
	return (unsigned) size;
      }

      bool empty() const {
	return (size == 0);
      }

      void dump(Formatter *f) const {
	f->dump_int("system_throughput", osd_max_throughput);
	f->dump_int("available throughput", throughput_available);
	f->dump_int("num_client", schedule.size());
	f->dump_int("num_map", requests.size());
	f->dump_int("num_req", size);
	f->dump_int("cur_clock", get_current_clock());

	f->open_array_section("dm_queue");
	for (size_t i = 0; i < schedule.size(); i++){
	    f->open_object_section("client_iops");
	    f->open_object_section("client");
	    schedule[i].cl.dump(f);
	    f->close_section(); //client
	    f->dump_bool("active?" , schedule[i].active);
	    f->dump_int("delta" , schedule[i].delta);
	    f->dump_int("rho" , schedule[i].rho);
	    std::ostringstream data;
	    data <<"["<< schedule[i].r_deadline << "(+"<< schedule[i].r_spacing <<"), "
		<< schedule[i].p_deadline << "(+"<< schedule[i].p_spacing <<"), "
	    	    << schedule[i].l_deadline << "(+"<< schedule[i].l_spacing <<")"<< " ]"
		    <<" SLO( "<< schedule[i].slo.reserve <<" , "<< schedule[i].slo.prop
		    <<" , " << schedule[i].slo.limit <<" )";

	    f->dump_string("deadline",  data.str());
	    f->close_section(); //client_iops
	}
	f->open_object_section("purge");
	std::ostringstream data;
	utime_t now = ceph_clock_now(NULL);
	f->dump_bool("purge_required", purge_required);
	data << " last_time_idled: " << when_idled_oldest << std::endl
	    <<" last_time_purged: "<< when_purged_oldest << std::endl
	    << " time_after_being_idle: "<< (now - when_idled_oldest) << std::endl
	    << " time_from_last_purge: "<< (now - when_purged_oldest);

	f->dump_string("purge", data.str());
	f->close_section(); //purge
	f->close_section(); //dm_queue
      }

      // helper function
      std::string print_status() {
	std::ostringstream data;
	data << "osd_max_throughput: " << osd_max_throughput << std::endl;
	data << "avail throughput: " << throughput_available << std::endl;
	data << "num_req: " << size << std::endl;
	data << "num_client: " << schedule.size() << std::endl;
	data << "num_map: " << requests.size() << std::endl;
	data << "clients: \n";
	for(size_t i= 0 ; i <schedule.size(); i++ )
	  data<<"\t"<<schedule[i].cl<<std::endl;
	data << print_iops() << std::endl;
	data << print_current_tag(T_NONE) << std::endl;
	return data.str();
      }
  };

  typedef std::map<unsigned, SubQueue> SubQueues;
  SubQueues high_queue;
  SubQueues queue;
  SubQueueDMClock dm_queue;

  SubQueue *create_queue(unsigned priority) {
    typename SubQueues::iterator p = queue.find(priority);
    if (p != queue.end())
      return &p->second;
    total_priority += priority;
    SubQueue *sq = &queue[priority];
    sq->set_max_tokens(max_tokens_per_subqueue);
    return sq;
  }

  void remove_queue(unsigned priority) {
    assert(queue.count(priority));
    queue.erase(priority);
    total_priority -= priority;
    assert(total_priority >= 0);
  }

  void distribute_tokens(unsigned cost) {
    if (total_priority == 0)
      return;
    for (typename SubQueues::iterator i = queue.begin(); i != queue.end();
	++i) {
      i->second.put_tokens(((i->first * cost) / total_priority) + 1);
    }
  }

  public:
  PrioritizedQueue(unsigned max_per, unsigned min_c, unsigned osd_max_throughput = 0) :
    total_priority(0), max_tokens_per_subqueue(max_per), min_cost(min_c) {
      dm_queue.set_osd_max_throughput(osd_max_throughput);
    }

  unsigned length() const {
    unsigned total = 0;
    for (typename SubQueues::const_iterator i = queue.begin();
	i != queue.end(); ++i) {
      assert(i->second.length());
      total += i->second.length();
    }
    for (typename SubQueues::const_iterator i = high_queue.begin();
	i != high_queue.end(); ++i) {
      assert(i->second.length());
      total += i->second.length();
    }
    total += dm_queue.length();
    return total;
  }

  template<class F>
    void remove_by_filter(F f, std::list<T> *removed = 0) {
      for (typename SubQueues::iterator i = queue.begin(); i != queue.end();
	  ) {
	unsigned priority = i->first;

	i->second.remove_by_filter(f, removed);
	if (i->second.empty()) {
	  ++i;
	  remove_queue(priority);
	} else {
	  ++i;
	}
      }
      for (typename SubQueues::iterator i = high_queue.begin();
	  i != high_queue.end();) {
	i->second.remove_by_filter(f, removed);
	if (i->second.empty()) {
	  high_queue.erase(i++);
	} else {
	  ++i;
	}
      }
    }

  template<class F>
    void remove_by_filter_dmClock(F f, std::list<T> *removed = 0) {
      dm_queue.remove_by_filter(f, removed);

      for (typename SubQueues::iterator i = high_queue.begin();
	  i != high_queue.end();) {
	i->second.remove_by_filter(f, removed);
	if (i->second.empty()) {
	  high_queue.erase(i++);
	} else {
	  ++i;
	}
      }
    }

  void remove_by_class(K k, std::list<T> *out = 0) {
    for (typename SubQueues::iterator i = queue.begin(); i != queue.end();
	) {
      i->second.remove_by_class(k, out);
      if (i->second.empty()) {
	unsigned priority = i->first;
	++i;
	remove_queue(priority);
      } else {
	++i;
      }
    }
    for (typename SubQueues::iterator i = high_queue.begin();
	i != high_queue.end();) {
      i->second.remove_by_class(k, out);
      if (i->second.empty()) {
	high_queue.erase(i++);
      } else {
	++i;
      }
    }
  }

  void remove_by_class_dmClock(K k, std::list<T> *out = 0) {
    dm_queue.remove_by_class(k, out);

    for (typename SubQueues::iterator i = high_queue.begin();
	i != high_queue.end();) {
      i->second.remove_by_class(k, out);
      if (i->second.empty()) {
	high_queue.erase(i++);
      } else {
	++i;
      }
    }
  }

  void enqueue_strict(K cl, unsigned priority, T item) {
    high_queue[priority].enqueue(cl, 0, item);
  }

  void enqueue_strict_front(K cl, unsigned priority, T item) {
    high_queue[priority].enqueue_front(cl, 0, item);
  }

  void enqueue_dmClock(K cl, unsigned slo_reserve, unsigned slo_prop,
      unsigned slo_limit, unsigned cost,  T item) { //int64_t delta, int64_t rho,
    dm_queue.enqueue(cl, SLO(slo_reserve, slo_prop, slo_limit), cost, item);
  }

  void enqueue_front_dmClock(K cl, unsigned slo_reserve, unsigned slo_prop,
      unsigned slo_limit, unsigned cost, T item) { //, int64_t delta, int64_t rho
    dm_queue.enqueue(cl, SLO(slo_reserve, slo_prop, slo_limit), cost, item);
  }

  void enqueue(K cl, unsigned priority, unsigned cost, T item) {
    if (cost < min_cost)
      cost = min_cost;
    if (cost > max_tokens_per_subqueue)
      cost = max_tokens_per_subqueue;
    create_queue(priority)->enqueue(cl, cost, item);
  }

  void enqueue_front(K cl, unsigned priority, unsigned share, T item) {
    if (share < min_cost)
      share = min_cost;
    if (share > max_tokens_per_subqueue)
      share = max_tokens_per_subqueue;

    create_queue(priority)->enqueue_front(cl, share, item);
  }

  bool empty() const {
    assert(total_priority >= 0);
    assert((total_priority == 0) || !(queue.empty()));
    return queue.empty() && high_queue.empty() && dm_queue.empty();
  }

  T dequeue_dmClock(int& tag_type) {
     assert(!empty());
     tag_type = T_NONE;
     if (!(high_queue.empty())) {
       T ret = high_queue.rbegin()->second.front().second;
       high_queue.rbegin()->second.pop_front();
       if (high_queue.rbegin()->second.empty())
 	high_queue.erase(high_queue.rbegin()->first);
       return ret;
     }

     assert(!(dm_queue.empty()));
     std::pair<T, tag_types_t> ret = dm_queue.pop_front();
     tag_type = ret.second;
     return ret.first;
  }

  T dequeue() {
    assert(!empty());

    if (!(high_queue.empty())) {
      T ret = high_queue.rbegin()->second.front().second;
      high_queue.rbegin()->second.pop_front();
      if (high_queue.rbegin()->second.empty())
	high_queue.erase(high_queue.rbegin()->first);
      return ret;
    }

    // if there are multiple buckets/subqueues with sufficient tokens,
    // we behave like a strict priority queue among all subqueues that
    // are eligible to run.
    for (typename SubQueues::iterator i = queue.begin(); i != queue.end();
	++i) {
      assert(!(i->second.empty()));
      if (i->second.front().first < i->second.num_tokens()) {
	T ret = i->second.front().second;
	unsigned cost = i->second.front().first;
	i->second.take_tokens(cost);
	i->second.pop_front();
	if (i->second.empty())
	  remove_queue(i->first);
	distribute_tokens(cost);
	return ret;
      }
    }

    // if no subqueues have sufficient tokens, we behave like a strict
    // priority queue.
    T ret = queue.rbegin()->second.front().second;
    unsigned cost = queue.rbegin()->second.front().first;
    queue.rbegin()->second.pop_front();
    if (queue.rbegin()->second.empty())
      remove_queue(queue.rbegin()->first);
    distribute_tokens(cost);
    return ret;
  }

  //osd deamon or timer will check this condition periodically
  bool purge_required_dmClock(){
    return dm_queue.is_purge_required();
  }

  //purging idle client(s) from internal scheduler
  void purge_dmClock() {
    dm_queue.purge_idle_clients();
  }

  std::string get_status() {
    return dm_queue.print_status();
  }

  void dump(Formatter *f) const {
    f->dump_int("total_priority", total_priority);
    f->dump_int("max_tokens_per_subqueue", max_tokens_per_subqueue);
    f->dump_int("min_cost", min_cost);
    f->open_array_section("high_queues");
    for (typename SubQueues::const_iterator p = high_queue.begin();
	p != high_queue.end(); ++p) {
      f->open_object_section("subqueue");
      f->dump_int("priority", p->first);
      p->second.dump(f);
      f->close_section();
    }
    f->close_section();
    f->open_array_section("queues");
    for (typename SubQueues::const_iterator p = queue.begin();
	p != queue.end(); ++p) {
      f->open_object_section("subqueue");
      f->dump_int("priority", p->first);
      p->second.dump(f);
      f->close_section();
    }
    f->close_section();
    f->open_object_section("dm_clock");
    dm_queue.dump(f);
    f->close_section();
  }

};

#endif
