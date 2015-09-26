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
#include "common/Timer.h"
#include "include/utime.h"

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

// SLO stands for Service Level Objective of a client
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

struct request_param_t {
  double_t delta;
  double_t rho;
  double_t cost;

  request_param_t(const request_param_t & old):
    delta(old.delta),
    rho(old.rho),
    cost(old.cost){
  }
  request_param_t(double_t d, double_t r, double_t c) {
    delta = d;
    rho = r;
    cost = c;
  }
};


template<typename T, typename K>
class PrioritizedQueue {
  int64_t total_priority;
  int64_t max_tokens_per_subqueue;
  int64_t min_cost;

  //tags that dmClock uses
  enum tag_types_t {
    T_NONE = -1, T_RESERVE = 0, T_PROP, T_LIMIT
  };

  //in C++11, it could be
  //template <typename Record>
  //using ListTypes2 = std::list<std::pair<Record, T> > ;
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

  typedef std::list<std::pair<request_param_t, T> > ListPairsDMColock;

  // this function can be merged with 'filter_list_pairs'
  // using template declaration in C++11 such as,
  // template <class F, class Record>
  // using ListTypes = std::list<std::pair<Record, T> > ;
  // static unsigned filter_list_pairs_dmclock(typename ListPairs<Record> *l, F f, std::list<T> *out)
  template<class F>
    static unsigned filter_list_pairs_dmclock(ListPairsDMColock *l, F f, std::list<T> *out) {
      unsigned ret = 0;
      if (out) {
	for (typename ListPairsDMColock::reverse_iterator i = l->rbegin();
	    i != l->rend(); ++i) {
	  if (f(i->second)) {
	    out->push_front(i->second);
	  }
	}
      }
      for (typename ListPairsDMColock::iterator i = l->begin(); i != l->end();) {
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

  /* implementation of dmClock algorithm */

  // Tag data-structure to hold per-client deadline info
  struct Tag {
    utime_t r_deadline, p_deadline, l_deadline;
    double_t r_spacing, p_spacing, l_spacing;
    bool active;
    tag_types_t selected_tag;
    K cl;
    SLO slo;
    utime_t when_idled, when_assigned;

    // a local count of total IO done for this 'cl'
    double_t delta_local;
    // a remote count of total IO done for this 'cl'
    double_t delta_remote;

    // a local count of total R-IO done for this 'cl'
    double_t rho_local;
    // a remote count of total R-IO done for this 'cl'
    double_t rho_remote;
    // R-tag to P-tag ratio
    double_t r_to_p_ratio;

    Tag(K _cl, SLO _slo) :
      r_deadline(utime_t()), p_deadline(utime_t()), l_deadline(utime_t()),
      r_spacing(0.0), p_spacing(0.0), l_spacing(0.0),
      active(true), selected_tag(T_NONE),
      cl(_cl), slo(_slo),
      when_idled(ceph_clock_now(NULL)),when_assigned(ceph_clock_now(NULL)),
      delta_local(0.0),
      delta_remote(1.0),
      rho_local(0.0),
      rho_remote(1.0),
      r_to_p_ratio(0.0)
      {
    }
    Tag(const Tag & old) :
      r_deadline(old.r_deadline), p_deadline(old.p_deadline), l_deadline(old.l_deadline),
      r_spacing(old.r_spacing), p_spacing(old.p_spacing), l_spacing(old.l_spacing),
      active(old.active), selected_tag(old.selected_tag),
      cl(old.cl), slo(old.slo),
      when_idled(old.when_idled),when_assigned(old.when_assigned),
      delta_local(old.delta_local),
      delta_remote(old.delta_remote),
      rho_local(old.rho_local),
      rho_remote(old.rho_remote),
      r_to_p_ratio(old.r_to_p_ratio){
    }

    bool operator==(Tag &rhs) const {
      return this->cl == rhs.cl;
    }
  };

  struct SubQueueDMClock {
    private:
      typedef std::map<K, ListPairsDMColock > Requests;
      Requests requests;
      unsigned throughput_available, aggregated_p_throughput, osd_max_throughput;
      int64_t size;
      int64_t virtual_clock;
      bool is_avaialble;


      size_t selected_tag_index;
      typedef std::vector<Tag> Schedule;
      Schedule schedule;

      struct Deadline {
	size_t cl_index;
	utime_t deadline;
	bool valid;
	Deadline() :
	  cl_index(0), deadline(utime_t()), valid(false) {
	  }
	void set_values(size_t ci, utime_t d, bool v = true) {
	  cl_index = ci;
	  deadline = d;
	  valid = v;
	}
      };
      Deadline min_tag_r, min_tag_p;
      std::map<K, size_t> client_map; //maps 'cl' to index of 'schedule' vector

      void create_new_tag(K cl, SLO slo) {
	assert((slo.reserve || slo.prop));

	Tag tag(cl, slo);
	utime_t now = get_current_clock();
	if (slo.reserve) {
	  tag.r_spacing = 1.0 / slo.reserve;
	  tag.r_deadline = now;
	  reserve_r_throughput(slo.reserve);
	}
	if (slo.limit) {
	  assert(slo.limit > slo.reserve);
	  tag.l_spacing = 1.0 / slo.limit;
	  tag.l_deadline = now;
	}
	if (slo.prop) {
	  reserve_p_throughput(slo.prop);
	  tag.p_deadline = now;
	}
	client_map[cl] = schedule.size(); //index of vector
	schedule.push_back(tag);
	recalc_prop_spacing();
      }

      // straight forward update-rule from paper
      void update_current_tag(size_t cl_index, request_param_t record) {
	Tag *tag = &schedule[cl_index];
	tag->when_assigned = get_current_clock();
	if (tag->selected_tag == T_RESERVE) {
	  if (tag->slo.reserve)
	    tag->r_deadline.set_from_double(
	      tag->r_deadline + MAX(1.0 , record.rho - tag->rho_local) * tag->r_spacing);
	}
	if (tag->slo.prop) {
	  tag->p_deadline.set_from_double(
	    tag->p_deadline + MAX(1.0 , record.delta - tag->delta_local) * tag->p_spacing);
	}
	if (tag->slo.limit) {
	  tag->l_deadline.set_from_double(
	    tag->l_deadline + MAX(1.0 , record.delta - tag->delta_local) * tag->l_spacing);
	}
      }

      // a separate function to update only idle tags for (i) better performance, and
      // (ii) possible future improvement to give idle-credit to handle client's burstiness
      void update_idle_tag(size_t cl_index, request_param_t record) {
	Tag *tag = &schedule[cl_index];
	tag->active = true;
	utime_t idle_time = get_current_clock();
	idle_time -= tag->when_idled;
	double_t spacing, offset;
	// debug
	tag->when_assigned = get_current_clock();

	if (tag->slo.reserve){
	  spacing = MAX(1.0 , record.rho - tag->rho_local) * tag->r_spacing;
	  offset = (spacing < idle_time) ? fmod(idle_time, spacing) : 0.0;
	  if(tag->selected_tag == T_RESERVE){
	    tag->r_deadline.set_from_double( tag->r_deadline + ((offset) ? idle_time : 0.0 + spacing - offset));
	  }else{
	    tag->r_deadline.set_from_double( tag->r_deadline + ((offset) ? idle_time : 0.0));
	  }
	}
	if (tag->slo.prop){
	  spacing = MAX(1.0 , record.delta - tag->delta_local) * tag->p_spacing;
	  offset = (spacing < idle_time) ? fmod(idle_time, spacing) : 0.0;
	  tag->p_deadline.set_from_double(tag->p_deadline + ((offset) ? idle_time : 0.0 + spacing - offset));
	}
	if (tag->slo.limit){
	  spacing = MAX(1.0 , record.delta - tag->delta_local) * tag->l_spacing;
	  offset = (spacing < idle_time) ? fmod(idle_time, spacing) : 0.0;
	  tag->l_deadline.set_from_double(tag->l_deadline + ((offset) ? idle_time : 0.0 + spacing - offset));
	}
      }

      // the heart of dm_clock algorithm. it sweeps through the schedule vector
      // and identifies minimum R and P tags at current time-stamp.
      // note: this function will be called every time when has_eligible_client()
      // is invoked -- so keep it fast.
      void find_min_deadlines(utime_t now) {
	min_tag_r.valid = min_tag_p.valid = false;
	size_t index = 0;
	for (typename Schedule::iterator it = schedule.begin();
	    it != schedule.end(); ++it, index++) {
	  Tag tag = *it;
	  if (!tag.active)
	    continue;

	  if (tag.slo.reserve
	      && ((tag.r_deadline >= tag.l_deadline)
		|| (tag.l_deadline <= now))) {
	    if (min_tag_r.valid) {
	      if (min_tag_r.deadline >= tag.r_deadline)
		min_tag_r.set_values(index, tag.r_deadline);
	    } else {//min_tag_r was not set before
	      min_tag_r.set_values(index, tag.r_deadline);
	    }
	  }

	  if (tag.slo.prop
	    && (tag.l_deadline <= now)) {
	    if (min_tag_p.valid) {
	      if (min_tag_p.deadline >= tag.p_deadline)
		min_tag_p.set_values(index, tag.p_deadline);
	    } else {//min_tag_p was not set before
	      min_tag_p.set_values(index, tag.p_deadline);
	    }
	  }
	}
      }

      void recalc_prop_spacing() {
	// required when a client dies and frees up its p-reservation
	if(aggregated_p_throughput <= 0)
	  return;

	double_t max_throughput_ = get_osd_max_throughput();
	assert( max_throughput_ > 0);
	for (typename Schedule::iterator it = schedule.begin();
	    it != schedule.end(); ++it) {
	  it->r_to_p_ratio = 0.0;
	  double_t p_throughput = 0.0;
	  if (it->slo.prop ) {
	    p_throughput = 1.0 * max_throughput_ * it->slo.prop / aggregated_p_throughput;
	    it->p_spacing = 1.0 / p_throughput;
	  }
	  // calc r_to_p ratio
	  if(it->slo.reserve){
	    if(it->slo.limit && p_throughput >= it->slo.limit)
	      p_throughput = it->slo.limit;
	    it->r_to_p_ratio = it->slo.reserve / (it->slo.reserve + p_throughput);
	  }
	}
      }

    public:
      SubQueueDMClock(const SubQueueDMClock &other) :
	requests(other.requests), throughput_available(
	other.throughput_available), aggregated_p_throughput(
	other.aggregated_p_throughput), osd_max_throughput(
	other.osd_max_throughput), size(other.size), schedule(
	other.schedule), virtual_clock(other.virtual_clock),
	is_avaialble(other.is_avaialble),
	selected_tag_index(other.selected_tag_index)
	{
      }

      SubQueueDMClock() :
	throughput_available(0),
	aggregated_p_throughput(0),
	osd_max_throughput( 0),
	size(0), virtual_clock(1),
	is_avaialble(false),
	selected_tag_index(0){
      }

      inline utime_t get_current_clock() const{
	//should call ceph_time_now()
	//return virtual_clock;
	return ceph_clock_now(NULL);
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

      // enforce throughput_available >= 0
      // in future, an admission controller will do it.
      void reserve_r_throughput(unsigned t) {
	assert(throughput_available >= t);
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

      void purge_a_client(K cl) {
	// note: iteration over 'schedule' vector is required
	// to recalculate the indices in 'client_map'
      	typename Schedule::iterator it = schedule.begin();
      	for (size_t index = 0; it != schedule.end();) {
	  if(it->cl != cl){
      	    client_map[it->cl] = index++;
      	    ++it;
      	    continue;
      	  }
      	  if (it->slo.reserve)
      	    release_r_throughput(it->slo.reserve);
      	  if (it->slo.prop)
      	    release_p_throughput(it->slo.prop);

      	  requests.erase(it->cl);  // remove from requests-map
	  client_map.erase(it->cl);// remove from client-map
	  it = schedule.erase(it); // remove schedule-vector
      	}
      	recalc_prop_spacing();
      }

      template<class F>
      void remove_by_filter(F f, std::list<T> *out) {
	typename Schedule::iterator it = schedule.begin();
	for (size_t index = 0; it != schedule.end();) {
	  size -= filter_list_pairs_dmclock(&(requests[it->cl]), f, out);
	  if(requests[it->cl].empty()){
	    if (it->slo.reserve)
	      release_r_throughput(it->slo.reserve);
	    if (it->slo.prop)
	      release_p_throughput(it->slo.prop);
	    //clean-up
	    requests.erase(it->cl);  // remove from requests-map
	    client_map.erase(it->cl);// remove from client-map
	    it = schedule.erase(it); // remove schedule-vector
	  }else{
	    client_map[it->cl] = index++;
	    ++it;
	  }
	}
	recalc_prop_spacing();
      }

      void remove_by_class(K k, std::list<T> *out) {
	typename Requests::iterator i = requests.find(k);
	if (i == requests.end())
	  return;
	size -= i->second.size();
	if (out) {
	  for (typename ListPairsDMColock::reverse_iterator j =
	      i->second.rbegin(); j != i->second.rend(); ++j) {
	    out->push_front(j->second);
	  }
	}
	purge_a_client(k);
      }

      bool has_eligible_client() {
	assert((size != 0));
	is_avaialble = false;
	utime_t now = get_current_clock();
	find_min_deadlines(now);

	if (min_tag_r.valid) {
	  Tag* cur_tag = &schedule[min_tag_r.cl_index];
	  if (cur_tag->r_deadline <= now) {
	    cur_tag->selected_tag = T_RESERVE;
	    selected_tag_index = min_tag_r.cl_index;
	    is_avaialble = true;
	    return is_avaialble;
	  }
	}
	if (min_tag_p.valid) {
	    Tag* cur_tag = &schedule[min_tag_p.cl_index];
	  if (cur_tag->p_deadline) {
	    cur_tag->selected_tag = T_PROP;
	    selected_tag_index = min_tag_p.cl_index;
	    is_avaialble = true;
	    return is_avaialble;
	  }
	}

	return is_avaialble;
      }

      // the dm_clock dequeue method
      std::pair<T, Tag> pop_front() {
	// note: make sure the caller first calls has_eligible_client()
	assert(is_avaialble && (size != 0));

	Tag *tag = &schedule[selected_tag_index];
	// update local delta & rho
	tag->when_assigned = get_current_clock();
	tag->delta_local++;
	if(tag->selected_tag == T_RESERVE)
	  tag->rho_local++;

	T ret = requests[tag->cl].front().second;
	requests[tag->cl].pop_front();
	size--;
	// update tag
	if (requests[tag->cl].empty()){
	  tag->active = false;
	  tag->when_idled = get_current_clock();
	}else{
	  request_param_t record = requests[tag->cl].front().first;
	  update_current_tag(selected_tag_index, record);
	}
	// management routine
	is_avaialble = false;
	return std::make_pair(ret, *tag);
      }

      void enqueue(K cl, SLO slo, request_param_t record, T item, bool in_front = false) {
	bool was_empty = false;
	bool new_cl = (requests.find(cl) == requests.end());
	if (new_cl) {
	  create_new_tag(cl, slo);
	}else {
	  was_empty = requests[cl].empty();
	}

	if(in_front){
	  requests[cl].push_front(std::make_pair(record, item));
	} else{
	  requests[cl].push_back(std::make_pair(record, item));
	}
	size++;

	if (was_empty) { // || new_cl
	  update_idle_tag(client_map[cl], record);
	}
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

	f->open_array_section("dm_queue");
	for (size_t i = 0; i < schedule.size(); i++){
	    f->open_object_section("client_iops");
	    f->open_object_section("client");
	    schedule[i].cl.dump(f);
	    f->close_section(); //client
	    f->dump_bool("active?" , schedule[i].active);
	    f->dump_int("delta" , schedule[i].delta_local);
	    f->dump_int("rho" , schedule[i].rho_local);
	    f->dump_float("ratio", schedule[i].r_to_p_ratio);
	    std::ostringstream data;
	    data << "cur_time "<<  ceph_clock_now(NULL)
		<<" || deadline ["<< schedule[i].r_deadline << " (+"<< schedule[i].r_spacing <<"), "
		<< schedule[i].p_deadline << " (+"<< schedule[i].p_spacing <<"), "
		<< schedule[i].l_deadline << " (+"<< schedule[i].l_spacing <<")"<< " ]"
		<<" SLO( "<< schedule[i].slo.reserve <<" , "<< schedule[i].slo.prop
		<<" , " << schedule[i].slo.limit <<" )";

	    f->dump_string("deadline",  data.str());
	    f->close_section(); //client_iops
	}
	f->close_section(); //dm_queue
      }

      //helper function
      std::string print_iops() {
	std::ostringstream data;
	data << "throughput at " << get_current_clock() << ":" << std::endl;
	for (size_t i = 0; i < schedule.size(); i++)
	  data << "\t cl " << schedule[i].cl << " IOPS :" << schedule[i].delta.read() << std::endl;
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
      unsigned slo_limit, int64_t delta, int64_t rho, unsigned cost,  T item) {
    dm_queue.enqueue(cl, SLO(slo_reserve, slo_prop, slo_limit), request_param_t((double_t)delta, (double_t)rho, cost), item);
  }

  void enqueue_front_dmClock(K cl, unsigned slo_reserve, unsigned slo_prop,
      unsigned slo_limit, int64_t delta, int64_t rho, unsigned cost, T item) {
    dm_queue.enqueue(cl, SLO(slo_reserve, slo_prop, slo_limit), request_param_t((double_t)delta, (double_t)rho, cost), item, true);
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

  bool is_avaialbe_dmClock(){
    if(!high_queue.empty())
      return true;
    if(dm_queue.empty())
      return false;
    // check whether any client is eligible
    return dm_queue.has_eligible_client();
  }

  T dequeue_dmClock(int& tag_type, double_t& r_to_p_ratio ) {
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
    std::pair<T, Tag> ret = dm_queue.pop_front();
    tag_type = ret.second.selected_tag;
    r_to_p_ratio = ret.second.r_to_p_ratio;
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

  //purging idle client(s) from internal scheduler
  void purge_dmClock(K cl) {
    dm_queue.purge_a_client(cl);
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
