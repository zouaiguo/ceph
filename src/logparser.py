#!/usr/bin/env python

import re
from numpy import spacing

data = '''
enqueue status: {
    "OSD:ShardedOpWQ:0": {
        "total_priority": 0,
        "max_tokens_per_subqueue": 4194304,
        "min_cost": 65536,
        "high_queues": [],
        "queues": [],
        "dm_clock": {
            "system_throughput": 10,
            "available throughput": 9,
            "num_client": 2,
            "num_map": 2,
            "num_req": 2,
            "dm_queue": [
                {
                    "client": {
                        "type": "mds",
                        "num": 0,
                        "nonce": 17545,
                        "addr": "127.0.0.1:6817"
                    },
                    "active?": false,
                    "delta": 3,
                    "rho": 0,
                    "ratio": 0.000000,
                    "deadline": "cur_time 2015-09-23 12:26:37.270689 || deadline [0.000000 (+0), 2015-09-23 12:26:37.270422 (+0.11), 0.000000 (+0) ] SLO( 0 , 10 , 0 )"
                },
                {
                    "client": {
                        "type": "client",
                        "num": 4115,
                        "nonce": 1017956,
                        "addr": "127.0.0.1:0"
                    },
                    "active?": true,
                    "delta": 2,
                    "rho": 1,
                    "ratio": 0.523810,
                    "deadline": "cur_time 2015-09-23 12:29:49.546327 || deadline [2015-09-23 12:29:50.502921 (+1), 2015-09-23 12:29:51.702920 (+1.1), 0.000000 (+0) ] SLO( 1 , 1 , 0 )"
                }
            ]
        }
    }
}

'''
#########################################################
from datetime import tzinfo, timedelta, datetime
import json;

def get_json_blocks(filename):
    json_blocks = [];
    with open(filename) as f:
            lines = f.readlines();
            json_block = ""
            block_start = False;
            bracket_count = 0;
            for line in lines:
                line  = line.strip()
                if line.endswith('dequeue status: {'):                
                    json_block = '{ "dequeue" : {';
                    bracket_count = 1;
                    block_start = True;
                    continue;
                elif line.endswith('enqueue status: {'):
                    json_block = '{ "enqueue" : {';
                    bracket_count = 1;
                    block_start = True;     
                    continue;           
                elif block_start and line.endswith('{'):
                    bracket_count += 1;
                elif block_start and line.startswith('}'):
                    bracket_count -= 1;                    
                else:
                    pass
                # append lines to json-block
                if block_start:
                    json_block += line
                    if not bracket_count:
                        block_start = False
                        json_block += "}";
                        json_blocks.append(json_block)
                        json_block = None
    return json_blocks;
    

#########################################################        
def parse_deadline(tag, dl):
    dl = dl.strip().split(',');
    i = 0 ; 
    for d in dl:
        d = d.replace("(+","").replace(")","").strip().split(" ");
        #print d     
        spacing = float(d[-1])*1000;        
        dt = parse_datetime(d[0]+" "+d[1]) if spacing else None;                       
        if(i == 0):
            tag.rs = spacing            
            tag.r = dt;            
        elif (i == 1):            
            tag.ps = spacing;
            tag.p = dt;        
        elif (i == 2):
            tag.ls = spacing
            tag.l = dt;                                        
        else:
            print 'error!'
        i += 1;

#########################################################
def parse_slo(tag , slo):
    slo = slo.strip().replace("SLO( ","").replace(' )','').split(",");    
    tag.slo_r = int(slo[0]);
    tag.slo_p = int(slo[1]);
    tag.slo_l = int(slo[2]);        

#########################################################
def parse_datetime(cur_time):
    cur_time = cur_time.strip().split(" ");
    str_d = cur_time[0]; 
    str_t = cur_time[-1];
    
    parts = str_d.strip().split('-');
    #print parts;
    Y = int(parts[0].strip());
    M = int(parts[1].strip());
    D = int(parts[2].strip());
    
    parts = str_t.strip().split(':');
    h = int(parts[0].strip());
    m = int(parts[1].strip());
    ss = float(parts[2].strip());    
    s = int(ss);
    ms = int((ss - s)*1000000)
    return datetime(Y, M, D, h, m, s, ms);

#########################################################

class Tag:
    def __init__(self):
        self.client="";
        self.curr_time = datetime(2015, 9, 20);
        self.r = datetime(2015, 9, 20);
        self.p = datetime(2015, 9, 20);
        self.l = datetime(2015, 9, 20);
        
        self.rs = 0.0
        self.ps = 0.0
        self.ls = 0.0
        self.active = 1;

        self.slo_r = 0;
        self.slo_p = 0;
        self.slo_l = 0;
        self.tag_str = ''
        
    def to_str(self, dt):        
        dif_r = (self.r - dt).total_seconds()*1000 if self.r else '.';
        dif_p = (self.p - dt).total_seconds()*1000 if self.p else '.';
        dif_l = (self.l - dt).total_seconds()*1000 if self.l else '.';    

        return "{} {}--{}({})--{}({})--{}({})".format(self.tag_str, self.active, dif_r, self.rs, dif_p, self.ps, dif_l, self.ls)     
       

class Timestamp:
    def __init__(self, qtype):
        self.nclients = 0;
        self.tags = []
        self.nreq = 0;
        self.dt = None;
        if qtype == "enqueue":
            self.qtype = "-->"
        elif qtype == "dequeue":
            self.qtype = "<--";
        else:
            pass
            
    def to_str(self, dt):        
        dif_t = (self.tags[0].curr_time - dt).total_seconds()*1000;
        s = '{}{}({})| '.format(self.qtype, dif_t, self.nreq);
        for t in self.tags:
            s += t.to_str(dt)
            s += ' ||| ';
        return s;
    
################ main operation ########################   
timestamps = [];
def parse_osd_log(filename):
    last_client="";
    active = ""
    deadline=""
    tag_col = []
    enq = "";
    clients = {}; #clients[client_id] = [tag1, tag2, ...]
    tag = None;
    timestamp = None; 
    rho  = 0;
    json_blocks = get_json_blocks(filename);
    for block in json_blocks:
        val = json.loads(block);                
        qtype = ''
        for _qtype in val:
            timestamp = Timestamp(_qtype);
            qtype = _qtype;
            break;
        dmclock = val[qtype]['OSD:ShardedOpWQ:0']['dm_clock'];
        
        timestamp.nclients = dmclock['num_client'];
        timestamp.nreq = dmclock['num_req'];
        
        for _t in dmclock['dm_queue']:
            t = _t['client'];
            if t['type'] == 'mds':
                continue;
            tag = Tag();
            tag.client = t['num'];
            t = _t;
            tag.active = 1 if t['active?'] else 0;
            if( t['rho'] > rho):
                tag.tag_str='R'
            else:
                tag.tag_str='P'
            
            if qtype == "enqueue":
                tag.tag_str='X'
                
            rho = t['rho'];
                            
            line = t['deadline'].replace('cur_time ',"");
            parts = line.split("||");
            # curr_time                        
            tag.curr_time = parse_datetime(parts[0]);
        
            # deadline part
            others = parts[-1].strip().replace("deadline [","").split(']');            
            parse_deadline(tag, others[0]);
            parse_slo(tag, others[-1]);
            
            # append to timestamp
            timestamp.tags.append(tag);                                
            tag = None;
        
        if timestamp.tags:
            timestamps.append(timestamp);
        timestamp = None;


parse_osd_log('out/osd.0.log')

print len(timestamps);
base_time = timestamps[0].tags[0].curr_time;
print "current time {:%M %S %f}".format(base_time) 
_p = base_time;
temp = _p;
for ts in timestamps:    
    temp = ts.tags[0].curr_time; # 2015-09-23 20:02:33.744436
    print "|\n|\n| ({}) {:%Y-%m-%d %H:%M:%S.%f}|\n|\n|".format((temp -_p).total_seconds()*1000, temp);
    print ts.to_str(base_time);   
    _p = temp;
 	
