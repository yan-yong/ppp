#encoding: utf-8
import urllib2, socket, random, Queue, threading, time, sys, thread, json, traceback, threading, os
from __builtin__ import False

def log_error(str):
    time_str = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
    sys.stderr.write('[%s] [%d] [%x] [error] %s\n' % (time_str, os.getpid(), thread.get_ident(), str))
    sys.stderr.flush()

def log_info(str):
    time_str = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
    sys.stdout.write('[%s] [%d] [%x] [info] %s\n' % (time_str, os.getpid(), thread.get_ident(), str))
    sys.stdout.flush()
    
def my_strip(str):
    white_space = [' ', '\t', '\r', '\n']
    beg = 0
    while beg < len(str) and str[beg] in white_space:
        beg += 1
    end = len(str) - 1
    while end >= 0 and str[end] in white_space:
        end -= 1
    return str[beg:end+1]

class Proxy:
    def __init__(self, ip, invalid_min_req_num = 8, invalid_max_req_num = 10, invalid_max_fail_rate = 0.8, type = 'all'):
        item_list = ip.split(':')
        self.m_ip   = ip
        self.m_is_foreign = None
        assert(len(item_list) > 1)
        self.m_ip   = item_list[0]
        self.m_port = item_list[1]
        assert(len(self.m_ip) > 0 and len(self.m_port) > 0)
        assert(self.m_ip.count('.') == 3 and self.m_port.isdigit())
        if len(item_list) > 2:
            self.m_is_foreign = int(item_list[2])
        self.m_type = type
        self.m_fetch_time = 0
        self.m_suc_num = 0
        self.m_fail_num = 0
        self.m_invalid_min_req_num   = invalid_min_req_num
        self.m_invalid_max_req_num   = invalid_max_req_num
        self.m_invalid_max_fail_rate = invalid_max_fail_rate
    def __cmp__(self, other):
        return self.m_fetch_time - other.m_fetch_time
    def __str__(self):
        if self.m_is_foreign is None:
            return '%s:%s:%d' % (self.m_ip, self.m_port, 0)
        return '%s:%s:%d' % (self.m_ip, self.m_port, int(self.m_is_foreign))
    def __hash__(self):
        return ('%s:%s' % (self.m_ip, self.m_port)).__hash__()
    def __eq__(self, other):
        return self.m_ip == other.m_ip and self.m_port == other.m_port 
    def is_foreign(self):
        return self.m_is_foreign == 1
    def update_fetch_time(self):
        self.m_fetch_time = time.time()
    def get_handler(self):
        self.m_fetch_time = time.time()
        handler_str = {}
        if self.m_type == 'all' or self.m_type == 'http':
            handler_str['http'] = '%s:%s' % (self.m_ip, self.m_port)
        if self.m_type == 'all' or self.m_type == 'https':
            handler_str['https'] = '%s:%s' % (self.m_ip, self.m_port)
        return urllib2.ProxyHandler(handler_str)
    def add_success(self):
        self.m_suc_num += 1
    def add_fail(self): 
        self.m_fail_num += 1
    def is_invalid(self):
        res = False
        req_num = self.m_suc_num + self.m_fail_num
        if req_num >= self.m_invalid_min_req_num and self.m_fail_num > req_num * self.m_invalid_max_fail_rate:
            res = True
        if req_num >= self.m_invalid_max_req_num:
            self.m_fail_num = 0
            self.m_suc_num = 0 
        return res
    def locate_ip(self):
        if self.m_is_foreign is not None:
            return
        ip_addr = '%s:%s' % (self.m_port, self.m_port)
        ip_loc_url_list = [\
            'http://ip.taobao.com/service/getIpInfo.php?ip=', \
            'http://int.dpool.sina.com.cn/iplookup/iplookup.php?format=json&ip=' \
        ]
        retry_num = 4
        for i in range(retry_num):
            loc_idx = random.randint(0, len(ip_loc_url_list)-1)
            loc_url = ip_loc_url_list[loc_idx] + self.m_ip
            try:
                html_str = urllib2.urlopen(loc_url).read()
                val_dict = json.loads(html_str)
                if loc_idx == 0:
                    self.m_is_foreign = (val_dict['data'])['country_id'] != 'CN'
                elif loc_idx == 1:
                    country  = val_dict['country'].encode('utf-8')
                    province = val_dict['province'].encode('utf-8')
                    self.m_is_foreign = country != '中国' or province == '香港' or province == '澳门' or province == '台湾'
                break           
            except Exception, err:
                log_error('request ip location %s error: %s, retry_times: %d' % (loc_url, err, i))

class Course:
    def __init__(self, course_id, req_list, ordered, timeout_sec):
        self.m_course_id = course_id
        self.m_condition = threading.Condition()
        self.m_cnt = len(req_list)
        self.m_res_lst = [(None,None,None,None) for x in range(self.m_cnt)]
        self.m_ordered = ordered
        self.m_idx = 0
        self.m_waiting = False
        self.m_timeout_sec = timeout_sec
        self.m_start_time = time.time()
        self.m_is_timeout = False
    def __str__(self):
        return 'Course %d' % self.m_course_id
    def __hash__(self):
        return self.m_course_id
    def __eq__(self, other):
        return self.m_course_id == other.m_course_id 
    def timeout_remain_sec(self):
        cur_time = time.time()
        timeout_stamp = self.m_start_time + self.m_timeout_sec
        if timeout_stamp > cur_time:
            return timeout_stamp - cur_time
        return 0
    def cancel(self):
        self.m_idx = self.m_cnt
    def is_finish(self):
        return self.m_idx == self.m_cnt
    def generator(self):
        while self.m_idx < len(self.m_res_lst):
            res = None
            self.m_condition.acquire()
            if not self.m_is_timeout:
                self.m_waiting = True
                while not self.m_is_timeout and self.m_res_lst[self.m_idx][0] is None:
                    cur_time = time.time()
                    if cur_time > self.m_start_time + self.m_timeout_sec:
                        log_info('Course %d timeout.' % self.m_course_id)
                        self.m_is_timeout = True
                        break
                    wait_sec = self.m_start_time + self.m_timeout_sec - cur_time
                    self.m_condition.wait(wait_sec)
                self.m_waiting = False
            res = self.m_res_lst[self.m_idx]
            self.m_idx += 1
            self.m_condition.release()
            log_info('Course %d %d/%d' % (self.m_course_id, self.m_idx - 1, self.m_cnt))
            yield res
            
    def put(self, idx, res):
        if res is None:
            log_error('Course %d recv none.' % self.m_course_id)
        if idx >= len(self.m_res_lst):
            log_error('Course %d put idx extend: %d %d' % (self.m_course_id, idx, self.m_cnt))
            return
        if self.m_idx >= len(self.m_res_lst):
            log_error('Course %d put m_idx extend: %d %d' % (self.m_course_id, self.m_idx, self.m_cnt))
            return
        self.m_condition.acquire()
        if not self.m_ordered:
            idx = self.m_idx
        while idx < self.m_cnt and self.m_res_lst[idx][0] is not None:
            idx += 1
        if idx >= self.m_cnt:
            self.m_condition.release()
            log_error('Course %d enough, count %d, skip put %d.' % (self.m_course_id, self.m_cnt, idx))
            return
        self.m_res_lst[idx] = res
        log_info('put Course success %d/%d' % (idx, len(self.m_res_lst)) )
        if self.m_res_lst[self.m_idx][0] is not None and self.m_waiting:
            self.m_condition.notify()
        self.m_condition.release()

def test_course(course):
    course.put(0, '0', '0')
    time.sleep(2)
    course.put(2, '2', '2')
    time.sleep(2)
    course.put(1, '1', '1')

if __name__ == "__main__":
    req_lst = [i for i in range(3)]
    course = Course(1, req_lst, True, 30)
    thd = threading.Thread(target=test_course, args=(course,))
    thd.start()
    for i in course.generator():
        print i