#encoding: utf-8
import urllib2, socket, random, Queue, threading, time, sys, thread, select
from proxy_common import *

class Priority:
    VeryLow  = 10
    Low      = 8
    Normal   = 4
    High     = 2
    VeryHigh = 1 
    Urgent   = 0

class ProxySpider:
    fetch_try_num = 5
    proxy_fetch_interval_sec = 1
    proxy_server_reconnect_interval_sec = 10
    def __init__(self, proxy_server_ip = '127.0.0.1', proxy_server_port = 3456, \
                 internal_thread_num = 1, foreign_thread_num = 1, \
                 proxy_concurency = False, socket_timeout = 30, max_timeout = 86400):
        self.m_socket_timeout = socket_timeout
        self.m_max_timeout = max_timeout
        socket.setdefaulttimeout(self.m_socket_timeout)
        self.m_proxy_server_ip   = proxy_server_ip
        self.m_proxy_server_port = proxy_server_port
        self.m_socket = None
        if not self.__connect_proxy_server():
            log_error('cannot connnect to proxy server (%s:%d)' % (self.m_proxy_server_ip, self.m_proxy_server_port))
            sys.exit(1)
        '''存放proxy，按时间排序的优先级队列'''
        self.m_internal_proxy_queue = Queue.PriorityQueue()
        self.m_foreign_proxy_queue   = Queue.PriorityQueue()
        '''存放请求的队列'''
        self.m_internal_request_queue = Queue.PriorityQueue()
        self.m_foreign_request_queue = Queue.PriorityQueue()
        self.m_proxy_concurency = proxy_concurency
        self.m_result_queue  = Queue.Queue()
        self.m_exit = False
        self.m_internal_thd_lst = []
        self.m_foreign_thd_lst = []
        self.m_proxy_set = set()
        self.m_foreign_cnt = 0
        self.m_internal_cnt = 0
        self.m_course_set = set()
        self.m_course_idx = 0
        self.m_lock = threading.Lock()
        '''国外代理与国内代理分开处理'''
        for i in range(foreign_thread_num):
            thd = threading.Thread(target=self.fetch_runtine, args=(self.m_foreign_proxy_queue, self.m_foreign_request_queue))
            self.m_foreign_thd_lst.append(thd)
        for i in range(internal_thread_num):
            thd = threading.Thread(target=self.fetch_runtine, args=(self.m_internal_proxy_queue, self.m_internal_request_queue))
            self.m_internal_thd_lst.append(thd)
        self.m_sync_thd = threading.Thread(target=self.sync_runtine)
        self.m_recv_content = ''
    def __get_course(self, request):
        is_timeout = False
        if not hasattr(request, 'm_course'):
            return None, is_timeout
        course = request.m_course
        self.m_lock.acquire()
        is_timeout = not (course in self.m_course_set)
        self.m_lock.release()
        return course, is_timeout
    def __remove_course(self, course):
        self.m_lock.acquire()
        ret = True
        try:
            self.m_course_set.remove(course)
        except:
            ret = False
        self.m_lock.release()
        return ret
    def __remain_timeout_sec(self, request):
        course, is_timeout = self.__get_course(request)
        if is_timeout:
            return -1
        if course is not None:
            return course.timeout_remain_sec()
        cur_time = time.time()
        start_time = cur_time
        if hasattr(request, 'm_start_time'):
            start_time = request.m_start_time
        timeout_stamp = start_time + self.m_max_timeout
        if hasattr(request, 'm_timeout_sec'):
            timeout_stamp = start_time + request.m_timeout_sec
        if timeout_stamp > cur_time:
            return timeout_stamp - cur_time
        return 0
    def __request_timeout_sec(self, request):
        timeout_sec = self.m_socket_timeout
        remain_sec = self.__remain_timeout_sec(request)
        if remain_sec < timeout_sec:
            timeout_sec = remain_sec
        return timeout_sec
    def __connect_proxy_server(self):
        try:
            if self.m_socket is not None:
                self.m_socket.close()
                self.m_socket = None
            self.m_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.m_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.m_socket.connect((self.m_proxy_server_ip, self.m_proxy_server_port))
            self.m_socket.setblocking(False)
            log_error('connect proxy server(%s:%d) success.' % (self.m_proxy_server_ip, self.m_proxy_server_port))
            return True
        except Exception, err:
            log_error('connect proxy server(%s:%d) error: %s' % (self.m_proxy_server_ip, self.m_proxy_server_port, err))
            return False
    def __remove_proxy(self, proxy):
        self.m_lock.acquire()
        if proxy in self.m_proxy_set:
            self.m_proxy_set.remove(proxy)
            log_info('delete %s from server msg.' % str(proxy))
        else:
            log_info('cannot find delete msg %s from server.' % str(proxy))
        self.m_lock.release()
    def __add_proxy(self, proxy):
        self.m_lock.acquire()
        if proxy in self.m_proxy_set:
            self.m_lock.release()
            log_info('recv duplicate proxy %s' % str(proxy))
            return
        self.m_proxy_set.add(proxy)
        self.m_lock.release()
        if proxy.is_foreign():
            self.m_foreign_cnt += 1
        else:
            self.m_internal_cnt += 1
        '''只有开了境外数据处理线程时, 才放入境外proxy中'''
        if proxy.is_foreign() and len(self.m_foreign_thd_lst) > 0:
            self.m_foreign_proxy_queue.put(proxy)
        else:
            self.m_internal_proxy_queue.put(proxy)
    def __should_cancel(self, request):
        if hasattr(request, 'm_cancel_callback'):
            try:
                return request.m_cancel_callback(request)
            except Exception, err:
                log_error('cancel exception: %s, %s\n%s\n' % (err, request.get_full_url(), traceback.format_exc()))
        return False
    def __proxy_invalid(self, proxy):
        is_invalid = True
        self.m_lock.acquire()
        if proxy in self.m_proxy_set:
            is_invalid = proxy.is_invalid()
            if is_invalid:
                log_error('remove invalid proxy: %s, high fail rate: %d %d' % \
                          (str(proxy), proxy.m_suc_num, proxy.m_fail_num))
                self.m_proxy_set.remove(proxy)
                if proxy.is_foreign():
                    self.m_foreign_cnt -= 1
                else:
                    self.m_internal_cnt += 1
        else:
            log_info('skip deleted proxy: %s %s %s' % (proxy.m_type, proxy.m_ip, proxy.m_port))
        self.m_lock.release()
        return is_invalid
    def __handle_result(self, response, response_headers, request, proxy):
        '''统计proxy成功率'''
        if proxy is not None:
            if response is not None:
                proxy.add_success()
            else:
                proxy.add_fail()
        course, course_timeout = self.__get_course(request)
        if course_timeout:
            '''来源于同步抓取，但是course超时了，直接把结果扔了'''
            pass
        elif course is not None:
            '''来源于同步抓取，返回给相应的同步抓取course'''
            course.put(request.m_course_seq, (response, response_headers, request, proxy))
        elif hasattr(request, 'm_callback'):
            '''来源于异步抓取，执行相应的回调'''
            try:
                callback = request.m_callback
                callback(response, response_headers, request, proxy)
            except Exception, err:
                log_error('process callback exception: %s, %s\n%s\n' % (err, request.get_full_url(), traceback.format_exc()))
        else:
            '''其它来源，扔入结果队列，外层需要get结果队列'''
            self.m_result_queue.put((response, response_headers, request, proxy))
    def __get_proxy_and_request(self, proxy_queue, request_queue):
        proxy   = None
        request = None
        while True:
            priority, request = request_queue.get()
            url = request.get_full_url()
            if self.__should_cancel(request):
                log_error('cancel callback canceled %s' % url)
                self.__handle_result(None, None, request, None)
                continue
            '''  ******       获取proxy   *******          '''
            if hasattr(request, 'm_proxy') and not self.__proxy_invalid(request.m_proxy):
                '''如果固定proxy, 则只使用指定的proxy'''
                proxy = request.m_proxy
            else:
                '''否则从队列里拿一个proxy'''
                timeout_sec = self.__remain_timeout_sec(request)
                while (proxy is None or self.__proxy_invalid(proxy)) and timeout_sec > 0:
                    try:
                        proxy = proxy_queue.get(True, timeout_sec)
                    except:
                        pass
                    timeout_sec = self.__remain_timeout_sec(request)
                if timeout_sec <= 0:
                    proxy = None
                elif self.m_proxy_concurency:
                    '''立即归还proxy'''
                    proxy_queue.put(proxy)
            '''获取proxy失败'''                  
            if proxy is None:
                log_error('%s failed, acquire proxy failed' % url)
                self.__handle_result(None, None, request, proxy)
                continue
            request_timeout = self.__request_timeout_sec(request)
            proxy.update_fetch_time()
            return proxy, request, request_timeout, priority
        return None, None, None, None
    def __create_course(self, request_list, ordered, timeout_sec):
        self.m_lock.acquire()
        course = Course(self.m_course_idx, request_list, ordered, timeout_sec)
        self.m_course_set.add(course)
        self.m_course_idx += 1
        self.m_lock.release()
        return course        
    def fetch_runtine(self, proxy_queue, request_queue):
        while True:
            res = None
            proxy, request, req_timeout, priority = self.__get_proxy_and_request(proxy_queue, request_queue)
            if proxy is None:
                break
            handler = proxy.get_handler()
            opener = urllib2.build_opener(handler)
            log_info('request %s, use proxy %s %s %s' % (request.get_full_url(), proxy.m_type, proxy.m_ip, proxy.m_port))
            try:
                fid = opener.open(request, timeout=req_timeout)
                res_header = fid.info()
                res_cont   = fid.read()
                '''不允许proxy并发 且 请求不指定固定的proxy时，下载完后应该归还proxy'''
                if not self.m_proxy_concurency and not hasattr(request, 'm_proxy'):
                    proxy_queue.put(proxy)
                self.__handle_result(res_cont, res_header, request, proxy)
                log_info('download %s success.' % request.get_full_url())
            except Exception, err:
                '''不允许proxy并发 且 请求不指定固定的proxy时，下载完后应该归还proxy'''
                if not self.m_proxy_concurency and not hasattr(request, 'm_proxy'):
                    proxy_queue.put(proxy)
                log_error('download %s error: %s' % (request.get_full_url(), err))
                '''重试'''
                if self.__remain_timeout_sec(request) > 0:
                    request_queue.put((priority, request))
                else:
                    log_error('%s failed' % request.get_full_url())
                    self.__handle_result(None, None, request, proxy)
    def __handle_rcv_data(self, data):
        idx = data.find('@end@')
        if idx < 0:
            self.m_recv_content += data
            return
        self.m_recv_content += data[0:idx]
        proxy_list = self.m_recv_content.split(',')
        self.m_recv_content = data[idx+5:]
        if len(proxy_list) == 0:
            log_error('receive 0 item proxy.')
            return
        for proxy_val in proxy_list:
            try:
                item_list = proxy_val.split(':')
                assert(len(item_list) == 4)
                op_val = item_list[0]
                assert(op_val == 'A' or op_val == 'D')
                proxy = Proxy(':'.join(item_list[1:]))
                if op_val == 'A':
                    self.__add_proxy(proxy)
                else:
                    self.__remove_proxy(proxy)
            except:
                log_error('invalid proxy %s' % proxy_val)
        log_info('proxy num: %d, foreign: %d, internal: %d' % (len(self.m_proxy_set), self.m_foreign_cnt, self.m_internal_cnt))
    '''与proxyserver同步的线程'''
    def sync_runtine(self):
        last_connect_time = 0
        while not self.m_exit:
            try:
                if self.m_socket is None:
                    log_error('reconnecting to proxy server(%s:%d)' % (self.m_proxy_server_ip, self.m_proxy_server_port))
                    self.__connect_proxy_server()
                read_lst = [self.m_socket.fileno()]
                rlist, wlist, _ = select.select(read_lst, [], [], 1)
                if self.m_socket.fileno() in rlist:
                    data = self.m_socket.recv(2048)
                    log_info('recv %d bytes from proxy server(%s:%d)' % (len(data), self.m_proxy_server_ip, self.m_proxy_server_port))
                    self.__handle_rcv_data(data)
            except Exception, err:
                log_error('sync from proxy server(%s:%d) error: %s' % (self.m_proxy_server_ip, self.m_proxy_server_port, err))
                time.sleep(self.proxy_server_reconnect_interval_sec)
                self.m_socket = None
    def start(self):
        for thd in self.m_internal_thd_lst:
            thd.start()
        for thd in self.m_foreign_thd_lst:
            thd.start()
        self.m_sync_thd.start()
    def exit(self):
        self.m_exit = True
        for thd in self.m_internal_thd_lst:
            thd.join()
        del(self.m_internal_thd_lst)
        for thd in self.m_foreign_thd_lst: 
            thd.join()
        del(self.m_foreign_thd_lst)
    def cancel_sync_generator(self, request):
        course = self.__get_course(request)
        course.cancel()
    def __put_request(self, request, internal, proxy = None, priority = Priority.Normal):
        request.m_start_time = time.time()
        if proxy is not None:
            request.m_proxy = proxy;
        if internal:
            self.m_internal_request_queue.put((priority, request))
        else:
            self.m_foreign_request_queue.put((priority, request))
    '''同步抓取'''
    def sync_generator(self, request_list, ordered, timeout_sec, \
            internal = True, proxy = None, priority = Priority.Normal):
        course = self.__create_course(request_list, ordered, timeout_sec)
        for i in range(len(request_list)):
            request = request_list[i]
            request.m_course_id  = course.m_course_id
            request.m_course_seq = i
            if proxy is not None:
                request.m_proxy = proxy;
            self.__put_request(request, internal, proxy, priority)
        for res, res_header, req, proxy in course.generator():
            yield res, res_header, req, proxy
        self.__remove_course(course)
    '''异步抓取'''
    '''callback(response_content, response_header, request, proxy)'''
    def async_fetch(self, request, callback, internal = True, proxy = None, timeout_sec = 0, priority = Priority.Normal):
        request.m_callback = callback
        request.m_timeout_sec = timeout_sec
        if proxy is not None:
            request.m_proxy = proxy;
        self.__put_request(request, internal, proxy, priority) 
    def get_result(self):
        while not self.m_exit:
            try:
                result = self.m_result_queue.get(True, 1)
                return result
            except:
                continue
        return None
    def qsize(self):
        return self.m_internal_request_queue.qsize() + self.m_foreign_request_queue.qsize()
    def wait(self):
        for thd in self.m_foreign_thd_lst:
            thd.join()
        for thd in self.m_internal_thd_lst:
            thd.join()

one_proxy = None
def handle_result(res_html, res_header, request, proxy):
    print 'len: %d, %s  proxy:%s' % (len(res_html), request.get_full_url(), str(proxy))
    global one_proxy
    one_proxy = proxy

def main():
    proxy_spider = ProxySpider(proxy_server_ip='172.19.32.91', internal_thread_num=20, foreign_thread_num=1, socket_timeout=20) 
    proxy_spider.start()
    google_headers = { \
                        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.65 Safari/537.36", \
                        "Cookie": "PREF=ID=c1c3b83eb56f31b6:U=3e390a81539d4359:FF=2:LD=en:TM=1416548753:LM=1420966430:S=3NfN0ID4akN4rgWI; NID=67=Kg_HA3Hm0iL9oldWCff02Gkez_Qym0vySziNHElxgC2fOB5ndwcGmC5bl9rRzKcgyzxp8FF-AQkc4w_BcNPyOhegwwftol1kQXp3E9KXaT0NpP4lU0EBjwQft3dJXqcz" \
                      }
    google_request = urllib2.Request('https://www.google.com.hk/search?safe=strict&site=&source=hp&q=%E5%AD%94%E5%AD%90%E5%AD%A6%E9%99%A2&oq=%E5%AD%94%E5%AD%90%E5%AD%A6%E9%99%A2&bav=on.2,or.&bvm=bv.83640239,d.dGc&fp=43093b7db664cbb8&biw=1440&bih=495&dpr=1&tch=1&ech=1&psi=QFC2VNyLM4X78QWisIDYBw.1421234239657.3', \
                                     headers = google_headers)
    
    proxy_spider.async_fetch(urllib2.Request('http://www.sogou.com/'), handle_result, internal = True, priority = 2, timeout_sec = 20)
    global one_proxy
    while one_proxy is None:
        time.sleep(1)
    proxy_spider.async_fetch(urllib2.Request('http://www.baidu.com/'), handle_result, internal = True, proxy = one_proxy, 
        priority = 1, timeout_sec = 20)

    '''
    for i in range(100):
        proxy_spider.async_put(google_request, internal = False)
        #proxy_spider.async_put(urllib2.Request('http://www.sogou.com/'))
        #proxy_spider.async_put(urllib2.Request('http://www.baidu.com/s?wd=%E9%99%88%E5%85%A8%E5%9B%BD&ie=utf-8'))
        #proxy_spider.async_put(urllib2.Request('http://www.sogou.com/sogou?query=%E9%99%88%E5%9B%BD%E5%85%A8'))
    suc_cnt = 0
    while True:
        res = proxy_spider.get_result()
        suc_cnt += 1
        html_len = len(res[0])
        print 'SUCCESS: %d LEN:%d' % (suc_cnt, html_len)
    '''
    
if __name__ == '__main__':
    stderr = sys.stderr
    stdout = sys.stdout
    reload(sys)
    sys.setdefaultencoding('utf-8')
    sys.stderr = stderr
    sys.stdout = stdout
    main()
