#encoding: utf-8
import urllib2, socket, random, Queue, threading, time, sys, thread, lxml, lxml.html, errno
from proxy_common import *
import select

g_invalid_min_req_num = 4
g_invalid_max_req_num = 4
g_invalid_min_fail_rate = 0.5

class SiteSource:
    default_fetch_page_num = 10
    default_fetch_interval_sec = 20
    overload_queue_size = 800
    def __init__(self, result_queue, url_pattern, ip_xpath, port_xpath, from_page, end_page, \
                 fetch_interval_sec, is_foreign):
        self.m_url_pattern = url_pattern
        self.m_ip_xpath = ip_xpath
        self.m_port_xpath = port_xpath
        self.m_start_page = from_page
        self.m_end_page = end_page
        if self.m_end_page < self.m_start_page:
            self.m_end_page = self.m_start_page + self.default_fetch_page_num
        self.m_fetch_interval_sec = self.default_fetch_interval_sec
        if fetch_interval_sec > 0:
            self.m_fetch_interval_sec = fetch_interval_sec
        self.m_url_lst = []
        self.m_last_fetch_time = 0
        for i in xrange(self.m_start_page, self.m_end_page + 1):
            self.m_url_lst.append(self.m_url_pattern.replace('{0}', str(i)))
        self.m_first_url = self.m_url_pattern.replace('{0}', str(self.m_start_page))
        self.m_thd = None
        self.m_result_queue = result_queue
        self.m_is_foreign = is_foreign
        self.m_http_headers = {"User-Agent":"Mozilla/5.0 (Windows NT 6.1; rv:34.0) Gecko/20100101 Firefox/34.0"}
    def set_http_header(self, http_header):
        self.m_http_headers = http_header
    def __fetch_source_page(self, url):
        cur_time = time.time()
        html_str = None
        try:
            log_info('request %s' % url)
            request = urllib2.Request(url, headers = self.m_http_headers)
            if self.m_fetch_interval_sec + self.m_last_fetch_time > cur_time:
                time.sleep(self.m_fetch_interval_sec + self.m_last_fetch_time - cur_time)
            self.m_last_fetch_time = time.time()
            html_str = urllib2.urlopen(request).read()
            log_info('download %s success' % url)
        except Exception, err:
            log_error('download %s failed: %s' % (url, err))
        return html_str
    def __obtain_addr_list(self, url):
        while self.m_result_queue.qsize() > self.overload_queue_size:
            log_info('wait for overload verifying queue size %d.' % self.m_result_queue.qsize())
            time.sleep(10)
        html_str = self.__fetch_source_page(url)
        if html_str is None:
            return
        try:
            html = lxml.html.document_fromstring(html_str)
            ip_list = html.xpath(self.m_ip_xpath)
            if len(ip_list) == 0:
                log_error('%s: find 0 items with ip xpath' % url)
                return
            if len(self.m_port_xpath) != 0:
                port_list = html.xpath(self.m_port_xpath)
                if len(port_list) == 0:
                    log_error('%s: find 0 items with port xpath', url)
                    return
                if len(port_list) != len(ip_list):
                    log_error('%s: ip items num %d != port items num %d' % \
                              (url, len(ip_list), len(port_list)))
                    return
                for i in range(len(ip_list)):
                    if ip_list[i].find(':') > 0:
                        log_error('%s: skip invalid ip %s:%s' % (url, ip_list[i], port_list[i]))
                        continue
                    ip_list[i] = '%s:%s' % (ip_list[i], port_list[i])
                if self.m_is_foreign >= 0:
                    ip_list[i] = '%s:%d' % (ip_list[i], self.m_is_foreign)
            cnt = 0
            for ip in ip_list:
                if ip.find(':') <= 0:
                    log_error('%s: skip invalid ip %s' % (url, ip_list[i]))
                    continue
                ip = my_strip(ip)
                global g_invalid_min_req_num, g_invalid_max_req_num, g_invalid_min_fail_rate
                self.m_result_queue.put(Proxy(ip, invalid_min_req_num = g_invalid_min_req_num, invalid_max_req_num = g_invalid_max_req_num, invalid_max_fail_rate = g_invalid_min_fail_rate))
                cnt += 1
            log_info('%s: found %d new items.' % (url, cnt))
        except Exception, err:
            log_error('%s: exception %s' % (url, err))      
    def run(self):
        last_fetch_time = 0
        '''第一次刷全量页面'''
        for url in self.m_url_lst:
            self.__obtain_addr_list(url)
        while True:
            '''后面只需刷第一页即可'''
            self.__obtain_addr_list(self.m_first_url)         
    def start(self):
        self.m_thd = threading.Thread(target=self.run)
        self.m_thd.start()

class SyncClient:
    addr_seperate = ','
    def __init__(self, cur_socket, socket_address, each_send_cnt = 30, \
                 max_send_interval_sec = 20, retry_times = 3):
        self.m_socket = cur_socket
        self.m_socket_addr = socket_address
        self.m_last_sync_time = 0
        self.m_addr_list = []
        self.m_each_send_cnt = each_send_cnt
        self.m_max_retry_num = retry_times
        self.m_cur_retry_num = 0
        self.m_max_send_interval_sec = max_send_interval_sec
        self.m_closed = False
    def ready_to_send(self):
        if time.time() - self.m_last_sync_time > self.m_max_send_interval_sec:
            return len(self.m_addr_list) > 0
        return len(self.m_addr_list) >= self.m_each_send_cnt
    def add_proxy(self, proxy):
        self.m_addr_list.append('A:' + str(proxy))
    def add_proxy_list(self, proxy_list):
        for proxy in proxy_list:
            self.add_proxy(proxy)
    def delete_proxy(self, proxy):
        add_addr = 'A:' + str(proxy)
        del_addr = 'D:' + str(proxy)
        try:
            idx = self.m_addr_list.index(add_addr)
            del(self.m_addr_list[idx])
        except:
            log_info('del msg: %s' % del_addr)
            self.m_addr_list.append(del_addr)
    def __remain_finish(self, send_cnt):
        return len(self.m_addr_list) < self.m_each_send_cnt + send_cnt
    def send(self):
        try:
            send_cnt = len(self.m_addr_list)
            if send_cnt > self.m_each_send_cnt:
                send_cnt = self.m_each_send_cnt
            content = self.addr_seperate.join(self.m_addr_list[0:send_cnt])
            if self.__remain_finish(send_cnt):
                content += '@end@'
            else:
                content += ','
            self.m_last_sync_time = time.time()
            send_bytes_num = self.m_socket.send(content)
            if send_bytes_num == len(content):
                del(self.m_addr_list[0:send_cnt])
                log_info('send %s proxies to %s' % (send_cnt, self.m_socket_addr))
            else:
                log_info('notice: only send %d/%d bytes to %s, resend.' % \
                         (send_bytes_num, len(content), self.m_socket_addr))
            self.m_cur_retry_num = 0
            return True
        except Exception, err:
            self.m_cur_retry_num += 1
            log_error('send to %s error: %s' % (self.m_socket_addr, err))
        return self.m_cur_retry_num < self.m_max_retry_num
    def fileno(self):
        return self.m_socket.fileno()
    def close(self):
        self.m_socket.close()
        self.m_closed = True
    
class ProxyServer:
    socket_timeout_sec = 30
    select_timeout_sec = 2
    verify_timeout_sec = 60*15
    min_file_dup_proxy_cnt = 100
    test_host_name = 'http://www.baidu.com/'
    ip_sepreate = ','
    def __init__(self, port = 3456, proc_thread_num = 100, dup_file_name = 'proxy.lst', \
                 dup_interval_sec = 120, proxy_type = 'all'):
        self.m_proxy_type = proxy_type
        assert(self.m_proxy_type == 'all' or self.m_proxy_type == 'internal' \
               or self.m_proxy_type == 'foreign')
        self.__init_socket(port, self.socket_timeout_sec)
        self.m_source_list = []
        self.m_sync_clients = {}
        self.m_wait_list = []
        self.m_read_lst = [self.m_server_socket.fileno()]
        self.m_write_lst = []
        self.m_lock = threading.Lock()
        '''存放验证后的结果全集'''
        self.m_verified_proxy_set = set()
        '''存放待验证的代理'''
        self.m_verify_proxy_queue = Queue.Queue()
        '''检验代理超时优先级队列'''
        self.m_timed_proxy_list = []
        self.m_request = urllib2.Request(self.test_host_name)
        self.m_request.get_method = lambda: 'HEAD'
        self.m_dup_file_name = dup_file_name
        self.m_dup_interval_sec = dup_interval_sec
        self.m_last_dup_time = 0
        self.m_verify_thread_list = []
        for i in range(proc_thread_num):
            self.m_verify_thread_list.append(threading.Thread(target=self.__verify_source_site))
        self.__load_ip_list()
    def __init_socket(self, port, timeout_sec):
        socket.setdefaulttimeout(timeout_sec)
        self.m_port = port
        self.m_server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.m_server_socket.setblocking(False)
        self.m_server_socket.bind(("0.0.0.0", self.m_port))
        self.m_server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.m_server_socket.listen(5)
    def __load_ip_list(self):
        try:
            ip_file = open(self.m_dup_file_name, 'r')
            cnt = 0
            for ip in ip_file.read().split(self.ip_sepreate):
                cnt += 1
                ip = my_strip(ip)
                if ip == '':
                    log_error('__load_ip_list: skip empty proxy from dup file.')
                    continue
                try:
                    global g_invalid_min_req_num, g_invalid_max_req_num, g_invalid_min_fail_rate
                    proxy = Proxy(ip, invalid_min_req_num = g_invalid_min_req_num, invalid_max_req_num = g_invalid_max_req_num, invalid_max_fail_rate = g_invalid_min_fail_rate)
                except Exception, err:
                    log_error('__load_ip_list: skip invalid ip %s' % ip)
                    continue
                self.m_verify_proxy_queue.put(proxy)
            ip_file.close()
            log_info('load %d ip from %s' % (cnt, self.m_dup_file_name))
        except Exception, err:
            log_error('load_ip_list %s error: %s' % (self.m_dup_file_name, err))
    def __close_sync_client(self, sync_client):
        fd = sync_client.fileno()
        log_info('remove fd: %d' % fd)
        self.m_lock.acquire()
        try:
            del(self.m_sync_clients[fd])
        except Exception, err:
            log_error('%s' % err)
        self.m_lock.release()
        try:
            self.m_write_lst.remove(fd)
            self.m_read_lst.remove(fd)
            sync_client.close()
            self.m_wait_list.remove(fd)
        except Exception, err:
            log_error('%s' % err)
            
    def poll(self):
        try:
            rlist, wlist, _ = select.select(self.m_read_lst, self.m_write_lst, [], self.select_timeout_sec)
            '''handle read'''
            for fd in rlist:
                if fd == self.m_server_socket.fileno():
                    client_socket, client_address = self.m_server_socket.accept()
                    client_socket.setblocking(False)
                    client_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                    client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    sync_client = SyncClient(client_socket, client_address) 
                    self.m_lock.acquire()
                    self.m_sync_clients[client_socket.fileno()] = sync_client
                    self.m_lock.release()
                    self.m_write_lst.append(client_socket.fileno())
                    self.m_read_lst.append(client_socket.fileno())
                    sync_client.add_proxy_list(list(self.m_verified_proxy_set))
                    log_info('recv connect from %s, send full data: %d proxies.' % (client_address, len(self.m_verified_proxy_set)))
                else:
                    sync_client = self.m_sync_clients[fd]
                    client_socket = sync_client.m_socket
                    try:
                        rcv_data = client_socket.recv(2048)
                        if len(rcv_data) == 0:
                            log_info('recv quit signal from %s, close client.' % sync_client.m_socket_addr)
                            self.__close_sync_client(sync_client)
                        else:
                            log_error('recv invalid content from %s: %s' % (sync_client.m_socket_addr, rcv_data))
                    except Exception, err:
                        #_, e, _ = sys.exc_info()
                        if hasattr(err, 'errno') and err.errno == errno.EINTR:
                            continue
                        log_error('recv from %s error: %s, close client.' % (sync_client.m_socket_addr, err))
                        self.__close_sync_client(sync_client)
            '''handle write'''
            for fd in wlist:
                if not self.m_sync_clients.get(fd):
                    continue
                sync_client = self.m_sync_clients[fd]
                '''看是否可发送：数据足够多或时间足够长'''
                if not sync_client.ready_to_send():
                    self.m_write_lst.remove(fd)
                    self.m_read_lst.remove(fd)
                    self.m_wait_list.append(fd)
                    continue
                '''发送失败, 关闭套接字'''
                if not sync_client.send():
                    log_error('%s extend max retry limit, close client' % sync_client.m_socket_addr)
                    self.__close_sync_client(sync_client)
                    continue
            '''check wait list'''
            cnt = 0
            while cnt < len(self.m_wait_list):
                fd = self.m_wait_list[cnt]
                sync_client = self.m_sync_clients[fd]
                if sync_client.ready_to_send():
                    del(self.m_wait_list[cnt])
                    self.m_write_lst.append(fd)
                    self.m_read_lst.append(fd)
                else:
                    cnt += 1
            '''process verify timeout'''
            cursor = 0
            self.m_lock.acquire()
            while cursor < len(self.m_timed_proxy_list):
                try:
                    proxy = self.m_timed_proxy_list[cursor]
                    if proxy not in self.m_verified_proxy_set:
                        del(self.m_timed_proxy_list[cursor])
                        continue
                    '''
                    if self.m_verify_proxy_queue.qsize() > self.overload_queue_size:
                        log_info('skip verify timeout for queue overload: %d' % self.m_verify_proxy_queue.qsize())
                        break
                    '''
                    if proxy.m_fetch_time + self.verify_timeout_sec > time.time():
                        break
                    del(self.m_timed_proxy_list[cursor])
                    #self.m_verified_proxy_set.remove(proxy)
                    self.m_verify_proxy_queue.put(proxy)
                    cursor += 1
                except Exception, err:
                    log_error('%s' % err)
            self.m_lock.release()
            log_info('verified proxies num: %d, verifying queue size: %d' % (len(self.m_verified_proxy_set), self.m_verify_proxy_queue.qsize()))
        except Exception, err:
            log_error('poll exception: %s, %s' % (err, traceback.format_exc()))
    def __verify_source_site(self):
        while True:
            proxy = self.m_verify_proxy_queue.get()
            if proxy.m_fetch_time == 0 and proxy in self.m_verified_proxy_set:
                continue
            if self.m_proxy_type == 'internal' and proxy.is_foreign():
                log_info('skip foreign proxy: %s' % proxy)
                continue
            handler_str = {}
            handler_str['http'] = proxy.m_ip
            handler_str['https'] = proxy.m_ip
            opener = urllib2.build_opener(urllib2.ProxyHandler(handler_str))
            failed = False
            try:
                opener.open(self.m_request).read()
                proxy.add_success()
            except Exception, err:
                proxy.add_fail()
                log_error('request proxy %s error: %s' % (str(proxy), err))
                if proxy.m_fetch_time > 0 and not proxy.is_invalid():
                    self.m_verify_proxy_queue.put(proxy)
                    continue
                log_error('throw invalid proxy %s:%s' % (proxy.m_ip, proxy.m_port))
                failed = True
            '''删减信息的同步'''
            if failed:
                try:
                    self.m_verified_proxy_set.remove(proxy)
                    for sync_client in self.m_sync_clients.values():
                        sync_client.delete_proxy(proxy)
                except:
                    pass
                continue
            proxy.update_fetch_time()
            proxy.locate_ip()
            self.m_lock.acquire()
            self.m_timed_proxy_list.append(proxy)
            if proxy not in self.m_verified_proxy_set:
                self.m_verified_proxy_set.add(proxy)
                '''增量信息的同步'''
                for sync_client in self.m_sync_clients.values():
                    sync_client.add_proxy(proxy)
            self.m_lock.release()
            '''process file dump''' 
            if self.m_last_dup_time + self.m_dup_interval_sec < time.time() and len(self.m_verified_proxy_set) >= self.min_file_dup_proxy_cnt:
                self.m_lock.acquire()
                try:
                    addr_lst = map(lambda x: str(x), self.m_verified_proxy_set)
                except Exception, err:
                    log_error('%s' % err)
                self.m_lock.release()
                try:
                    log_info('start dump %s' % self.m_dup_file_name)
                    dup_file = open(self.m_dup_file_name, 'w')
                    dup_file.write(self.ip_sepreate.join(addr_lst))
                    self.m_last_dup_time = time.time()
                    dup_file.close()
                    log_info('end dump: write %d proxies to %s' % (len(self.m_timed_proxy_list), self.m_dup_file_name))
                except Exception, err:
                    log_error('dup %s error: %s' % (self.m_dup_file_name, err))
    def add_source_site(self, url_pattern, ip_xpath, port_xpath, from_page, \
            end_page = -1, fetch_interval_sec = -1, is_foreign = -1, http_headers = {}):
        source = SiteSource(self.m_verify_proxy_queue, url_pattern, ip_xpath, port_xpath, from_page, end_page, fetch_interval_sec, is_foreign)
        self.m_source_list.append(source)
    def start(self):
        for source_site in self.m_source_list:
            source_site.start()
        for thd in self.m_verify_thread_list:
            thd.start()
            
def main():
    reload(sys)
    sys.setdefaultencoding('utf-8')
    processor = ProxyServer()
    
    processor.add_source_site('http://www.proxy.com.ru/list_{0}.html', \
        '/html/body/center/font/table[1]/tr/td[2]/font/table/*[position() > 1]/td[2]//text()', \
        '/html/body/center/font/table[1]/tr/td[2]/font/table/*[position() > 1]/td[3]//text()', \
        1, 10, fetch_interval_sec = 30)

    processor.add_source_site('http://free-proxy-list.net/', \
        'id("proxylisttable")/tbody/*/td[1]/text()', \
        'id("proxylisttable")/tbody/*/td[2]/text()', \
        1, 1, fetch_interval_sec = 60)

    processor.add_source_site('http://www.kuaidaili.com/free/inha/{0}/', \
        'id("list")/table/tbody/*/td[1]/text()', \
        'id("list")/table/tbody/*/td[2]/text()', \
        1, 50, fetch_interval_sec = 30)

    processor.add_source_site('http://www.kuaidaili.com/free/intr/{0}/', \
        'id("list")/table/tbody/*/td[1]/text()', \
        'id("list")/table/tbody/*/td[2]/text()', \
        1, 50, fetch_interval_sec = 30)
    
    processor.start()
    while True:
        processor.poll()

if __name__ == "__main__":
    main()
