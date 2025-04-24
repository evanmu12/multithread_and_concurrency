import queue
import threading
import random
import json
import time


class MultiThreadWebCrawler(object):
    def __init__(self, visited_log, queue_log, start_urls):
        self.visited = set()
        self.visited_log = visited_log
        self.queue_log = queue_log
        self.start_urls = start_urls
        self.max_depth = 3

        # Data Structure
        self.visited = set()
        self.visited_lock = threading.Lock()
        self.url_queue = queue.Queue()

        # Thread limit
        self.thread_num = 10
        self.threads = []
        self.crawler_interval = 0.1

        self.snapshot_thread = threading.Thread(target=self.__queue_snapshot, daemon=True)
        self.snapshot_interval = 0.1
        self.stop = False

    def start(self):
        for url in self.start_urls:
            self.url_queue.put((url, 0))
        
        for i in range(self.thread_num):
            thread = threading.Thread(target=self.crawler_worker, daemon=True)
            self.threads.append(thread)

        self.snapshot_thread.start()
        
        for thread in self.threads:
            thread.start()
        
        self.url_queue.join()

        self.stop = True

        for thread in self.threads:
            thread.join()
        
        print("Finish go throw all URL!!!")

        
    # Dummy Function get next urls from target URL
    def __get_urls(self, url):
        next_urls = []
        for i in range(5):
            next_urls.append(f"url_number_{random.randint(1, 20)}")

        return next_urls
    
    def __queue_snapshot(self):
        while not self.stop:
            time.sleep(self.snapshot_interval)
            with open(self.queue_log, 'w') as f:
                json.dump(list(self.url_queue.queue), f)
                f.flush()

    def __append_visited_file(self, url):
        with open(self.visited_log, 'a') as f:
            f.write(url + '\n')
            f.flush()

    def crawler_worker(self):
        while not self.stop:
            time.sleep(self.crawler_interval)
            try:
                task = self.url_queue.get(timeout=0.1)
            except queue.Empty:
                break
                
            url, depth = task
            with self.visited_lock:
                if url in self.visited or depth >= self.max_depth:
                    self.url_queue.task_done()
                    continue

                self.visited.add(url)
                self.__append_visited_file(url)
            
            next_urls = self.__get_urls(url)

            for next_url in next_urls:
                self.url_queue.put((next_url, depth + 1))
            self.url_queue.task_done()

def RunTest():
    crawler = MultiThreadWebCrawler('multicrawler/visited_log.log', 'multicrawler/queue_snap.log', {"url_number_0", "url_number_1"})

    crawler.start()

RunTest()

                    


