
import threading
import json
from readerwriterlock import rwlock
import atexit
import time

class DurationKVStore(object):
    def __init__(self, log_file, snapshot_file):
        self.kv_store = {}
        self.log_file = log_file
        self.snapshot_file = snapshot_file

        rw_lock = rwlock.RWLockFair()
        self.rlock = rw_lock.gen_rlock()
        self.wlock = rw_lock.gen_wlock()

        # Batch flush
        self.batch_size = 5
        self.batch_buffer = []

        # Async batch buffer log
        self.flush_interval = 0.1
        self.snapshot_interval = 0.3
        self.shutdown_flag = False
        self.flush_thread = threading.Thread(target=self.__flush_loop, daemon=True)
        self.snapshot_thread = threading.Thread(target=self.__snapshot_loop, daemon=True)

        # Recover
        self.__recover()

        # Start
        self.flush_thread.start()
        self.snapshot_thread.start()

        # Register close
        atexit.register(self.close)

    def __recover(self):
        with open(self.snapshot_file, 'r') as f:
            self.kv_store = json.load(f)
        
        with open(self.log_file, 'r') as f:
            for line in f:
                entry = json.load(line.strip())
                self.__apply_entry(entry)
    
    def __apply_entry(self, entry):
        op = entry['op']
        if op == "set":
            self.store[entry["key"]] = entry["value"]
        elif op == "delete":
            self.store.pop(entry["key"], None)
        elif op == "clear":
            self.store.clear()
    
    def __flush_snapshot(self):
        with open(self.snapshot_file, 'w') as s:
            json.dump(self.kv_store, s)
            s.flush()
        open(self.log_file, 'w').close()
        self.batch_buffer.clear()

    def __flush_buffer(self, prefix):
        if len(self.batch_buffer) == 0:
            return
        with open(self.log_file, 'a') as f:
            f.write(prefix)
            f.writelines(self.batch_buffer)
            f.flush()
        self.batch_buffer.clear()
    
    def __snapshot_loop(self):
        while not self.shutdown_flag:
            time.sleep(self.snapshot_interval)
            with self.wlock:
                self.__flush_snapshot()
    
    def __flush_loop(self):
        while not self.shutdown_flag:
            time.sleep(self.flush_interval)
            with self.wlock:
                self.__flush_buffer("Interval Flush\n")

    def __append_log(self, entry):
        self.batch_buffer.append(json.dumps(entry) + '\n')

        if len(self.batch_buffer) >= self.batch_size:
            self.__flush_buffer("Buffer Flush\n")
    
    def set(self, key, value):
        with self.wlock:
            self.kv_store[key] = value
            self.__append_log({'op': 'set', 'key': key, 'value': value})

    def get(self, key):
        with self.rlock:
            return self.kv_store.get(key)

            
    def delete(self, key):
        with self.wlock:
            if key in self.kv_store:
                del self.kv_store[key]
                self.__append_log({'op': 'del', 'key': key})
    
    def clear(self):
        with self.wlock:
            self.kv_store.clear()
            self.__append_log({'op': 'clear'})

    def keys(self):
        with self.rlock:
            return list(self.kv_store.keys())
    
    def close(self):
        self.shutdown_flag = True
        self.snapshot_thread.join()
        self.flush_thread.join()
        self.__flush_buffer("Final Flush\n")


        
def RunTest():
    kv = DurationKVStore('kv_store/store_log_file.log', 'kv_store/snapshot.log')

    def worker(i):
        for j in range(20):
            time.sleep(0.1)
            kv.set(f"user_{i}_{j}", f"value_{i}_{j}")
            print(f"Thread-{i}: {kv.get(f'user_{i}_{j}')}")


    threads = []
    for i in range(10):
        t = threading.Thread(target=worker, args=(i,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()
    
    kv.close()

    print("All keys:", kv.keys())

RunTest()