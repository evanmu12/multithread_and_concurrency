'''
1. multi-thread commit
2. group commit with flush_interval
3. 
'''


import threading
import json
import queue
import os
import atexit
import time

class GroupCommit(object):
    def __init__(self, wal_log, force_flush):
        self.wal_log = wal_log
        self.force_flush = force_flush
        self.lock = threading.Lock()

        self.txn_queue = queue.Queue()
        self.flush_interval = 0.01
        self.batch_size = 8
        self.stop_flag = False

        self.flush_thread = threading.Thread(target=self.__flush_loop, daemon=True)

        # Start
        self.flush_thread.start()

        #Register atexit
        atexit.register(self.close)
    
    def __flush_to_disk(self, batch):
        with self.lock:
            with open(self.wal_log, 'a') as f:
                for txn, _ in batch:
                    f.write(json.dumps(txn) + '\n')
                f.flush
                if self.force_flush:
                    os.fsync(f.fileno())
            print(f"Flush {len(batch)} TXN to WAL.")
            for _, ack_event in batch:
                ack_event.set()
            


    def __flush_loop(self):
        while not self.stop_flag:
            batch = []
            start = time.time()
            while len(batch) < self.batch_size and time.time() - start < self.flush_interval:
                try:
                    txn, ack_event = self.txn_queue.get(timeout=self.flush_interval)
                    batch.append((txn, ack_event))

                except queue.Empty:
                    break
            
            if batch:
                self.__flush_to_disk(batch)



    def commit(self, txn):
        ack_event = threading.Event()
        self.txn_queue.put((txn, ack_event))
        ack_event.wait()
        return True
    
    def close(self):
        self.stop_flag = True
        self.flush_thread.join()


def RunTest():
    def simulate_txn(committer, txn_id):
        for i in range(20):
            txn = {"id": txn_id, "data": f"value_{txn_id}"}
            print(f"Txn {txn_id} -> commit request")
            committer.commit(txn)
            print(f"Txn {txn_id} -> commit success")
            time.sleep(0.01)

    committer = GroupCommit("group_commit/wal.log", True)
    threads = [threading.Thread(target=simulate_txn, args=(committer, i)) for i in range(10)]

    for t in threads:
        t.start()

    for t in threads:
        t.join()

    committer.close()

RunTest()