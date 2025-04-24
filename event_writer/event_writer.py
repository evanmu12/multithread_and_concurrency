import threading
import json
import queue
import os
import time

import atexit

class EventWriter(object):
    def __init__(self, file_path, need_fsync = False):
        self.file_path = file_path
        self.lock = threading.Lock()

        #Create thread-safe event queue
        self.event_queue = queue.Queue()
        self.stop_signal = False

        # force fsync
        self.need_fsync = need_fsync

        #Batch Size and Flush Interval for batch flush
        self.batch_size = 10
        self.flush_interval = 0.2

        # Create async thread to flush event to disk
        self.write_thread = threading.Thread(target = self._write_loop, daemon = True)
        self.write_thread.start()

        # Register at exit
        atexit.register(self.close)

    def _write_loop(self):
        batch = []
        while not self.stop_signal or not self.event_queue.empty():
            batch = []
            start = time.time()
            while len(batch) < self.batch_size and time.time() - start < self.flush_interval:
                try:
                    event = self.event_queue.get(timeout = 0.5)
                    try:
                        batch.append(json.dumps(event) + '\n')
                    finally:
                        self.event_queue.task_done()
            
                except queue.Empty:
                    continue
            
            if batch:
                try:
                    self._write_to_file(batch)
                
                except Exception as e:
                    print(f"[Error] Unexpected Error: {e}")


    def _write_to_file(self, batch):
        try:
            with open(self.file_path, 'a') as f:
                f.write("Write Batch!\n")
                f.writelines(batch)
                f.flush()
                if self.need_fsync:
                    os.fsync(f.fileno())
        except Exception as e:
            print(f"[Error] Failed to write log to disk: {e}")



    def writeEvent(self, event):
        self.event_queue.put(event)

    def close(self):
        if self.stop_signal:
            return
        
        self.stop_signal = True
        self.event_queue.join()
        self.write_thread.join()


def RunTest():
    event_writer = EventWriter('event_writer/event_record.log', True)

    def simulate_log(id):
        for i in range(5):
            event_writer.writeEvent(f"Thread-{id}: log message {i}")
            time.sleep(0.1)

    threads = [threading.Thread(target=simulate_log, args=(i,)) for i in range(3)]
    [t.start() for t in threads]
    [t.join() for t in threads]

    event_writer.close()

RunTest()

