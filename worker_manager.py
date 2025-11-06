import subprocess
import time
import os
import signal
import sys

class CeleryWorkerManager:
    def __init__(self, num_workers=3):
        self.num_workers = num_workers
        self.processes = []
        
    def start_workers(self):
        """Start multiple Celery workers"""
        print(f"🚀 Starting {self.num_workers} Celery workers...")
        
        for i in range(1, self.num_workers + 1):
            cmd = [
                'celery', '-A', 'crawling_tasks', 'worker',
                '--loglevel=info', '--pool=solo',
                f'--hostname=worker{i}@%h'
            ]
            
            print(f"   Starting worker-{i}...")
            process = subprocess.Popen(cmd, cwd=os.getcwd())
            self.processes.append(process)
            time.sleep(2)
            
        print(f"✅ All {self.num_workers} workers started!")
        
    def stop_workers(self):
        """Stop all workers"""
        print("🛑 Stopping all workers...")
        for i, process in enumerate(self.processes, 1):
            process.terminate()
            
        for process in self.processes:
            process.wait()
        print("✅ All workers stopped!")
        
    def status(self):
        """Check worker status"""
        alive = sum(1 for p in self.processes if p.poll() is None)
        # print(f"📊 Status: {alive}/{self.num_workers} workers running")
        return alive
        
    def monitor(self):
        """Monitor workers with Ctrl+C support"""
        print("🔍 Monitoring workers... (Press Ctrl+C to stop)")
        try:
            while True:
                self.status()
                time.sleep(10)
        except KeyboardInterrupt:
            print("\n🛑 Stopping workers...")
            self.stop_workers()

if __name__ == "__main__":
    manager = CeleryWorkerManager(num_workers=2)
    
    try:
        manager.start_workers()
        manager.monitor()
    except Exception as e:
        print(f"❌ Error: {e}")
        manager.stop_workers()