from apscheduler.schedulers.background import BackgroundScheduler
import json
from pathlib import Path
from time import time
import atexit

class TimeManagerClass:
    def __init__(self):
        self.storagePath = Path("data/active.json")
        self.active = {}
        self.load_active_session()
        
        self.scheduler = BackgroundScheduler()
        self.scheduler.add_job(
            func=self.save_active_session,
            trigger='interval',
            seconds=300,
            id='auto_save'
        )
        self.scheduler.start()
        
        atexit.register(self.save_active_session)
    
    def load_active_session(self):
        if self.storagePath.exists():
            with open(self.storagePath, 'r') as f:
                self.active = json.load(f)
        else:
            self.active = {}
    
    def save_active_session(self):
        with open(self.storagePath, 'w') as f:
            json.dump(self.active, f)
    
    def add_sessions(self, sessions: dict):
        # Adds multiple sessions to the active dict (meant to bundle multiple to reduce blocking calls)
        self.active.update(sessions)
    
    def remove_sessions(self, sessions: dict):
        # Removes multiple sessions from the active dict (meant to bundle multiple to reduce blocking calls)
        for key in sessions.keys():
            if key in self.active:
                del self.active[key]
    
    def shutdown(self):
        self.save_active_session()
        self.scheduler.shutdown()

TimeManager = TimeManagerClass()