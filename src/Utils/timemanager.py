import apscheduler
import json
from pathlib import Path
from time import time

class TimeManager:
    def __init__(self):
        self.storagePath = Path("data/active.json")
        self.active = {}
        self.load_active_session()
    
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
        self.save_active_session()
    
    def remove_sessions(self, sessions: dict):
        # Removes multiple sessions from the active dict (meant to bundle multiple to reduce blocking calls)
        for key in sessions.keys():
            if key in self.active:
                del self.active[key]
        self.save_active_session()