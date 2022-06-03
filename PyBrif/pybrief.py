import json as js
from typing import Dict
import numpy as np
import datetime as dt
import threading as thr

class Thread(thr.Thread):
    def __init__(self, intervalSec: int=1) -> None:
        self.__intervalSec = intervalSec
        super().__init__()
        self.__kill = thr.Event()
    
    def execute(self):
        # will override
        pass

    def run(self):
        while (True):
            self.execute()

            if (self.__kill.wait(self.__intervalSec)):
                break
    
    def kill(self):
        self.__kill.set()
    
    def terminate(self):
        self.kill()
    
    def stop(self):
        self.kill()

def getDumps(json: Dict, indent: int=4) -> str:
    def convert(obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, dt.datetime):
            return obj.__str__()
    
    return js.dumps(json, indent=indent, default=convert)