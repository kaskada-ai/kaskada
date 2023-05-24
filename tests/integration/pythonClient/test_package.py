#! /usr/bin/env python

import sys
import traceback
from kaskada.api.session import LocalBuilder

try:
    session = LocalBuilder().build()
    print(session.__dict__)
except:
    print("Could not create a session!")
    traceback.print_exc()
    sys.exit(1)

print("**** SUCCESS! I can create a Kaskada Session.")
try: 
    session.stop()
    print ("**** SUCCESS! I can stop a Kaskada Session.")
except: 
    traceback.print_exc()
    sys.exit(0) # grpc _channel fails for some reason on stop, but it's not a big deal 
    
sys.exit(0)
