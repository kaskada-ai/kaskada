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
sys.exit(0)
