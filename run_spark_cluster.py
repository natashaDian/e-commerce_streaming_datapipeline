#!/usr/bin/env python3
import sys
import os

sys.path.insert(0, '/opt/spark/apps')
os.chdir('/opt/spark/apps')

from src.consumers.stream_processor import main

if __name__ == "__main__":
    main()