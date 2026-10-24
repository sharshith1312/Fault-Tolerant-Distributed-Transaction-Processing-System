#!/usr/bin/env python3
"""
Paxos CLI Launcher
"""
import sys
import os
# Add src to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(current_dir, 'src')
sys.path.insert(0, src_dir)

# Import and run the CLI
if __name__ == "__main__":
    from client.main import main
    main()
