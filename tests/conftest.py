import sys
import os

# Expose project root so unit tests can import src.*
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
