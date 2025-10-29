#!/usr/bin/env python3
"""
Main entry point for ARNy Plotter application
"""

import sys
import os

# Add the src directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

if __name__ == "__main__":
    from src.core.app import app
    
    # Run the Flask application
    app.run(host='0.0.0.0', port=4242, debug=True)