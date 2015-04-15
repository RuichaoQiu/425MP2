#!/bin/bash
kill `lsof -t -i:3000`
#python bla.py
#python chord.py
python run_chord.py -g test_output
