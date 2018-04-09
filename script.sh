#!/bin/bash

start=$(date +%s.%N)

python triangle-to-full.py
python lab3-multiplicationPart1.py newinputQ6.txt
python lab3-multiplicationPart2.py newinputQ6.txt partialOutputQ6.txt

end=$(date +%s.%N)    
runtime=$(python -c "print(${end} - ${start})")

#Different format
#start=$(date +%s)
#end=$(date +%s)
#runtime=$(python -c "print '%u:%02u' % ((${end} - ${start})/60, (${end} - ${start})%60)") 

echo "Runtime was $runtime"