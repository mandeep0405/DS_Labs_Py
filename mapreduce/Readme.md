## This python implementation is based on MIT Lab 1
https://pdos.csail.mit.edu/6.824/labs/lab-mr.html

## How to run it?
```
# Start coordinator
python mrcoordinator.py pg*

# Start workers (in separate terminals)
python mrworker.py wc.py
python mrworker.py wc.py

# View results
cat mr-out-* | sort

```
