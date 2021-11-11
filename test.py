import time
from collections import defaultdict

d = {2: 3, 3: 4, 1: 2, 4: 5}
d = sorted(d.items())

for k, v in d:
    print(k, v)
