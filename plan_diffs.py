import filecmp
import sys
from os import listdir
from os.path import isfile, join

old, new = sys.argv[1], sys.argv[2]
old_plans = set([f for f in listdir(old) if isfile(join(old, f)) and f.endswith('.sql')])
new_plans = set([f for f in listdir(old) if isfile(join(old, f)) and f.endswith('.sql')])

if old_plans != new_plans:
    print("Directories don't match")
for f in old_plans:
    if filecmp.cmp(join(old, f), join(new, f)) != True:
        print(f"{f} plan changed!")