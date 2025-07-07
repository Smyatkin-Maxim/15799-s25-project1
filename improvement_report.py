#!/usr/bin/python3

from collections import defaultdict
from termcolor import colored


def getQueries(score_filename):
    with open(score_filename) as f:
        queries = defaultdict(float)
        for line in f:
            if line != '':
                q, t = line.split(' ')
                queries[q] = float(t)
        return queries

old = getQueries('old_scores.txt')
new = getQueries('scores.txt')

if len(old) == 0:
    exit(0)

for q, t_new in new.items():
    t_old = old[q]
    color = 'yellow'
    if abs(t_new/t_old - 1) > 0.1:
        if t_new < t_old:
            color = 'green'
        else:
            color = 'red'
    print(colored(f'{q} {t_old/t_new}', color))