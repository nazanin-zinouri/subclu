import os
import csv
import itertools

def load_csv(py_file, name):
    filename = os.path.join(os.path.dirname(os.path.realpath(py_file)), name + ".csv")

    with open(filename, newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader, None) # skip header
            for row in reader:
                if len(row) < 1:
                    continue # blank row
                yield(tuple(row))

def aabb_analogies_from_tuples(tuples):
    for tpl1, tpl2 in itertools.combinations(tuples, 2):
        yield(tpl1[0], tpl1[1], tpl2[0], tpl2[1])
