#!/usr/bin/env python3

import sys, argparse

class Collector:
    def __init__(self, grouping):
        if grouping:
            self._set = set()
        self._grouping = grouping
    def collect(self, word):
        if self._grouping:
            if not word in self._set:
                self._set.add(word)
                print(word)
        else:
            print(word)

def main():
    aparse = argparse.ArgumentParser()
    aparse.add_argument('filename', type=str)
    aparse.add_argument('col', type=int)
    aparse.add_argument('-g', nargs='?', const=True, default=False)
    args = aparse.parse_args()
    grouping = args.g
    collector = Collector(grouping)
    col = args.col

    firstLine = True
    with open(args.filename, 'r') as f:
        for l in f:
            if firstLine:
                firstLine = False
                continue
            collector.collect(l.split('\t')[col])

if __name__ == "__main__":
    main()
