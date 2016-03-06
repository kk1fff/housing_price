#!/usr/bin/env python3

import sys, argparse

class Collector:
    def __init__(self, grouping, collector_chain = [], filter_chain = []):
        if grouping:
            self._set = set()
        self._grouping = grouping
        self._collectors = collector_chain
        self._filters = filter_chain
    def _send_collect(self, word):
        for cltr in self._collectors:
            cltr(word)
    def _try_filter(self, word):
        for fltr in self._filters:
            if fltr(word) == False:
                return False
        return True
    def collect(self, word):
        if not self._try_filter(word):
            return
        if self._grouping:
            if not word in self._set:
                self._set.add(word)
                self._send_collect(word)
        else:
            self._send_collect(word)

def main():
    aparse = argparse.ArgumentParser()
    aparse.add_argument('filename', type=str)
    aparse.add_argument('col', type=int)
    aparse.add_argument('-g', nargs='?', const=True, default=False)
    args = aparse.parse_args()
    grouping = args.g
    collector = Collector(grouping,
                          collector_chain = [
                              lambda w: print(w)
                          ])
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
