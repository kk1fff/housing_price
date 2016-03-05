# Process XML table in parallel.
# The main process creates a tmp dir and all processes processes its only XML
# file. Each of them gaves a name to the result file and put the header into
# the <result name>.header and body into <result name>.body. After all worker
# processse are done. Main process uses the header they dumped to create a
# merged header, and use it to merge the table.

#!/usr/bin/env python3

import xml.sax, os, sys, re, tempfile, multiprocessing

FILENAME_PATTERN = re.compile(r'\w_lvr_land_\w\.xml', re.I)
class ColumnCollector(xml.sax.handler.ContentHandler):
    def __init__(self):
        self._cols = ['type'] # built-in
        self._aux_col_set = set()
        self._level = 0
    def startElement(self, name, attrs):
        self._level += 1
        if self._level == 3:
            if not name in self._aux_col_set:
                self._cols.append(name)
                self._aux_col_set.add(name)
    def endElement(self, name):
        self._level -= 1
    def list_col(self):
        return self._cols
class DataWriter(xml.sax.handler.ContentHandler):
    def __init__(self, col_map, file_obj):
        self._file = file_obj
        self._col_map = col_map
        self._current_data = None
        self._element_target = -1
        self._level = 0
    def startElement(self, name, attrs):
        self._level += 1
        if self._level == 2:
            self._current_data = [""] * len(self._col_map)
            # element name of level 2 is the value of type.
            self._current_data[self._col_map['type']] = name
        elif self._level == 3:
            self._element_target = self._col_map[name]
    def characters(self, content):
        if self._level == 3:
            self._current_data[self._element_target] += content
    def endElement(self, name):
        self._level -= 1
        if self._level == 2:
            self._element_target = -1
        elif self._level == 1:
            # write
            self._file.write('\t'.join(map(lambda x: x.strip(), self._current_data)) + "\n")
            self._current_data = None

def parse_one_table(filename, tmp):
    try:
        name = os.path.basename(filename)
        if FILENAME_PATTERN.match(name) == None:
            return None
        name = name.replace(".", "_")
        
        col_collector = ColumnCollector()
        xml.sax.parse(filename, col_collector)
        cols = col_collector.list_col()
        with open(os.path.join(tmp, name + '.header'), 'w') as f:
            f.write('\t'.join(cols) + '\n')
        with open(os.path.join(tmp, name + '.body'), 'w') as f:
            col_map = dict()
            for i in range(len(cols)):
                col_map[cols[i]] = i
            xml.sax.parse(filename, DataWriter(col_map, f))
        return name
    except Exception as e:
        print(e)
        return None

class MergedHeader:
    def __init__(self):
        self._columns = []
        self._auxiliary_column_set = set()
        self._header_for_name = dict() # key -> col_index -> field index

    def append_one_header(self, key, headerfile):
        cols = None
        with open(headerfile) as f:
            txt = f.read()
            txt = ''.join(map(lambda l: l.strip(), txt.split('\n')))
            cols = txt.split('\t')

        col_name_pos = dict()
        for col in cols:
            if not col in self._auxiliary_column_set:
                self._columns.append(col)
                self._auxiliary_column_set.add(col)
            col_name_pos[col] = len(col_name_pos)

        col_table = []
        for col in self._columns:
            if col in col_name_pos:
                col_table.append(col_name_pos[col])
            else:
                col_table.append(-1)
        self._header_for_name[key] = col_table

    def dump_cols(self):
        return self._columns

    def get_header_map_for_key(self, key):
        header_map = self._header_for_name[key]
        for i in range(len(self._columns)):
            if i < len(header_map):
                yield header_map[i]
            else:
                yield -1

def main():
    source_dir = sys.argv[1]
    output_file = sys.argv[2]

    # for each file
    files = []
    for filename in os.listdir(source_dir):
        full_fn = os.path.join(source_dir, filename)
        if os.path.isfile(full_fn):
            files.append(full_fn)

    # parse all data in multiprocess
    names = None
    with tempfile.TemporaryDirectory() as tmp:
        print(tmp)
        with multiprocessing.Pool(processes=6) as p:
            results = [p.apply_async(parse_one_table, (fn, tmp)) for fn in files]
            names = [res.get(60) for res in results]
        
        merged_header = MergedHeader()
        for n in names:
            if n != None:
                merged_header.append_one_header(n, os.path.join(tmp, n + ".header"))

        with open(output_file, "w") as out:
            out.write('\t'.join(merged_header.dump_cols()) + "\n")
            for n in names:
                if n != None:
                    with open(os.path.join(tmp, n + ".body")) as data_in:
                        for l in data_in:
                            if l.strip() == "":
                                continue
                            cols = l.split('\t')
                            out_cols = []
                            for c in merged_header.get_header_map_for_key(n):
                                if c < 0:
                                    out_cols.append("")
                                else:
                                    out_cols.append(cols[c].strip())
                            out.write('\t'.join(out_cols) + "\n")

if __name__ == "__main__":
    main()
