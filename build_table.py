#!/usr/bin/env python3

# Process XML table in parallel.
# The main process creates a tmp dir and all processes processes its only XML
# file. Each of them gaves a name to the result file and put the header into
# the <result name>.header and body into <result name>.body. After all worker
# processse are done. Main process uses the header they dumped to create a
# merged header, and use it to merge the table.


import xml.sax, os, sys, re, tempfile, multiprocessing

MERGEING_KEY = "編號"

# Helpers
def load_header(basepath, name):
    with open(os.path.join(basepath, name + ".header")) as f:
        txt = f.readline()
        return list(map(lambda s: s.strip(), txt.split('\t')))

def load_body(basepath, name):
    with open(os.path.join(basepath, name + ".body")) as f:
        for l in f:
            if len(l.strip()) == 0:
                continue
            yield list(map(lambda s: s.strip(), l.split('\t')))
    
class ColumnCollector(xml.sax.handler.ContentHandler):
    def __init__(self, element_type):
        self._cols = [element_type] # built-in
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
    def __init__(self, element_type, col_map, file_obj):
        self._file = file_obj
        self._col_map = col_map
        self._element_type = element_type
        self._current_data = None
        self._element_target = -1
        self._level = 0
    def startElement(self, name, attrs):
        self._level += 1
        if self._level == 2:
            self._current_data = [""] * len(self._col_map)
            # element name of level 2 is the value of type.
            self._current_data[self._col_map[self._element_type]] = name
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

class BestTryHandler(xml.sax.handler.ErrorHandler):
    def error(self, e):
        print("Error occurs: " + str(e))
    def fatalError(self, e):
        print("Fatal occurs: " + str(e))
    def warning(self, e):
        print("Warning: " + str(e))

class MergedHeader:
    def __init__(self):
        self._columns = []
        self._auxiliary_column_set = set()
        self._header_for_name = dict() # key -> col_index -> field index

    def append_one_header(self, key, header_columns):
        cols = header_columns
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

def parse_one_table(filename, tmp):
    MAIN_FILENAME_PATTERN = re.compile(r'A_lvr_land_A\.xml', re.I)
    SUPPORT_FILENAME_PATTERN = re.compile(r'A_lvr_land_A_\w+\.xml', re.I)
    file_group = "A"
    
    name = os.path.basename(filename)
    filetype = None
    element_type = None
    if MAIN_FILENAME_PATTERN.match(name) != None:
        filetype = "main"
        element_type = "tradetype"
    elif SUPPORT_FILENAME_PATTERN.match(name) != None:
        filetype = "support"
        element_type = "supportingtype"
    if filetype == None:
        return None    
    name = name.replace(".", "_")
    print(name)

    try:
        col_collector = ColumnCollector(element_type)
        xml.sax.parse(filename, col_collector, BestTryHandler())
        cols = col_collector.list_col()
        with open(os.path.join(tmp, name + '.header'), 'w') as f:
            f.write('\t'.join(cols) + '\n')
        with open(os.path.join(tmp, name + '.body'), 'w') as f:
            col_map = dict()
            for i in range(len(cols)):
                col_map[cols[i]] = i
            xml.sax.parse(filename, DataWriter(element_type, col_map, f), BestTryHandler())
        return (file_group, filetype, name)
    except Exception as e:
        print(e)
        return None

def merge_table_by_key(tmp, main_table, support_tables):
    lookup_table = dict() # key -> [(name string, col array)]
    merged_header = MergedHeader()
    target_table = main_table + "_".join(support_tables)
    # Load support_tables
    for n in support_tables:
        cols = load_header(tmp, n)
        key_pos = None
        for i in range(len(cols)):
            print("'{}'".format(cols[i]))
            if cols[i] == MERGEING_KEY:
                key_pos = i
        if key_pos == None:
            # key is not found:
            raise Exception("key: {} is not found in {}".format(MERGEING_KEY, n))
        merged_header.append_one_header(n, cols)

        for parts in load_body(tmp, n):
            id_val = parts[key_pos]
            if not id_val in lookup_table:
                lookup_table[id_val] = [(n, parts)]
            else:
                lookup_table[id_val].append((n, parts))
    # load main table.
    cols = load_header(tmp, main_table)
    merged_header.append_one_header(main_table, cols)
    key_pos = None
    for i in range(len(cols)):
        print("'{}'".format(cols[i]))
        if cols[i] == MERGEING_KEY:
            key_pos = i
    if key_pos == None:
        raise Exception("key: {} is not found in {}".format(MERGEING_KEY,
                                                            main_table))
    with open(os.path.join(tmp, target_table + ".header"), 'w') as f:
        f.write('\t'.join(merged_header.dump_cols()) + '\n')
    with open(os.path.join(tmp, target_table + ".body"), 'w') as f:
        for col_line in load_body(tmp, main_table):
            out_cols = []
            # remap col of main table
            for c in merged_header.get_header_map_for_key(main_table):
                if c < 0:
                    out_cols.append("")
                else:
                    out_cols.append(col_line[c].strip())
            key = col_line[key_pos]
            if key in lookup_table:
                for n, sup_cols in lookup_table[key]:
                    idx = 0
                    for c in merged_header.get_header_map_for_key(n):
                        if c >= 0:
                            out_cols[idx] = sup_cols[c].strip()
                        idx += 1
            f.write("\t".join(out_cols) + "\n")
    return target_table

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
            grouped_files = dict() # group -> [main, support array]
            for r in results:
                res = r.get(60)
                if res == None:
                    continue
                group = res[0]
                if not group in grouped_files:
                    grouped_files[group] = [None, []]
                if res[1] == 'main':
                    grouped_files[group][0] = res[2]
                else:
                    grouped_files[group][1].append(res[2])

            # merge groups
            results = [p.apply_async(merge_table_by_key, (tmp, v[0], v[1]))
                       for k, v in grouped_files.items()]
            names = [res.get(60) for res in results]
        
        merged_header = MergedHeader()
        for n in names:
            if n != None:
                merged_header.append_one_header(n, load_header(tmp, n))

        with open(output_file, "w") as out:
            out.write('\t'.join(merged_header.dump_cols()) + "\n")
            for n in names:
                if n != None:
                    for cols in load_body(tmp, n):
                        out_cols = []
                        for c in merged_header.get_header_map_for_key(n):
                            if c < 0:
                                out_cols.append("")
                            else:
                                out_cols.append(cols[c].strip())
                        out.write('\t'.join(out_cols) + "\n")

if __name__ == "__main__":
    main()
