#!/usr/bin/env python3
import argparse, json
import chinese_table_transform_functions

def line_to_cols(line):
    return list(map(lambda x: x.strip(), line.split('\t')))

def col_name_to_pos(col, names):
    d = dict()
    for c in col:
        d[c] = len(d)
    return list(map(d.get, names))

class ColumnBuilder:
    Operator = {
        "ChineseFloorTransformer": chinese_table_transform_functions.builder_ChineseFloorTransformer,
    }
    def __init__(self, target_column, ops, operator):
        self._target = target_column
        self._ops = ops
        self._operator = ColumnBuilder.Operator[operator]
    def transform(self, row):
        ops = [ row[i] for i in self._ops ]
        row.append(self._operator(ops))
    def target_name(self):
        return self._target

def filter_Includes(val, ops):
    for v in ops:
        if val.find(v) < 0:
            return False
    return True

def filter_NotIncludes(val, ops):
    for v in ops:
        if val.find(v) >= 0:
            return False
    return True
        
class ColumnFilter:
    Operator = {
        "Includes": filter_Includes,
        "NotIncludes": filter_NotIncludes
    }
    def __init__(self, target_column, ops, operator):
        self._target = target_column
        self._ops = ops
        self._operator = ColumnFilter.Operator[operator]        
    def do_filter(self, row):
        return self._operator(row[self._target],
                              self._ops)

def main():
    # 1. build new column.
    # 2. apply filter.
    # 3. remap columns.
    aparse = argparse.ArgumentParser()
    aparse.add_argument("input_file", help="source table")
    aparse.add_argument("transformer", help="transformer file")
    aparse.add_argument("target_file", help="target table")
    args = aparse.parse_args()

    transformer = None
    with open(args.transformer) as f:
        transformer = json.load(f)

    with open(args.input_file, "r") as fi:
        cols = line_to_cols(fi.readline())
        builders = [ ColumnBuilder(b["column_name"],
                                   col_name_to_pos(cols, b["ops"]),
                                   b["operator"])
                     for b in transformer["builds"] ]

        # append columns that are built by builders, so we can get
        # a overall map
        for builder in builders:
            cols.append(builder.target_name())

        filters = [ ColumnFilter(cols.index(f["column_name"]),
                                 f["ops"],
                                 f["operator"])
                    for f in transformer["filters"] ]

        columns = col_name_to_pos(cols, transformer["columns"])
        with open(args.target_file, "w") as fo:
            out_cols = [ cols[i] for i in columns ]
            fo.write("\t".join(out_cols) + "\n")
            for l in fi:
                row = line_to_cols(l)
                for b in builders: b.transform(row)

                drop = False
                for f in filters:
                    if f.do_filter(row) == False:
                        drop = True
                        break
                if drop:
                    continue

                out_row = [ row[i] for i in columns ]
                fo.write("\t".join(out_row) + "\n")
            
if __name__ == "__main__":
    main()
