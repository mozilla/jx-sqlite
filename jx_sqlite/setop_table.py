# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#


from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from mo_dots import listwrap, Data, unwraplist, split_field, join_field, startswith_field, unwrap, relative_field, concat_field
from mo_math import UNION, MAX

from jx_sqlite import quote_table, quoted_UID, get_column, _make_column_name, ORDER, COLUMN, set_column, quoted_PARENT, ColumnMapping
from jx_sqlite.insert_table import InsertTable
from pyLibrary.queries.containers import STRUCT
from pyLibrary.queries.expressions import sql_type_to_json_type, LeavesOp
from pyLibrary.queries.meta import Column
from pyLibrary.sql.sqlite import quote_value


class SetOpTable(InsertTable):
    def _set_op(self, query, frum):
        # GET LIST OF COLUMNS
        frum_path = split_field(frum)
        primary_nested_path = join_field(frum_path[1:])
        vars_ = UNION([s.value.vars() for s in listwrap(query.select)])
        schema = self.sf.tables[primary_nested_path].schema

        nest_to_alias = {
            nested_path: "__" + unichr(ord('a') + i) + "__"
            for i, (nested_path, sub_table) in enumerate(self.sf.tables.items())
        }

        active_columns = {".": []}
        for cname, cols in schema.items():
            if any(startswith_field(cname, v) for v in vars_):
                for c in cols:
                    if c.type in STRUCT:
                        continue
                    nest = c.nested_path[0]
                    active = active_columns.get(nest)
                    if not active:
                        active = active_columns[nest] = []
                    active.append(c)
        # ANY VARS MENTIONED WITH NO COLUMNS?
        for v in vars_:
            if not any(startswith_field(cname, v) for cname in schema.keys()):
                active_columns["."].append(Column(
                    names={".": v},
                    type="null",
                    es_column=".",
                    es_index=".",
                    nested_path=["."]
                ))

        # EVERY COLUMN, AND THE INDEX IT TAKES UP
        index_to_column = {}  # MAP FROM INDEX TO COLUMN (OR SELECT CLAUSE)
        index_to_uid = {}  # FROM NESTED PATH TO THE INDEX OF UID
        sql_selects = []  # EVERY SELECT CLAUSE (NOT TO BE USED ON ALL TABLES, OF COURSE)
        nest_to_alias = {
            nested_path: "__" + unichr(ord('a') + i) + "__"
            for i, (nested_path, sub_table) in enumerate(self.sf.tables.items())
            }

        sorts = []
        if query.sort:
            for s in query.sort:
                col = s.value.to_sql(schema)[0]
                for t, sql in col.sql.items():
                    json_type = sql_type_to_json_type[t]
                    if json_type in STRUCT:
                        continue
                    column_number = len(sql_selects)
                    # SQL HAS ABS TABLE REFERENCE
                    column_alias = _make_column_name(column_number)
                    sql_selects.append(sql + " AS " + column_alias)
                    if s.sort == -1:
                        sorts.append(column_alias + " IS NOT NULL")
                        sorts.append(column_alias + " DESC")
                    else:
                        sorts.append(column_alias + " IS NULL")
                        sorts.append(column_alias)

        primary_doc_details = Data()
        # EVERY SELECT STATEMENT THAT WILL BE REQUIRED, NO MATTER THE DEPTH
        # WE WILL CREATE THEM ACCORDING TO THE DEPTH REQUIRED
        for nested_path, sub_table in self.sf.tables.items():
            nested_doc_details = {
                "sub_table": sub_table,
                "children": [],
                "index_to_column": {},
                "nested_path": [nested_path]  # fake the real nested path, we only look at [0] anyway
            }

            # INSERT INTO TREE
            if not primary_doc_details:
                primary_doc_details = nested_doc_details
            else:
                def place(parent_doc_details):
                    if startswith_field(nested_path, parent_doc_details['nested_path'][0]):
                        for c in parent_doc_details['children']:
                            if place(c):
                                return True
                        parent_doc_details['children'].append(nested_doc_details)

                place(primary_doc_details)

            alias = nested_doc_details['alias'] = nest_to_alias[nested_path]

            # WE ALWAYS ADD THE UID AND ORDER
            column_number = index_to_uid[nested_path] = nested_doc_details['id_coord'] = len(sql_selects)
            sql_select = alias + "." + quoted_UID
            sql_selects.append(sql_select + " AS " + _make_column_name(column_number))
            if nested_path != ".":
                sql_select = alias + "." + quote_table(ORDER)
                sql_selects.append(sql_select + " AS " + _make_column_name(column_number+1))

            # WE DO NOT NEED DATA FROM TABLES WE REQUEST NOTHING FROM
            if nested_path not in active_columns:
                continue

            if len(active_columns[nested_path]) != 0:
                # ADD SQL SELECT COLUMNS FOR EACH jx SELECT CLAUSE
                si = 0
                for s in listwrap(query.select):
                    try:
                        column_number = len(sql_selects)
                        s.pull = get_column(column_number)
                        db_columns = s.value.to_sql(schema)

                        if isinstance(s.value, LeavesOp):
                            for column in db_columns:
                                for t, unsorted_sql in column.sql.items():
                                    json_type = sql_type_to_json_type[t]
                                    if json_type in STRUCT:
                                        continue
                                    column_number = len(sql_selects)
                                    # SQL HAS ABS TABLE REFERENCE
                                    column_alias = _make_column_name(column_number)
                                    sql_selects.append(unsorted_sql + " AS " + column_alias)
                                    index_to_column[column_number] = nested_doc_details['index_to_column'][column_number] = ColumnMapping(
                                        push_name=concat_field(s.name, column.name),
                                        push_column_name=concat_field(s.name, column.name),
                                        push_column=si,
                                        push_child=".",
                                        pull=get_column(column_number),
                                        sql=unsorted_sql,
                                        type=json_type,
                                        nested_path=[nested_path]
                                        # fake the real nested path, we only look at [0] anyway
                                    )
                                    si += 1
                        else:
                            for column in db_columns:
                                for t, unsorted_sql in column.sql.items():
                                    json_type = sql_type_to_json_type[t]
                                    if json_type in STRUCT:
                                        continue
                                    column_number = len(sql_selects)
                                    # SQL HAS ABS TABLE REFERENCE
                                    column_alias = _make_column_name(column_number)
                                    sql_selects.append(unsorted_sql + " AS " + column_alias)
                                    index_to_column[column_number] = nested_doc_details['index_to_column'][column_number] = ColumnMapping(
                                        push_name=s.name,
                                        push_column_name=s.name,
                                        push_column=si,
                                        push_child=column.name,
                                        pull=get_column(column_number),
                                        sql=unsorted_sql,
                                        type=json_type,
                                        nested_path=[nested_path]
                                        # fake the real nested path, we only look at [0] anyway
                                    )
                    finally:
                        si += 1
            elif startswith_field(nested_path, primary_nested_path):
                # ADD REQUIRED COLUMNS, FOR DEEP STUFF
                for ci, c in enumerate(active_columns[nested_path]):
                    if c.type in STRUCT:
                        continue

                    column_number = len(sql_selects)
                    nested_path = c.nested_path
                    unsorted_sql = nest_to_alias[nested_path[0]] + "." + quote_table(c.es_column)
                    column_alias = _make_column_name(column_number)
                    sql_selects.append(unsorted_sql + " AS " + column_alias)
                    index_to_column[column_number] = nested_doc_details['index_to_column'][column_number] = ColumnMapping(
                        push_name=s.name,
                        push_column_name=s.name,
                        push_column=si,
                        push_child=relative_field(c.names["."], s.name),
                        pull=get_column(column_number),
                        sql=unsorted_sql,
                        type=c.type,
                        nested_path=nested_path
                    )

        where_clause = query.where.to_sql(schema, boolean=True)[0].sql.b
        unsorted_sql = self._make_sql_for_one_nest_in_set_op(
            ".",
            sql_selects,
            where_clause,
            active_columns,
            index_to_column
        )

        for n, _ in self.sf.tables.items():
            sorts.append(COLUMN + unicode(index_to_uid[n]))

        ordered_sql = (
            "SELECT * FROM (\n" +
            unsorted_sql +
            "\n)" +
            "\nORDER BY\n" + ",\n".join(sorts) +
            "\nLIMIT " + quote_value(query.limit)
        )
        result = self.db.query(ordered_sql)

        def _accumulate_nested(rows, row, nested_doc_details, parent_doc_id, parent_id_coord):
            """
            :param rows: REVERSED STACK OF ROWS (WITH push() AND pop())
            :param row: CURRENT ROW BEING EXTRACTED
            :param nested_doc_details: {
                    "nested_path": wrap_nested_path(nested_path),
                    "index_to_column": map from column number to column details
                    "children": all possible direct decedents' nested_doc_details
                 }
            :param parent_doc_id: the id of the parent doc (for detecting when to step out of loop)
            :param parent_id_coord: the column number for the parent id (so we ca extract from each row)
            :return: the nested property (usually an array)
            """
            previous_doc_id = None
            doc = None
            output = []
            id_coord = nested_doc_details['id_coord']

            while True:
                doc_id = row[id_coord]

                if doc_id == None or (parent_id_coord is not None and row[parent_id_coord] != parent_doc_id):
                    rows.append(row)  # UNDO PREVIOUS POP (RECORD IS NOT A NESTED RECORD OF parent_doc)
                    output = unwraplist(output)
                    return output if output else None

                if doc_id != previous_doc_id:
                    previous_doc_id = doc_id
                    doc = None
                    curr_nested_path = nested_doc_details['nested_path'][0]
                    index_to_column = nested_doc_details['index_to_column'].items()
                    if index_to_column:
                        for i, c in index_to_column:
                            value = row[i]
                            if value == None:
                                continue
                            if value == '':
                                continue

                            if isinstance(query.select, list) or isinstance(query.select.value, LeavesOp):
                                # ASSIGN INNER PROPERTIES
                                relative_path=relative_field(join_field([c.push_name]+split_field(c.push_child)), curr_nested_path)
                            else:           # FACT IS EXPECTED TO BE A SINGLE VALUE, NOT AN OBJECT
                                relative_path=c.push_child

                            if relative_path == ".":
                                doc = value
                            elif doc is None:
                                doc = Data()
                                doc[relative_path] = value
                            else:
                                doc[relative_path] = value

                for child_details in nested_doc_details['children']:
                    # EACH NESTED TABLE MUST BE ASSEMBLED INTO A LIST OF OBJECTS
                    child_id = row[child_details['id_coord']]
                    if child_id is not None:
                        nested_value = _accumulate_nested(rows, row, child_details, doc_id, id_coord)
                        if nested_value is not None:
                            push_name = child_details['nested_path'][0]
                            if isinstance(query.select, list) or isinstance(query.select.value, LeavesOp):
                                # ASSIGN INNER PROPERTIES
                                relative_path=relative_field(push_name, curr_nested_path)
                            else:           # FACT IS EXPECTED TO BE A SINGLE VALUE, NOT AN OBJECT
                                relative_path="."

                            if relative_path == ".":
                                doc = nested_value
                            elif doc is None:
                                doc = Data()
                                doc[relative_path] = nested_value
                            else:
                                doc[relative_path] = nested_value

                output.append(doc)

                try:
                    row = rows.pop()
                except IndexError:
                    output = unwraplist(output)
                    return output if output else None

        cols = tuple(index_to_column.values())
        rows = list(reversed(unwrap(result.data)))
        if rows:
            row = rows.pop()
            data=listwrap(_accumulate_nested(rows, row, primary_doc_details, None, None))
        else:
            data = result.data

        if query.format == "cube":
            if len(data) == len(result.data):       # means no accumulated nested object
                data = result.data

                num_rows = len(data)
                num_cols = MAX([c.push_column for c in cols]) + 1 if len(cols) else 0
                map_index_to_name = {c.push_column: c.push_name for c in cols}
                temp_data = [[None]*num_rows for _ in range(num_cols)]
                for rownum, d in enumerate(data):
                    for c in cols:
                        if c.push_child == ".":
                            temp_data[c.push_column][rownum] = c.pull(d)
                        else:
                            column = temp_data[c.push_column][rownum]
                            if column is None:
                                column = temp_data[c.push_column][rownum] = {}
                            column[c.push_child] = c.pull(d)
                output = Data(
                    meta={"format": "cube"},
                    data={n: temp_data[c] for c, n in map_index_to_name.items()},
                    edges=[{
                        "name": "rownum",
                        "domain": {
                            "type": "rownum",
                            "min": 0,
                            "max": num_rows,
                            "interval": 1
                        }
                    }]
                )
                return output
            else:
                num_rows = len(data)
                map_index_to_name = {c.push_column: c.push_name for c in cols}
                temp_data = [data]

                return Data(
                    meta={"format": "cube"},
                    data={n: temp_data[c] for c, n in map_index_to_name.items()},
                    edges=[{
                        "name": "rownum",
                        "domain": {
                            "type": "rownum",
                            "min": 0,
                            "max": num_rows,
                            "interval": 1
                        }
                    }]
                )

        elif query.format == "table":
            if len(data)==len(result.data):
                data = result.data

                num_column = MAX([c.push_column for c in cols])+1
                header = [None]*num_column
                for c in cols:
                    header[c.push_column] = c.push_name

                output_data = []
                for d in data:
                    row = [None] * num_column
                    for c in cols:
                        set_column(row, c.push_column, c.push_child, c.pull(d))
                    output_data.append(row)

                return Data(
                    meta={"format": "table"},
                    header=header,
                    data=output_data
                )
            else:
                if isinstance(query.select, list):
                    column_names = listwrap(query.select).name
                    return Data(
                        meta={"format": "table"},
                        header=column_names,
                        data=data
                    )
                else:
                    column_names = listwrap(query.select).name
                    return Data(
                        meta={"format": "table"},
                        header=column_names,
                        data=[[d] for d in data]
                    )

        else:
            output = Data(
                meta={"format": "list"},
                data=data
            )
            return output

    def _make_sql_for_one_nest_in_set_op(
        self,
        primary_nested_path,
        selects,  # EVERY SELECT CLAUSE (NOT TO BE USED ON ALL TABLES, OF COURSE
        where_clause,
        active_columns,
        index_to_sql_select  # MAP FROM INDEX TO COLUMN (OR SELECT CLAUSE)
    ):
        """
        FOR EACH NESTED LEVEL, WE MAKE A QUERY THAT PULLS THE VALUES/COLUMNS REQUIRED
        WE `UNION ALL` THEM WHEN DONE
        :param primary_nested_path:
        :param selects:
        :param where_clause:
        :param active_columns:
        :param index_to_sql_select:
        :return: SQL FOR ONE NESTED LEVEL
        """

        parent_alias = "a"
        from_clause = ""
        select_clause = []
        children_sql = []
        done = []
        if not where_clause:
            where_clause = "1"
        # STATEMENT FOR EACH NESTED PATH
        for i, (nested_path, sub_table) in enumerate(self.sf.tables.items()):
            if any(startswith_field(nested_path, d) for d in done):
                continue

            alias = "__" + unichr(ord('a') + i) + "__"

            if primary_nested_path == nested_path:
                select_clause = []
                # ADD SELECT CLAUSE HERE
                for select_index, s in enumerate(selects):
                    sql_select = index_to_sql_select.get(select_index)
                    if not sql_select:
                        select_clause.append(selects[select_index])
                        continue

                    if startswith_field(sql_select.nested_path[0], nested_path):
                        select_clause.append(sql_select.sql + " AS " + _make_column_name(select_index))
                    else:
                        # DO NOT INCLUDE DEEP STUFF AT THIS LEVEL
                        select_clause.append("NULL AS " + _make_column_name(select_index))

                if nested_path == ".":
                    from_clause += "\nFROM " + quote_table(self.sf.fact) + " " + alias + "\n"
                else:
                    from_clause += "\nLEFT JOIN " + quote_table(concat_field(self.sf.fact,sub_table.name)) + " " + alias + "\n" \
                                                                                                " ON " + alias + "." + quoted_PARENT + " = " + parent_alias + "." + quoted_UID + "\n"
                    where_clause = "(" + where_clause + ") AND " + alias + "." + quote_table(ORDER) + " > 0\n"

            elif startswith_field(primary_nested_path, nested_path):
                # PARENT TABLE
                # NO NEED TO INCLUDE COLUMNS, BUT WILL INCLUDE ID AND ORDER
                if nested_path == ".":
                    from_clause += "\nFROM " + quote_table(self.sf.fact) + " " + alias + "\n"
                else:
                    parent_alias = alias = unichr(ord('a') + i - 1)
                    from_clause += "\nLEFT JOIN " + quote_table(concat_field(self.sf.fact,sub_table.name)) + " " + alias + \
                                   " ON " + alias + "." + quoted_PARENT + " = " + parent_alias + "." + quoted_UID
                    where_clause = "(" + where_clause + ") AND " + parent_alias + "." + quote_table(ORDER) + " > 0\n"

            elif startswith_field(nested_path, primary_nested_path):
                # CHILD TABLE
                # GET FIRST ROW FOR EACH NESTED TABLE
                from_clause += "\nLEFT JOIN " + quote_table(concat_field(self.sf.fact,sub_table.name)) + " " + alias + \
                               " ON " + alias + "." + quoted_PARENT + " = " + parent_alias + "." + quoted_UID + \
                               " AND " + alias + "." + quote_table(ORDER) + " = 0\n"

                # IMMEDIATE CHILDREN ONLY
                done.append(nested_path)
                # NESTED TABLES WILL USE RECURSION
                children_sql.append(self._make_sql_for_one_nest_in_set_op(
                    nested_path,
                    selects,  # EVERY SELECT CLAUSE (NOT TO BE USED ON ALL TABLES, OF COURSE
                    where_clause,
                    active_columns,
                    index_to_sql_select  # MAP FROM INDEX TO COLUMN (OR SELECT CLAUSE)
                ))
            else:
                # SIBLING PATHS ARE IGNORED
                continue

            parent_alias = alias

        sql = "\nUNION ALL\n".join(
            ["SELECT\n" + ",\n".join(select_clause) + from_clause + "\nWHERE\n" + where_clause] +
            children_sql
        )

        return sql

