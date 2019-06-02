# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#


from __future__ import absolute_import, division, unicode_literals

from jx_base import Column
from jx_sqlite.expressions import BooleanOp
from jx_base.language import is_op
from jx_base.queries import get_property_name
from jx_sqlite import COLUMN, ColumnMapping, ORDER, _make_column_name, get_column, quoted_ORDER, quoted_PARENT, quoted_UID, set_column
from jx_sqlite.expressions import LeavesOp, SQLang, sql_type_to_json_type
from jx_sqlite.insert_table import InsertTable
from mo_dots import Data, Null, concat_field, is_list, listwrap, literal_field, startswith_field, tail_field, unwrap, unwraplist
from mo_future import text_type, unichr
from mo_json import IS_NULL, STRUCT
from mo_math import MAX, UNION
from mo_times import Date
from pyLibrary.sql import SQL_AND, SQL_FROM, SQL_IS_NOT_NULL, SQL_IS_NULL, SQL_LEFT_JOIN, SQL_LIMIT, SQL_NULL, SQL_ON, SQL_ORDERBY, SQL_SELECT, SQL_TRUE, SQL_UNION_ALL, SQL_WHERE, sql_alias, sql_iso, sql_list
from pyLibrary.sql.sqlite import join_column, quote_column, quote_value, json_type_to_sqlite_type


class SetOpTable(InsertTable):
    def _set_op(self, query):
        # GET LIST OF SELECTED COLUMNS
        vars_ = UNION([v.var for select in listwrap(query.select) for v in select.value.vars()])
        schema = self.schema
        known_vars = schema.keys()

        active_columns = {".": set()}
        for v in vars_:
            for c in schema.leaves(v):
                nest = c.nested_path[0]
                active_columns.setdefault(nest, set()).add(c)

        # ANY VARS MENTIONED WITH NO COLUMNS?
        for v in vars_:
            if not any(startswith_field(cname, v) for cname in known_vars):
                active_columns["."].add(Column(
                    name=v,
                    jx_type=IS_NULL,
                    es_column=".",
                    es_index=".",
                    es_type=json_type_to_sqlite_type[IS_NULL],
                    nested_path=["."],
                    last_updated=Date.now()
                ))

        # EVERY COLUMN, AND THE INDEX IT TAKES UP
        index_to_column = {}  # MAP FROM INDEX TO COLUMN (OR SELECT CLAUSE)
        index_to_uid = {}  # FROM NESTED PATH TO THE INDEX OF UID
        sql_selects = []  # EVERY SELECT CLAUSE (NOT TO BE USED ON ALL TABLES, OF COURSE)
        nest_to_alias = {
            nested_path: "__" + unichr(ord('a') + i) + "__"
            for i, nested_path in enumerate(self.sf.query_paths)
        }

        sorts = []
        if query.sort:
            for select in query.sort:
                col = SQLang[select.value].to_sql(schema)[0]
                for t, sql in col.sql.items():
                    json_type = sql_type_to_json_type[t]
                    if json_type in STRUCT:
                        continue
                    column_number = len(sql_selects)
                    # SQL HAS ABS TABLE REFERENCE
                    column_alias = _make_column_name(column_number)
                    sql_selects.append(sql_alias(sql, column_alias))
                    if select.sort == -1:
                        sorts.append(column_alias + SQL_IS_NOT_NULL)
                        sorts.append(column_alias + " DESC")
                    else:
                        sorts.append(column_alias + SQL_IS_NULL)
                        sorts.append(column_alias)

        primary_doc_details = Data()
        # EVERY SELECT STATEMENT THAT WILL BE REQUIRED, NO MATTER THE DEPTH
        # WE WILL CREATE THEM ACCORDING TO THE DEPTH REQUIRED
        nested_path = []
        for step, sub_table in self.sf.tables:
            nested_path.insert(0, step)
            nested_doc_details = {
                "sub_table": sub_table,
                "children": [],
                "index_to_column": {},
                "nested_path": nested_path
            }

            # INSERT INTO TREE
            if not primary_doc_details:
                primary_doc_details = nested_doc_details
            else:
                def place(parent_doc_details):
                    if startswith_field(step, parent_doc_details['nested_path'][0]):
                        for c in parent_doc_details['children']:
                            if place(c):
                                return True
                        parent_doc_details['children'].append(nested_doc_details)

                place(primary_doc_details)

            alias = nested_doc_details['alias'] = nest_to_alias[step]

            # WE ALWAYS ADD THE UID
            column_number = index_to_uid[step] = nested_doc_details['id_coord'] = len(sql_selects)
            sql_select = join_column(alias, quoted_UID)
            sql_selects.append(sql_alias(sql_select, _make_column_name(column_number)))
            if step != ".":
                # ID AND ORDER FOR CHILD TABLES
                index_to_column[column_number] = ColumnMapping(
                    sql=sql_select,
                    type="number",
                    nested_path=nested_path,
                    column_alias=_make_column_name(column_number)
                )
                column_number = len(sql_selects)
                sql_select = join_column(alias, quoted_ORDER)
                sql_selects.append(sql_alias(sql_select, _make_column_name(column_number)))
                index_to_column[column_number] = ColumnMapping(
                    sql=sql_select,
                    type="number",
                    nested_path=nested_path,
                    column_alias=_make_column_name(column_number)
                )

            # WE DO NOT NEED DATA FROM TABLES WE REQUEST NOTHING FROM
            if step not in active_columns:
                continue

            # ADD SQL SELECT COLUMNS FOR EACH jx SELECT CLAUSE
            si = 0
            for select in listwrap(query.select):
                try:
                    column_number = len(sql_selects)
                    select.pull = get_column(column_number)
                    db_columns = SQLang[select.value].partial_eval().to_sql(schema)

                    for column in db_columns:
                        for t, unsorted_sql in column.sql.items():
                            json_type = sql_type_to_json_type[t]
                            if json_type in STRUCT:
                                continue
                            column_number = len(sql_selects)
                            column_alias = _make_column_name(column_number)
                            sql_selects.append(sql_alias(unsorted_sql, column_alias))
                            if startswith_field(schema.path, step) and is_op(select.value, LeavesOp):
                                # ONLY FLATTEN primary_nested_path AND PARENTS, NOT CHILDREN
                                index_to_column[column_number] = nested_doc_details['index_to_column'][column_number] = ColumnMapping(
                                    push_name=literal_field(get_property_name(concat_field(select.name, column.name))),
                                    push_child=".",
                                    push_column_name=get_property_name(concat_field(select.name, column.name)),
                                    push_column=si,
                                    pull=get_column(column_number),
                                    sql=unsorted_sql,
                                    type=json_type,
                                    column_alias=column_alias,
                                    nested_path=nested_path
                                )
                                si += 1
                            else:
                                index_to_column[column_number] = nested_doc_details['index_to_column'][column_number] = ColumnMapping(
                                    push_name=select.name,
                                    push_child=column.name,
                                    push_column_name=select.name,
                                    push_column=si,
                                    pull=get_column(column_number),
                                    sql=unsorted_sql,
                                    type=json_type,
                                    column_alias=column_alias,
                                    nested_path=nested_path
                                )
                finally:
                    si += 1

        where_clause = BooleanOp(query.where).partial_eval().to_sql(schema, boolean=True)[0].sql.b
        unsorted_sql = self._make_sql_for_one_nest_in_set_op(
            ".",
            sql_selects,
            where_clause,
            active_columns,
            index_to_column
        )

        for n, _ in self.sf.tables:
            sorts.append(quote_column(COLUMN + text_type(index_to_uid[n])))

        ordered_sql = (
            SQL_SELECT + "*" +
            SQL_FROM + sql_iso(unsorted_sql) +
            SQL_ORDERBY + sql_list(sorts) +
            SQL_LIMIT + quote_value(query.limit)
        )
        self.db.create_new_functions()  # creating new functions: regexp
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
            doc = Data()
            output = []
            id_coord = nested_doc_details['id_coord']

            while True:
                doc_id = row[id_coord]

                if doc_id == None or (parent_id_coord is not None and row[parent_id_coord] != parent_doc_id):
                    rows.append(row)  # UNDO PREVIOUS POP (RECORD IS NOT A NESTED RECORD OF parent_doc)
                    return output

                if doc_id != previous_doc_id:
                    previous_doc_id = doc_id
                    doc = Data()
                    curr_nested_path = nested_doc_details['nested_path'][0]
                    index_to_column = nested_doc_details['index_to_column'].items()
                    if index_to_column:
                        for i, c in index_to_column:
                            value = row[i]
                            if is_list(query.select) or is_op(query.select.value, LeavesOp):
                                # ASSIGN INNER PROPERTIES
                                relative_field = concat_field(c.push_name, c.push_child)
                            else:  # FACT IS EXPECTED TO BE A SINGLE VALUE, NOT AN OBJECT
                                relative_field = c.push_child

                            if relative_field == ".":
                                if value == '':
                                    doc = Null
                                else:
                                    doc = value
                            elif value != None and value != '':
                                doc[relative_field] = value

                for child_details in nested_doc_details['children']:
                    # EACH NESTED TABLE MUST BE ASSEMBLED INTO A LIST OF OBJECTS
                    child_id = row[child_details['id_coord']]
                    if child_id is not None:
                        nested_value = _accumulate_nested(rows, row, child_details, doc_id, id_coord)
                        if nested_value:
                            push_name = child_details['nested_path'][0]
                            if is_list(query.select) or is_op(query.select.value, LeavesOp):
                                # ASSIGN INNER PROPERTIES
                                relative_field = relative_field(push_name, curr_nested_path)
                            else:  # FACT IS EXPECTED TO BE A SINGLE VALUE, NOT AN OBJECT
                                relative_field = "."

                            if relative_field == "." and doc is Null:
                                doc = nested_value
                            elif relative_field == ".":
                                doc = unwraplist(nested_value)
                            else:
                                doc[relative_field] = unwraplist(nested_value)

                output.append(doc)

                try:
                    row = rows.pop()
                except IndexError:
                    return output

        cols = tuple([i for i in index_to_column.values() if i.push_name != None])
        rows = list(reversed(unwrap(result.data)))
        if rows:
            row = rows.pop()
            data = _accumulate_nested(rows, row, primary_doc_details, None, None)
        else:
            data = result.data

        if query.format == "cube":
            # for f, full_name in self.sf.tables:
            #     if f != '.' or (test_dots(cols) and is_list(query.select)):
            #         num_rows = len(result.data)
            #         num_cols = MAX([c.push_column for c in cols]) + 1 if len(cols) else 0
            #         map_index_to_name = {c.push_column: c.push_column_name for c in cols}
            #         temp_data = [[None] * num_rows for _ in range(num_cols)]
            #         for rownum, d in enumerate(result.data):
            #             for c in cols:
            #                 if c.push_child == ".":
            #                     temp_data[c.push_column][rownum] = c.pull(d)
            #                 else:
            #                     column = temp_data[c.push_column][rownum]
            #                     if column is None:
            #                         column = temp_data[c.push_column][rownum] = {}
            #                     column[c.push_child] = c.pull(d)
            #         output = Data(
            #             meta={"format": "cube"},
            #             data={n: temp_data[c] for c, n in map_index_to_name.items()},
            #             edges=[{
            #                 "name": "rownum",
            #                 "domain": {
            #                     "type": "rownum",
            #                     "min": 0,
            #                     "max": num_rows,
            #                     "interval": 1
            #                 }
            #             }]
            #         )
            #         return output

            if is_list(query.select) or is_op(query.select.value, LeavesOp):
                num_rows = len(data)
                temp_data = {c.push_column_name: [None] * num_rows for c in cols}
                for rownum, d in enumerate(data):
                    for c in cols:
                        temp_data[c.push_column_name][rownum] = d[c.push_name]
                return Data(
                    meta={"format": "cube"},
                    data=temp_data,
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
            else:
                num_rows = len(data)
                map_index_to_name = {c.push_column: c.push_column_name for c in cols}
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
            # for f, _ in self.sf.tables:
            #     if frum.endswith(f):
            #         num_column = MAX([c.push_column for c in cols]) + 1
            #         header = [None] * num_column
            #         for c in cols:
            #             header[c.push_column] = c.push_column_name
            #
            #         output_data = []
            #         for d in result.data:
            #             row = [None] * num_column
            #             for c in cols:
            #                 set_column(row, c.push_column, c.push_child, c.pull(d))
            #             output_data.append(row)
            #
            #         return Data(
            #             meta={"format": "table"},
            #             header=header,
            #             data=output_data
            #         )
            if is_list(query.select) or is_op(query.select.value, LeavesOp):
                column_names = [None] * (max(c.push_column for c in cols) + 1)
                for c in cols:
                    column_names[c.push_column] = c.push_column_name

                temp_data = []
                for rownum, d in enumerate(data):
                    row = [None] * len(column_names)
                    for c in cols:
                        row[c.push_column] = d[c.push_name]
                    temp_data.append(row)

                return Data(
                    meta={"format": "table"},
                    header=column_names,
                    data=temp_data
                )
            else:
                column_names = listwrap(query.select).name
                return Data(
                    meta={"format": "table"},
                    header=column_names,
                    data=[[d] for d in data]
                )

        else:
            # for f, _ in self.sf.tables:
            #     if frum.endswith(f) or (test_dots(cols) and is_list(query.select)):
            #         data = []
            #         for d in result.data:
            #             row = Data()
            #             for c in cols:
            #                 if c.push_child == ".":
            #                     row[c.push_name] = c.pull(d)
            #                 elif c.num_push_columns:
            #                     tuple_value = row[c.push_name]
            #                     if not tuple_value:
            #                         tuple_value = row[c.push_name] = [None] * c.num_push_columns
            #                     tuple_value[c.push_child] = c.pull(d)
            #                 else:
            #                     row[c.push_name][c.push_child] = c.pull(d)
            #
            #             data.append(row)
            #
            #         return Data(
            #             meta={"format": "list"},
            #             data=data
            #         )

            if is_list(query.select) or is_op(query.select.value, LeavesOp):
                temp_data = []
                for rownum, d in enumerate(data):
                    row = {}
                    for c in cols:
                        row[c.push_column_name] = d[c.push_name]
                    temp_data.append(row)
                return Data(
                    meta={"format": "list"},
                    data=temp_data
                )
            else:
                return Data(
                    meta={"format": "list"},
                    data=data
                )

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
            where_clause = SQL_TRUE
        # STATEMENT FOR EACH NESTED PATH
        for i, (nested_path, sub_table) in enumerate(self.sf.tables):
            if any(startswith_field(nested_path, d) for d in done):
                continue

            alias = quote_column("__" + unichr(ord('a') + i) + "__")

            if primary_nested_path == nested_path:
                select_clause = []
                # ADD SELECT CLAUSE HERE
                for select_index, s in enumerate(selects):
                    sql_select = index_to_sql_select.get(select_index)
                    if not sql_select:
                        select_clause.append(selects[select_index])
                        continue

                    if startswith_field(sql_select.nested_path[0], nested_path):
                        select_clause.append(sql_alias(sql_select.sql, sql_select.column_alias))
                    else:
                        # DO NOT INCLUDE DEEP STUFF AT THIS LEVEL
                        select_clause.append(sql_alias(SQL_NULL, sql_select.column_alias))

                if nested_path == ".":
                    from_clause += SQL_FROM + sql_alias(quote_column(self.sf.fact_name), alias)
                else:
                    from_clause += (
                        SQL_LEFT_JOIN + sql_alias(quote_column(concat_field(self.sf.fact_name, sub_table.name)), alias) +
                        SQL_ON + join_column(alias, quoted_PARENT) + " = " + join_column(parent_alias, quoted_UID)
                    )
                    where_clause = sql_iso(where_clause) + SQL_AND + join_column(alias, quoted_ORDER) + " > 0"
                parent_alias = alias

            elif startswith_field(primary_nested_path, nested_path):
                # PARENT TABLE
                # NO NEED TO INCLUDE COLUMNS, BUT WILL INCLUDE ID AND ORDER
                if nested_path == ".":
                    from_clause += SQL_FROM + quote_column(self.sf.fact_name + " " + alias)
                else:
                    parent_alias = alias = unichr(ord('a') + i - 1)
                    from_clause += (
                        SQL_LEFT_JOIN + quote_column(concat_field(self.sf.fact_name, sub_table.name)) + " " + alias +
                        SQL_ON + join_column(alias, quoted_PARENT) + " = " + join_column(parent_alias, quoted_UID)
                    )
                    where_clause = sql_iso(where_clause) + SQL_AND + join_column(parent_alias, quoted_ORDER) + " > 0"
                parent_alias = alias

            elif startswith_field(nested_path, primary_nested_path):
                # CHILD TABLE
                # GET FIRST ROW FOR EACH NESTED TABLE
                from_clause += (
                    SQL_LEFT_JOIN + sql_alias(quote_column(concat_field(self.sf.fact_name, sub_table.name)), alias) +
                    SQL_ON + join_column(alias, quoted_PARENT) + " = " + join_column(parent_alias, quoted_UID) +
                    SQL_AND + join_column(alias, ORDER) + " = 0"
                )

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

        sql = SQL_UNION_ALL.join(
            [
                SQL_SELECT + sql_list(select_clause) +
                from_clause +
                SQL_WHERE + where_clause
            ] +
            children_sql
        )

        return sql


def test_dots(cols):
    for c in cols:
        if "\\" in c.push_column_name:
            return True
    return False
