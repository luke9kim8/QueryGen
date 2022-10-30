import os
import random
from sqlalchemy import select

import randoms
from common import *
from wheregen import *
from mutator import CreateSequences, TableStat

def print_title(title):
    print("\n\n")
    print("=" * len(title))
    print(title)
    print("=" * len(title))


def ret_where_child(obj, _list=False):
    if _list is False:
        if hasattr(obj, 'clauses'):
            # return obj.clauses
            return obj.clauses
        return None
    else:
        out = []
        for item in obj:
            if hasattr(item, 'clauses'):
                out = out + item.clauses

        if len(out) > 0:
            return out
        else:
            return None


class SelectMutation(object):

    def __init__(self, table_stat, table_spec, sqlalchemy_tables):
        self.tables_stat = table_stat
        self.tables_spec = table_spec
        self.sqlalchemy_tables = sqlalchemy_tables
        self.seed_query, self.seed_table, self.seed_all_columns\
            = self.generate_seedquery()
        # print (literalquery(self.seed_query))

    def generate_seedquery(self):
        """
        generate simple select statement

        1) randomly choose table
        2) choose all columns
        3) generate select statement
        """
        # print("generate_seedquery")
        # 1) randomly choose one table
        idx = random.choice(range(len(self.sqlalchemy_tables)))
        cur_sqlalchemy_table = self.sqlalchemy_tables[idx]

        # 2) choose all columns
        column_names = self.tables_stat[idx].column_name
        select_columns = CreateSequences.choose_columns_sqlalchemy(
            cur_sqlalchemy_table, column_names, option="all")

        # 3) return generated select statement
        stmt = select(select_columns)
        return stmt, cur_sqlalchemy_table, column_names

    def select_mutation(self, in_query=None):
        """
        input: initially generated query of query spec
         - if we have seed query, just use it
         - otherwise, we need to make a seed query
        output: mutated query
        """

        """
        - [ ] SELECT
          - [ ] the query can start from the first created table with all
            columns(which will be referenced)
          - [ ] change chosen columns (e.g., select a,b,c ==> select a,b)
          - [ ] add function on selected column (e.g., select a,b ==> select
            a, count(b))
          - [ ] add limit (and/or add offset)
          - [ ] add groupby
          - [ ] add having
          - [ ] add cast on selected column
          - [ ] add several one line selection fuzzer things
        """

        if "DEBUG" in os.environ:
            print_title("SELECT MUTATION")

        # print("\n[*] select mutation")
        if in_query is None:
            # print("no seed query available")
            cur_query = self.seed_query
        else:
            cur_query = in_query

        def select_all():
            """
            1) change the column list to ALL
            2) it should not modify the other clauses (e.g., where, having)
            """

            out = []
            for item in self.seed_all_columns:
                selected_col = getattr(self.seed_table.c, item)
                out.append(selected_col)
            query = select(out)

            # for checking index generation
            if "DEBUG" in os.environ:
                for table in self.sqlalchemy_tables:
                    if len(table.indexes) > 0:
                        print(table.indexes)

            # for test
            print("\n[*] Select all")
            print(literalquery(query))

        def select_change_chosen_columns():
            """
            1) randomly change the column list of the select query
            2) it should not modify the other clauses (e.g., where, having)
            """

            query = copy.deepcopy(cur_query)
            selected_columns = CreateSequences.choose_columns_sqlalchemy(
                self.seed_table, self.seed_all_columns, option="random")
            tmp_query = select(selected_columns)

            # How to change the column? should define another statemane
            #  - and we need to overwrite some attributes
            query.columns = tmp_query.columns
            query._raw_columns = tmp_query._raw_columns
            query._columns_plus_names = tmp_query._columns_plus_names

            print("\n[*] Select specific columns")
            print(literalquery(query))

        def select_add_function_on_column():
            print("\n[*] Select_add_function_on_column")

        def select_add_limit():
            """
            1) search for the total number of tuple inserted
            2) add limit within the range
            3) sometimes we use nasty number (-MININT, 0, neg, float, string)
            """

            query = copy.deepcopy(cur_query)

            # 1) return table stat
            tablename = cur_query._raw_columns[0].table.name
            cur_stat = TableStat.ret_tablestat_with_tblname(
                self.tables_stat, tablename)
            # num_tuples = cur_stat.table_size

            # 2) add limit
            limit, offset = CreateSequences.ret_limit_num(cur_stat)

            # TODO: 3) change limit to nasty number
            query = query.limit(limit)

            print("\n[*] Select add limit")
            print(literalquery(query))

        def select_add_limit_offset():

            query = copy.deepcopy(cur_query)

            # 1) return table stat
            tablename = cur_query._raw_columns[0].table.name
            cur_stat = TableStat.ret_tablestat_with_tblname(
                self.tables_stat, tablename)
            # num_tuples = cur_stat.table_size

            # 2) add limit
            limit, offset = CreateSequences.ret_limit_num(cur_stat)

            # TODO: 3) change limit to nasty number
            query = query.limit(limit).offset(offset)

            print("\n[*] Select add limit and offset")
            print(literalquery(query))

        def select_add_groupby():
            """
            1) enumerate column names from the current query
            2) randomly select columns
            3) use the column name in the groupby
            """

            print("\n[*] Select add groupby")

            # 1) enumerate column names
            query = copy.deepcopy(cur_query)
            chosen_columns = random.choices(
                query._raw_columns,
                k=randoms.random_int_range(len(query._raw_columns)))

            # If we need one column for groupby
            # chosen_columns = random.choice(query._raw_columns)

            query = query.group_by(*chosen_columns)
            print(literalquery(query))

        def select_add_having():
            """
            1) enumerate column names from the current query
            2) randomly select columns
            3) use the column name in the groupby
            4) add having

            skip 1) ~ 3) if we have groupby
            """

            print("\n[*] Select add groupby and having")
            query = copy.deepcopy(cur_query)

            # for testing the case when query has group_by
            if "DEBUG" in os.environ:
                chosen_columns = random.choices(
                    query._raw_columns,
                    k=randoms.random_int_range(len(query._raw_columns)))
                query = query.group_by(*chosen_columns)

            # if the query does not have group_by clause
            if len(query._group_by_clause.clauses) == 0:
                if "DEBUG" in os.environ:
                    print(" > when query does not have group_by")

                # 1) enumerate column names and select columns
                chosen_columns = random.choices(
                    query._raw_columns,
                    k=randoms.random_int_range(len(query._raw_columns)))

                # 2) add group_by clause
                query = query.group_by(*chosen_columns)

                # 3) select one column from group_by column list
                having_col = random.choice(chosen_columns)
                tablename = query._raw_columns[0].table.name
                tblstat = TableStat.ret_tablestat_with_tblname(
                    self.tables_stat, tablename)

                # 4) find the statistics of the data, and add having
                having_col_stat = tblstat.ret_stat(having_col.name)
                having_col_data = tblstat.ret_string(having_col.name)
                having_col_cond = where_generator(
                    having_col, None, having_col_stat, None, having_col_data)

                query = query.having(having_col_cond)
                print(literalquery(query))

            # if the query already has group_by clause
            # so we skip the above 1) ~ 3)
            else:
                if "DEBUG" in os.environ:
                    print(" > when query has group_by")

                # 1) select one column from the group_by-ed columns
                having_col = random.choice(query._group_by_clause.clauses)
                tablename = query._raw_columns[0].table.name
                tblstat = TableStat.ret_tablestat_with_tblname(
                    self.tables_stat, tablename)

                # 4) find the statistics of the data, and add having
                having_col_stat = tblstat.ret_stat(having_col.name)
                having_col_data = tblstat.ret_string(having_col.name)
                having_col_cond = where_generator(
                    having_col, None, having_col_stat, None, having_col_data)

                query = query.having(having_col_cond)
                print(literalquery(query))

        def select_add_cast_on_column():
            print("\n[*] Select add cast operation")

        # will not use at this time
        def select_oneline_fuzz():
            print("\n[*] Select oneline fuzzing")

        def select_add_distinct():

            query = copy.deepcopy(cur_query)
            query = query.distinct()

            print("\n[*] Select with DISTINCT")
            print(literalquery(query))

        mutation_candidates = [select_all,
                               select_change_chosen_columns,
                               select_add_function_on_column,
                               select_add_limit,
                               select_add_limit_offset,
                               select_add_groupby,
                               select_add_having,
                               select_add_cast_on_column,
                               select_add_distinct]

        # Randomly choose mutation
        # chosen_mutation = random.choice(mutation_candidates)
        # chosen_mutation()

        # Test all mutations
        for mutation in mutation_candidates:
            mutation()


class IndexMutation(object):

    def __init__(
            self, table_stat, table_spec, sqlalchemy_tables, sqlite_engine):
        self.tables_stat = table_stat
        self.tables_spec = table_spec
        self.sqlalchemy_tables = sqlalchemy_tables
        self.sqlite_engine = sqlite_engine
        self.seed_query, self.seed_table, self.seed_all_columns\
            = self.generate_seedquery()

    # we don't need this actually, only need for testing
    def generate_seedquery(self):
        """
        generate simple select statement

        1) randomly choose table
        2) choose all columns
        3) generate select statement
        """

        # 1) randomly choose one table
        idx = random.choice(range(len(self.sqlalchemy_tables)))
        cur_sqlalchemy_table = self.sqlalchemy_tables[idx]

        # 2) choose all columns
        column_names = self.tables_stat[idx].column_name
        select_columns = CreateSequences.choose_columns_sqlalchemy(
            cur_sqlalchemy_table, column_names, option="all")

        # 3) return generated select statement
        stmt = select(select_columns)
        return stmt, cur_sqlalchemy_table, column_names

    def create_composite_index(self, composite_func, target_col=None):

        # 1) find a cloumn without an index
        if target_col is None:
            selected_columns = CreateSequences.choose_columns_sqlalchemy(
                self.seed_table, self.seed_all_columns, option="wo_idx")
            target_column = random.choice(selected_columns)
        else:
            target_column = target_col

        tablename = self.seed_query._raw_columns[0].table.name
        cur_stat = TableStat.ret_tablestat_with_tblname(
            self.tables_stat, tablename)

        # 2) assign random func
        column_stat = cur_stat.ret_stat(target_column.name)
        column_data = cur_stat.ret_string(target_column.name)
        column_cond = composite_func(
            target_column, None, column_stat, None, column_data)

        # 3) create an index on the column
        idx_name = "%s" % (randoms.random_strings(6))
        idx1 = Index(idx_name, column_cond).create(self.sqlite_engine)

        return idx1, idx_name

    def index_mutation(self):

        if "DEBUG" in os.environ:
            print_title("INDEX MUTATION")

        def create_simple_index():
            """
            create index on one column

            1) emumerate index on the table
            2) select column which does not have index
            3) create index on the column (random name)
            """

            print("\n[*] Create simple index")

            # 1) emumerate index on the table
            selected_columns = CreateSequences.choose_columns_sqlalchemy(
                self.seed_table, self.seed_all_columns, option="wo_idx")

            # 2) select column without index
            target_column = random.choice(selected_columns)
            idx_name = "%s" % (randoms.random_strings(6))

            # 3) create an index on the column
            idx1 = Index(idx_name, target_column).create(self.sqlite_engine)

            # TODO: print "CREATE INDEX ON TABLE"
            print(idx1)

            # remove index in debug mode
            if "DEBUG" in os.environ:
                CreateSequences.remove_idx_by_name(
                    self.seed_table, idx_name, self.sqlite_engine)

        def create_func_index():
            """
            create one index with function on one column
            """
            print("\n[*] Create index with function")

            idx, idx_name = self.create_composite_index(func_generator)
            # print(idx, idx_name)

            # remove index in debug mode
            if "DEBUG" in os.environ:
                CreateSequences.remove_idx_by_name(
                    self.seed_table, idx_name, self.sqlite_engine)

        def create_cond_index():
            """
            create one index with conditions on column
            """
            print("\n[*] Create index with condition")

            idx, idx_name = self.create_composite_index(cond_generator)
            print(idx, idx_name)

            # remove index in debug mode
            if "DEBUG" in os.environ:
                CreateSequences.remove_idx_by_name(
                    self.seed_table, idx_name, self.sqlite_engine)

        def _modify_index(composite_func):
            # FOR TEST: create index with some cond/func
            if "DEBUG" in os.environ:
                idx, idx_name = self.create_composite_index(composite_func)

            # 1) choose one column with idx
            indexes = list(self.seed_table.indexes)
            if len(indexes) > 0:
                chosen_indexes = random.choice(indexes)
            else:
                return  # just return the original object
            column_name = str(list(chosen_indexes.columns)[0]).split(".")[1]

            # 2) remove index with that column

            # 2-1) find out which column we are interested
            target_column = CreateSequences.ret_column_by_name(
                self.seed_table, column_name)

            # 2-2) remote index
            out = []
            chosen_indexes.drop(self.sqlite_engine)
            out.append(chosen_indexes)
            self.seed_table.indexes =\
                self.seed_table.indexes.difference(set(out))

            # 3) set up index with new function
            self.create_composite_index(
                composite_func, target_col=target_column)
            # print(self.seed_table.indexes)

        def modify_func_index():
            """
            * modify one column to index with function

            1) choose one column with idx
            2) remove index with that column
            3) set up index with new function
            """

            print("\n[*] Modify index with function")
            _modify_index(func_generator)

        def modify_cond_index():
            """
            * modify one column to index with a condition

            1) choose column with idx
            2) remove index with that column
            3) set up index with new function
            """

            print("\n[*] Modify index with condition")
            _modify_index(cond_generator)

        def remove_all_index():

            print("\n[*] Remove all indices")
            indexes = list(self.seed_table.indexes)
            # print(indexes)
            for idx in indexes:
                idx.drop(self.sqlite_engine)

            self.seed_table.indexes = set()

        def remove_some_index():
            """
            1) choose column with idx
            2) randomly remove some idx
            """

            print("\n[*] Remove some indices")

            if "DEBUG" in os.environ:
                # print("\n[DEBUG] temporal index generation for test")

                # a) emumerate index on the table
                selected_columns = CreateSequences.choose_columns_sqlalchemy(
                    self.seed_table, self.seed_all_columns, option="wo_idx")

                # b) select column without index
                target_column = random.choice(selected_columns)
                idx_name = "%s" % (randoms.random_strings(6))

                # c) create an index on the column
                Index(idx_name, target_column).create(
                    self.sqlite_engine)
                print(" >> index generated")

            # 1) choose column with idx
            out = []
            indexes = list(self.seed_table.indexes)

            if len(indexes) > 0:
                chosen_indexes = random.choices(
                    indexes, k=randoms.random_int_range(len(indexes)))
            else:
                chosen_indexes = []

            # 2) remove some indexes
            for idx in chosen_indexes:
                out.append(idx)
                idx.drop(self.sqlite_engine)

            self.seed_table.indexes =\
                self.seed_table.indexes.difference(set(out))

        # we randomly choose mutation function
        mutation_candidates = [create_simple_index,
                               create_func_index,
                               create_cond_index,
                               modify_func_index,
                               modify_cond_index,
                               remove_all_index,
                               remove_some_index]

        # Test all mutations
        # call each mutation function only once
        for mutation in mutation_candidates:
            mutation()


class WhereMutation(object):

    def __init__(
            self, table_stat, table_spec, sqlalchemy_tables, sqlite_engine):
        self.tables_stat = table_stat
        self.tables_spec = table_spec
        self.sqlalchemy_tables = sqlalchemy_tables
        self.sqlite_engine = sqlite_engine
        self.seed_query, self.seed_table, self.seed_all_columns, self.tblstat \
            = self.generate_seedquery()

    # we don't need this actually, only need for testing
    def generate_seedquery(self):
        """
        generate simple select statement

        1) randomly choose table
        2) choose all columns
        3) generate select statement
        """

        # 1) randomly choose one table
        idx = random.choice(range(len(self.sqlalchemy_tables)))
        cur_sqlalchemy_table = self.sqlalchemy_tables[idx]

        # 2) choose all columns
        column_names = self.tables_stat[idx].column_name
        target_column = CreateSequences.choose_columns_sqlalchemy(
            cur_sqlalchemy_table, column_names, option="one")[0]
        all_columns = CreateSequences.choose_columns_sqlalchemy(
            cur_sqlalchemy_table, column_names, option="all")

        tablename = cur_sqlalchemy_table.name
        tblstat = TableStat.ret_tablestat_with_tblname(
            self.tables_stat, tablename)

        # 3) add condition
        col_stat = tblstat.ret_stat(target_column.name)
        col_data = tblstat.ret_string(target_column.name)

        where_function = random.choice(
            [cond_generator, where_func_generator])
        col_cond1 = where_function(
            target_column, None, col_stat, None, col_data)

        where_function = random.choice(
            [cond_generator, where_func_generator])
        col_cond2 = where_function(
            target_column, None, col_stat, None, col_data)

        where_function = random.choice(
            [cond_generator, where_func_generator])
        col_cond3 = where_function(
            target_column, None, col_stat, None, col_data)

        # 3) return generated select statement
        composite_cond = or_(and_(col_cond1, col_cond2), col_cond3)
        stmt = select(all_columns).where(composite_cond)
        return stmt, cur_sqlalchemy_table, column_names, tblstat

    def _where_gen(self, gen_func, conjunct, ret_cond=False):
        """
        We create one random where condition

        1) select one column
        2) create where using either cond or func
        3) add to the where clause

        gen_func: cond_generator, where_func_generator
        conjunct: and_, or_
        """

        # 1) select one column
        target_col = CreateSequences.choose_columns_sqlalchemy(
            self.seed_table, self.seed_all_columns, option="one")[0]

        # 2) create where using either cond or func
        current_where = self.seed_query._whereclause
        col_stat = self.tblstat.ret_stat(target_col.name)
        col_data = self.tblstat.ret_string(target_col.name)
        new_cond = gen_func(
            target_col, None, col_stat, None, col_data)

        if ret_cond is True:
            return new_cond

        # 3) add to the where clause: randomly selects between AND, OR
        current_where = conjunct(current_where, new_cond)
        stmt = copy.deepcopy(self.seed_query)
        stmt._whereclause = current_where

        return stmt

    def traverse_where(self, where, target):
        """
        traverse conditions in the where clause
         - where_clause
         - target: cond or func

        if BooleanClauseList: visit child
        if Grouping: store
        """

        out = []
        candidates = ret_where_child(where)
        while True:
            FurtherVisit = []
            if candidates is None:
                break

            for child in candidates:
                if "BooleanClauseList" in type(child).__name__:
                    FurtherVisit.append(child)
                elif "Grouping" in type(child).__name__\
                    or "BinaryE" in type(child).__name__:
                    out.append(child)
            candidates = ret_where_child(FurtherVisit, _list=True)

        # print("OUT:", out)
        return out

    def ret_active_col_from_where(self, where_cond):
        """
        Return active column from the where_clause

        @input: BinaryExpression
        @output: table_name, Column
        """

        if type(where_cond.left).__name__ == "Column":
            return where_cond.left.table.name, where_cond.left, "left"
        elif type(where_cond.right).__name__ == "Column":
            return where_cond.right.table.name, where_cond.right, "right"
        raise Exception('Not possible!')

    def where_mutation(self):

        if "DEBUG" in os.environ:
            print_title("WHERE MUTATION")

        def create_where():
            """
            We create one random where condition

            1) select one column
            2) create where using either cond or func
            3) add to the where clause
            """

            print("\n[*] Create new where condition")
            where_function = random.choice(
                [cond_generator, where_func_generator])
            conjunct = random.choice([and_, or_])
            stmt = self._where_gen(where_function, conjunct)
            print(literalquery(stmt))

        def flip_condition():
            """
            1) find column with where condition
            2) flip the condition
            """

            print("\n[*] Flip where condition")

            stmt = copy.deepcopy(self.seed_query)
            stmt._whereclause = ~stmt._whereclause
            print(literalquery(stmt))

        def add_condition():
            """
            1) add AND, OR condition
            """

            print("\n[*] Add new where condition")
            conjunct = random.choice([and_, or_])
            stmt = self._where_gen(cond_generator, conjunct)
            print(literalquery(stmt))

        def add_function():
            """
            1) add AND, OR function
            """

            print("\n[*] Add function on column")
            conjunct = random.choice([and_, or_])
            stmt = self._where_gen(where_func_generator, conjunct)
            print(literalquery(stmt))

        def change_condition_type():
            """
            1) traverse and select one condition in the where
             - should store in the list, then randomly select
            2) change to another condition
            """

            print("\n[*] Change condition type")

            # 1) traverse and select one condition
            current_where = self.seed_query._whereclause
            cond_candidates = self.traverse_where(current_where, "cond")
            where_cond = random.choice(cond_candidates)

            # TODO: currently, we are not using this actually!!
            # 2) mutate that condition
            # 2-1) if we observe column in the BinaryExpression
            if type(where_cond.left).__name__ == "Column" or\
                type(where_cond.right).__name__ == "Column":
                cur_tblname, cur_col, direction = \
                    self.ret_active_col_from_where(where_cond)
                cur_tbl = TableStat.ret_table_with_tblname(
                    self.sqlalchemy_tables, cur_tblname)
                cur_tblstat = TableStat.ret_tablestat_with_tblname(
                    self.tables_stat, cur_tblname)
                columns = cur_tblstat.column_name

                dst_col = CreateSequences.choose_columns_sqlalchemy(
                    cur_tbl, columns, option="one")[0]

                current_where = self.seed_query._whereclause
                col_stat = cur_tblstat.ret_stat(dst_col.name)
                col_data = cur_tblstat.ret_string(dst_col.name)

                # print("ABF", where_cond)
                where_cond = mutate_cond_column(
                    where_cond, cur_col, dst_col, col_stat,
                    col_data, direction)
                # print("AAF", where_cond)

            # 2-2) if we cannot observe the BinaryExpression
            else:
                # print("BF", where_cond)
                where_cond = mutate_cond_value(where_cond)
                # print("AF", where_cond)

            print(literalquery(self.seed_query))

        def change_function_type():
            """
            1) traverse and select one with function
             - should store in the list, then randomly select
            2) change value or change operator
            """
            # TODO: change column in the function
            # TODO: change function itself (e.g., min ==> max)

            print("\n[*] Change function type")

            current_where = self.seed_query._whereclause
            cond_candidates = self.traverse_where(current_where, "cond")
            where_cond = random.choice(cond_candidates)

            if "Function" in type(where_cond.left).__name__ or\
                "Function" in type(where_cond.right).__name__:

                # TODO: assuming the function on my left
                tbl_name, column_name = str(where_cond.left)\
                    .split("(")[1].split(")")[0].split(".")
                cur_tbl = TableStat.ret_table_with_tblname(
                    self.sqlalchemy_tables, tbl_name.replace("\"", ""))
                cur_tblstat = TableStat.ret_tablestat_with_tblname(
                    self.tables_stat, tbl_name.replace("\"", ""))
                columns = cur_tblstat.column_name
                dst_col = CreateSequences.choose_columns_sqlalchemy(
                    cur_tbl, columns, option="one")[0]
                """
                cur_col = CreateSequences.ret_column_by_name(
                    cur_tbl, column_name.replace("\"", ""))
                """
                where_cond.left = mutate_func(where_cond.left, dst_col)

        def add_predicate_value():
            """
            1) add TRUE/FALSE condition
            """

            print("\n[*] Add predicate data value")

            # 1) traverse and select one condition
            current_where = self.seed_query._whereclause
            cond_candidates = self.traverse_where(current_where, "cond")
            where_cond = random.choice(cond_candidates)

            where_cond = mutate_cond_value(where_cond, TF=True)
            print(literalquery(self.seed_query))

        def add_casting():
            """
            1) create new condition with casting
             - e.g., a > 1 ==> cast(a as text)
            """

            print("\n[*] Add casting operation")

            where_function = cast_generator
            conjunct = random.choice([and_, or_])
            stmt = self._where_gen(where_function, conjunct)
            print(literalquery(stmt))

            exit()

        def add_nasty_data():
            """
            1) use nasty value
            """

            print("\n[*] Use nasty data")

        def add_nasty_relationship():
            """
            1) use nasty relation
            """

            print("\n[*] Add nasty relationships")

        def remove_condition():
            """
            1) select condition in where
            2) remove the condition
            """

            print("\n[*] Remove condition")

        # we randomly choose mutation function
        mutation_candidates = [create_where,
                               flip_condition,
                               add_condition,
                               add_function,
                               change_condition_type,
                               change_function_type,
                               add_predicate_value,
                               add_casting,
                               add_nasty_data,
                               add_nasty_relationship,
                               remove_condition]

        # Test all mutations
        for mutation in mutation_candidates:
            mutation()
