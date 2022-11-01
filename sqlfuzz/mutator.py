#!/usr/bin/env python3
import os
import sys
import signal
import random
import shutil
import argparse
import subprocess
import json
from statistics import mean
import traceback
import sys
import conf
from loader import *
from model import *
from mutator_conf import *
from common import *
from PrincipledMutation import *

from sqlalchemy import create_engine, Table, Column, \
    String, DateTime, MetaData, ForeignKey
from sqlalchemy import select, join, alias, true, false
from sqlalchemy.schema import CreateTable
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.sql.sqltypes import *
from sqlalchemy.dialects import postgresql  # mysql, sqlite,
from sqlalchemy.orm import load_only
from sqlalchemy import or_
from sqlalchemy import not_
from sqlalchemy import inspect

"""
./mutator.py -s select
./mutator.py -s sequence
HOW TO RUN IT WITH EXISTING DATABASES:
./mutator.py --db_info=db_conf.json --output=demo.sql -s seq
"""

# TODO:
"""
1. array
 - mytable = Table("mytable", metadata, Column("data", ARRAY(Integer)))
2. enum
 - import enum
 - class MyEnum(enum.Enum):
    one = 1     two = 2     three = 3
 - t = Table('data', MetaData(), Column('value', Enum(MyEnum)))
3. Sequence
"""

# reference: https://www.manuelrigger.at/pqs/


def exit_gracefully(original_sigint):
    def _exit_gracefully(signum, frame):
        signal.signal(signal.SIGINT, original_sigint)
        try:
            if input("\nReally quit? (y/n)> ").lower().startswith('y'):
                sys.exit(1)
        except KeyboardInterrupt:
            print("Ok ok, quitting")
            sys.exit(1)
        signal.signal(signal.SIGINT, _exit_gracefully)

    return _exit_gracefully


def mkdirs(pn):
    try:
        os.makedirs(pn)
    except OSError:
        pass


def rmdirs(pn):
    try:
        shutil.rmtree(pn)
    except OSError:
        pass


def run_query_pg(query):

    with open(TMP_QUERY, 'w') as f:
        f.write(query)

    cmd = "timeout 5s psql -t -F ',' --no-align -f %s" % TMP_QUERY
    print(subprocess.getoutput(cmd))


def run_query_my(query):

    with open(TMP_QUERY, 'w') as f:
        f.write(query)

    cmd = "timeout 5s mysql -N --skip-column-names -u mysql -pmysql  < %s" \
        % TMP_QUERY
    subprocess.getoutput(cmd)


def run_set_operation(query1, query2):
    chosen_operation = random.choice(SET_OPERATION)
    if chosen_operation == "intersect":
        print("try intersect")
        return query1.intersect(query2)
    elif chosen_operation == "intersect_all":
        print("try intersect all")
        return query1.intersect_all(query2)
    elif chosen_operation == "union":
        print("try union")
        return query1.union(query2)
    elif chosen_operation == "union_all":
        print("try union all")
        return query1.union_all(query2)
    elif chosen_operation == "except_":
        print("try except_")
        return query1.except_(query2)
    elif chosen_operation == "except_all":
        print("try except_all")
        return query1.except_all(query2)
    else:
        raise ValueError('a strange operation without sqlalchemy operation')


# This class is for each table


class TableSpec(object):
    def __init__(self, name):
        self.table_name = name
        self.columns = []
        self.row_data = []
        self.pk_idx = None
        self.fk_idx = -1
        self.num_tuples = -1

    def add_column(self, column_name, column_type):
        self.columns.append((column_name, column_type))


# This statistics class is built for each table
class TableStat(object):
    # maintain statistics for each table

    def __init__(self, tablename):
        self.tablename = tablename
        self.columns = []
        self.column_name = []
        self.column_type = []

        # min, max, average
        self.columns_stat = []
        self.table_size = 0

        # sqlalchemy table
        # self.sqlalchemy_tbl = None

    """
    def add_sqlalchemy_tbl(self, tbl):
        self.sqlalchemy_tbl = tbl
    """

    def add_column(self, column_name, column_type):
        self.column_name.append(column_name)
        self.column_type.append(column_type)
        self.columns.append([])

    # get row-wise data and transform to column-wise data
    def add_data(self, data):
        for x in range(len(data)):
            self.columns[x].append(data[x])
            self.table_size += 1

    # ret stat data by columnname
    def ret_stat(self, columnname):
        for x in range(len(self.column_name)):
            if self.column_name[x] == columnname:
                return self.columns_stat[x]
            else:
                AssertionError("No matching column name, my mistake")

    # ret string data by columnname
    def ret_string(self, columnname):
        for x in range(len(self.column_name)):
            if self.column_name[x] == columnname:
                return self.columns[x]
            else:
                AssertionError("No matching column name, my mistake")

    @staticmethod
    def ret_table_with_tblname(sqlalchemy_tbllist, tblname):
        for idx in range(len(sqlalchemy_tbllist)):
            name = sqlalchemy_tbllist[idx].name
            # print(name)
            if tblname == name:
                return sqlalchemy_tbllist[idx]
        return None

    @staticmethod
    def ret_tablestat_with_tblname(tbl_stat_list, tblname):
        for idx in range(len(tbl_stat_list)):
            name = tbl_stat_list[idx].tablename
            if tblname == name:
                return tbl_stat_list[idx]
        return None

    # when insertion is done, we calculate the stat

    def calculate_stat(self):

        # debug
        # print(self.columns)

        for x in range(len(self.columns)):

            # 1) if string/text ==> store length
            if self.column_type[x] == "String":
                temp_arr = []
                for y in range(len(self.columns[x])):
                    temp_arr.append(len(self.columns[x][y]))

                _min, _max, _avg = self.stat_from_arr(temp_arr)

            # 2) if DateTime
            elif self.column_type[x] == "DateTime":
                temp_arr = []
                for y in range(len(self.columns[x])):
                    # print("sampled datatime", y)
                    temp_arr.append(int(self.columns[x][y].strftime("%Y%m%d %H:%M:%S")))
                    # temp_arr.append(int(self.columns[x][y]))


                _min, _max, _avg = self.stat_from_arr(temp_arr)

            # 3) if numetic
            else:
                _min, _max, _avg = self.stat_from_arr(self.columns[x])

            self.columns_stat.append([_min, _max, _avg])

    def calculate_stat_existing_db(self, column_data, x):
        # call once for each column, different from previous method calculate_stat and populate data
        # debug
        # print(self.columns)
        # print("column data is ",column_data, x, self.column_type[x], type(column_data[0]))
        # 1) if string/text ==> store length
        print("column_type[x]:", self.column_type[x])
        if self.column_type[x] == "String":
            temp_arr = []
            # print("printing column data")
            print(column_data)
            for y in range(len(column_data)):
                if column_data[y]:
                    temp_arr.append(len(column_data[y]))
                else:
                    temp_arr.append(0)

            _min, _max, _avg = self.stat_from_arr(temp_arr)
        elif isinstance((column_data[0]), str):
            # get stat for a char(1) column
            # print("Char(1)")
            temp_arr = []
            for y in range(len(column_data)):
                temp_arr.append(len(column_data[y]))
            _min, _max, _avg = self.stat_from_arr(temp_arr)

        # 2) if DateTime
        elif isinstance((column_data[0]), datetime.date):
            temp_arr = []
            for y in range(len(column_data)):
                temp_arr.append(int(column_data[y].strftime("%Y%m%d")))
                # print("sampled datatime", column_data[y])
                # temp_arr.append(int(column_data[y]))

            _min, _max, _avg = self.stat_from_arr(temp_arr)
        elif self.column_type[x] == "Integer" or self.column_type[x] == "Float" or self.column_type[x] == "SmallInt":
            _min, _max, _avg = self.stat_from_arr(column_data)
        # 3) if numetic
        else:
            print('column data:', column_data)
            _min, _max, _avg = 0,0,0

        self.columns_stat.append([_min, _max, _avg])
        self.columns[x].extend(column_data)
        # print("finish run update for this column")

    def stat_from_arr(self, array):
        _min = min(array)
        _max = max(array)
        _avg = mean(array)
        return _min, _max, _avg





def load_existing_dbschema(config_data):
    # return 4 datafield in createsequences class
    tables = []  # tables spec (Tableclass), name,
    tables_stat = []  # tables_stat # tables statistics (TableStat class)
    db_name = config_data["name"]
    table_names = (config_data["tables"])
    conn_str = "postgresql://{}:postgres@localhost:{}/{}".format( \
        conf.USERS['postgres'], conf.TARGETS['postgres'], db_name)
    postgres_engine = create_engine(conn_str)
    schemameta = MetaData(postgres_engine)
    DBSession = sessionmaker(bind=postgres_engine)
    session = DBSession()
    alc_tables = []
    for table_name in table_names:
        messages = Table(table_name,
                         schemameta,
                         autoload=True,
                         autoload_with=postgres_engine)
        alc_tables.append(messages)
        
        table_stat = TableStat(table_name)
        table_class = TableSpec(table_name)
        
        table_class.pk_idx = -1
        table_class.fk_idx = -1
        results = session.query(messages)
        print("messages")
        print(messages)
        print("results")
        print(results)
        sample_results = (results[:5]) # could be a chance to randomize
        print("sample results")
        print(sample_results)
        for res in sample_results:
            print(res)
        column_index = 0
        for c in messages.columns:
            column_data = [i[column_index] for i in sample_results]
            # print("column type is", c.type, type(column_data[0]), type(c.type))
            # if isinstance(c.type, Integer):
            #     print("yeah", c.type)
            # column_type=''

            # need to use sqlalchemy type instead of real database's type
            table_class.add_column(c.name, (c.type))
            typename = ret_typename_from_class(c.type)
            table_stat.add_column(c.name, typename)
            # some type may not use for intersection calculation
            column_index += 1

        tables.append(table_class)
        tables_stat.append(table_stat)
        for c in range(len(messages.columns)):
            column_data = [i[c] for i in sample_results]
            # print(sample_results[messages.columns.index(c)])
            tables_stat[(
                tables_stat).index(table_stat)].calculate_stat_existing_db(
                    column_data, c)
        # update stat for each table
        table_stat.table_size = len(results.all())
        # print("table_size", table_stat.table_size)
        # print("****************************")

    # the tpch does not have any pk or fk
    # print(len(alc_tables))

    return tables, tables_stat, alc_tables, alc_tables


# TODO: apply Table class when generate spec


class CreateSequences(object):
    """ Create queries for Create Table, Update, Insert, and Select """
    def __init__(self,
                 max_table=1,
                 max_column=3,
                 max_tuple=5,
                 db_name="sqlalchemy"):
        # initial data
        self.metadata = MetaData()
        self.max_column = max_column  # max number of columns per table
        self.max_table = max_table
        self.max_tuple = max_tuple

        # output also required for existing database
        self.tables = []  # tables spec (TableSpec class)
        self.tables_stat = []  # tables statistics (TableStat class)
        self.sqlalchemy_tables = []  # sqlalchemy tables
        self.alc_tables = []  # sqlalchemy tables
        # output required for starting from scratch
        self.create_insert = ''  # store "create table", "insert", "index"
        self.update = ''
        self.delete = ''

        # directory
        self.TMP_DIR = "%s" % (FUZZ_MAIN)
        self.TMP_QUERY_PN = os.path.join(FUZZ_MAIN, "sqlsmith_query")
        self.TMP_ERR_PN = os.path.join(FUZZ_MAIN, "sqlsmith_err")

        # sqlite engine
        # self.sqlite_engine = create_engine('sqlite:///:memory:', echo=False)
        rmdirs(FUZZ_MAIN)
        try:
            os.remove("%s" % (DB_FILE))
        except Exception:
            pass
        mkdirs(FUZZ_MAIN)

        self.sqlite_engine = create_engine('sqlite:///%s' % (DB_FILE),
                                           echo=False)
        self.mysql_engine = create_engine(
            'mysql://mysql:mysql@localhost/sqlalchemy')
        self.postgres_engine = create_engine('postgresql:///' + db_name)

    def update_from_existing_db(self, tables, tables_stat, sqlalchemy_tables,
                                alc_tables):
        self.tables = tables
        self.tables_stat = tables_stat
        self.sqlalchemy_tables = sqlalchemy_tables
        self.alc_tables = alc_tables
        print("Finish loading schema info for ", len(self.sqlalchemy_tables),
              " tables")

    def create_tables(self):
        """
        1) need to decide the number of columns for each table
        2) for each table, decide which column is primary key
        3) - for each table (not all), decide which column is foreign key
           - also decide which column (w/ primary key) is referenced
        """
        """
        TODO:
        1) CREATE TABLE t0(c0 INT UNIQUE COLLATE NOCASE);
        2) Add two or more primary keys
        3) support "Nullable" option:
         - e.g Column('user_id', Integer, ForeignKey("user.id"),
           nullable=False)
         - we also should support use null data
        """

        # Spec: num_column, types, PK, FK(optional)
        #  - as a start, second to last tables always have FK
        table_spec = []
        tables = []

        for x in range(self.max_table):
            # 1) num_columns
            # 2) decide PK
            num_column = randoms.random_int_range(
                self.max_column) + 1  # at least two
            pk_column = randoms.random_int_range(num_column) - 1  # index
            fk_column = -1

            # 3) FK
            # if this is second or later table (e.g., 3rd table)
            if x > 0:
                fk_column = self.ret_fk(num_column, pk_column)
            table_spec.append((num_column, pk_column, fk_column))

        # 4) generate table creation queries
        prev_columns = None
        prev_table_name = ''
        for x in range(self.max_table):
            table_name = "TABLE%d" % x

            # generate new table
            temp_table = Table(table_name, self.metadata)
            columns = self.ret_columns(table_spec, x)

            # generate table-stat
            table_stat = TableStat(table_name)

            # create class for spec
            table_class = TableSpec(table_name)
            table_class.pk_idx = table_spec[x][1]
            table_class.fk_idx = table_spec[x][2]

            for y in range(len(columns)):
                column_name, column_type = columns[y]
                # print (column_name, column_type)

                # if not PK: then check FK and no-key
                if table_spec[x][1] != y:
                    # TODO: add nullable option
                    # e.g., Column('email_address', String (30),
                    #       nullable=False)

                    # if FK
                    if table_spec[x][2] == y:
                        # FK_name = prev_PK_name
                        FK_name = prev_columns[table_spec[x - 1][1]][0]
                        prev_table_pk_idx = self.tables[-1].pk_idx

                        # get type
                        column_type = self.tables[-1].\
                            columns[prev_table_pk_idx][1]
                        cur_column = Column(
                            column_name, column_type,
                            ForeignKey(prev_table_name + "." + FK_name))
                    # if not FK
                    else:
                        cur_column = Column(column_name, column_type)

                # if PK: then add primary condition to column
                else:
                    # prev_PK_name = column_name
                    cur_column = Column(column_name,
                                        column_type,
                                        primary_key=True)

                # add column to table and table-stat
                temp_table.append_column(cur_column)
                table_class.add_column(column_name, column_type)
                typename = self.ret_typename_from_class(column_type)
                table_stat.add_column(column_name, typename)

            # store table data
            prev_columns = columns
            prev_table_name = table_name
            tables.append(temp_table)
            # table_stat.add_sqlalchemy_tbl(temp_table)

            # 4-1) store created table's spec
            self.tables.append(table_class)
            self.tables_stat.append(table_stat)

        # 5) dump table into SQL
        for table in tables:
            table.create(self.sqlite_engine, checkfirst=True)
            ct_data = CreateTable(table).compile(self.sqlite_engine,
                                                 dialect=postgresql.dialect())
            print(ct_data)
            # print out created table to the console

            self.create_insert += str(ct_data).strip() + ";\n\n"
            self.alc_tables.append(table)

        self.sqlalchemy_tables = tables

    def choose_join_tables(self):
        """
        finds proper tables and column for making join

        input: none
        return: tbl1, tbl2, tbl1_col, tbl2_col
        """

        tbl_index = randoms.random_int_range(len(self.tables) - 1)
        tbl1 = self.alc_tables[tbl_index - 1]
        tbl2 = self.alc_tables[tbl_index]

        tbl1_referenced_idx = self.tables[tbl_index - 1].pk_idx
        tbl2_foreignkey_idx = self.tables[tbl_index].fk_idx
        cname1 = self.tables_stat[tbl_index - 1].\
            column_name[tbl1_referenced_idx]
        cname2 = self.tables_stat[tbl_index].column_name[tbl2_foreignkey_idx]

        # print (tbl_index-1)
        # print(cname1)
        # print(cname2)
        col1 = getattr(self.alc_tables[tbl_index - 1].c, cname1)
        col2 = getattr(self.alc_tables[tbl_index].c, cname2)

        return tbl1, tbl2, col1, col2, tbl_index - 1

    def ret_types_from_table(self, tbl):
        tbl_idx = self.alc_tables.index(tbl)
        tbl_types = self.tables_stat[tbl_idx].column_type
        return tbl_types

    def choose_same_type_columns(self, tbl1, tbl2):
        """
        given sqlalchemy_table, we will return two columns with same type

        input:
        return: tbl1_sametype_col, tbl2_sametype_col, typename
        """
        # 1) enumerate types for each table
        tbl1_types = self.ret_types_from_table(tbl1)
        tbl2_types = self.ret_types_from_table(tbl2)

        # 2) find common types
        common = (set(tbl1_types).intersection(tbl2_types))

        if len(common) < 1:
            return None, None, None, None

        # 3) randomly choose table
        chosen_type = random.choice(list(common))
        col1 = self.choose_columns_sqlalchemy_type(tbl1, chosen_type)
        col2 = self.choose_columns_sqlalchemy_type(tbl2, chosen_type)
        # print("!")

        return col1, col2, chosen_type

    def insert_tuples(self):
        """ Insert data from first to last table
          1) read tables spec:
            - table_name, column_name, column_type, constraints
          2) generate input
        """
        """ test
        print (self.tables[0])
        print (self.tables[0].columns)
        print (self.tables[0].pk_idx)
        print (self.tables[0].fk_idx)
        """

        # table iterator
        for x in range(len(self.tables)):
            # tuple iterator
            current_alc_table = self.alc_tables[x]

            prev_primary_idx = 0

            # tuple insert
            num_of_tuples = randoms.random_int_range(self.max_tuple)
            for y in range(num_of_tuples):
                # colume iterator
                row_data = []
                insert_dict = {}

                # TODO: consider primary key (do not allow unique values)
                for z in range(len(self.tables[x].columns)):

                    # read type name (VARCHAR special case, it is not a class)
                    if type(self.tables[x].columns[z][1]) == String:
                        typename = "String"
                    else:
                        typename = self.tables[x].columns[z][1].__name__
                    gendata = self.get_rand_data(typename)

                    # generate data
                    if x < 1 or z != self.tables[x].fk_idx:
                        row_data.append(gendata)
                    elif randoms.prob(conf.PROB_TABLE["PROB_SHARED_DATA"]) \
                            and len(self.tables[x - 1].row_data) >\
                            prev_primary_idx - 2:
                        row_data.append(gendata)

                    else:
                        if y - 1 < self.tables[x - 1].num_tuples and \
                                prev_primary_idx < self.tables[x - 1].\
                                num_tuples:
                            prev_pk_idx = self.tables[x - 1].pk_idx
                            row_data.append(self.tables[
                                x - 1].row_data[prev_primary_idx][prev_pk_idx])
                            prev_primary_idx = prev_primary_idx + 1

                        else:
                            row_data.append(gendata)

                for z in range(len(row_data)):
                    column_name = self.tables[x].columns[z][0]
                    # {column_name:row_data}
                    insert_dict[column_name] = row_data[z]

                # update data to tables (sqlalchemy) and table statistics
                self.tables[x].row_data.append(row_data)
                self.tables_stat[x].add_data(row_data)

                # insert rowdata to statistics, then it will convert
                # column-wise

                # SQLalchemy query to string
                query = current_alc_table.insert().values(insert_dict)
                conn_sqlite = self.sqlite_engine.connect()
                # conn_postgres = self.postgres_engine.connect()
                # conn_mysql = self.mysql_engine.connect()

                conn_sqlite.execute(query)
                query.bind = self.sqlite_engine

                self.create_insert += literalquery(query) + ";\n\n"
                # self.create_insert += str(in_data).strip() + ";\n\n"

            self.tables[x].num_tuples = num_of_tuples

            # update stat explicitly
            self.tables_stat[x].calculate_stat()

    def ret_fk(self, num_column, pk_column):

        count = 0
        while True:
            count += 1
            assert count < 100

            temp_idx = randoms.random_int_range(num_column) - 1
            if temp_idx != pk_column:
                return temp_idx

    def ret_rand_type(self):
        """ return type for column generation"""
        return COLUMN_TYPES[randoms.random_int_range(NUM_COLUMN_TYPES) - 1]

    def ret_columns(self, table_spec, cur_idx):
        """ return columns (name, type, option)
          - name
          - type: int, string, ...
          - option: foreign / primary key
        """

        num_column, _, fk_column = table_spec[cur_idx]
        # print (num_column, pk_column, fk_column)

        column_spec = []  # return data: [('name', 'type, 'option'), ...]

        for x in range(num_column):
            column_name = "%s" % (randoms.random_strings(6))
            if x != fk_column:
                column_type = self.ret_rand_type()
            else:
                # foreign key column
                column_type = None

            column_spec.append((column_name, column_type))

        return column_spec

    def run_sqlite_query(self, query):
        with open(self.TMP_QUERY_PN, 'w') as f:
            f.write(query)

        cmd = "sqlite3 %s < %s" % (DB_FILE, self.TMP_QUERY_PN)
        output = subprocess.getoutput(cmd)

        return output

    def _gen_sqlsmith_queries(self, query_num, timeout):
        """ generate sqlsmith queries on postgres DB using OLDEST version """

        dsn = "file:%s?mode=ro" % DB_FILE

        cmd = "timeout %ds ./sqlsmith --verbose  --exclude-catalog \
            --dump-all-queries \
            --seed=%d --max-queries=%d --sqlite=\"%s\" \
            1> %s 2> %s"                                                                         % \
            (timeout, randoms.random_int_range(1000000), query_num, dsn,
                self.TMP_QUERY_PN, self.TMP_ERR_PN)

        subprocess.getoutput(cmd)

    def extract_valid_query(self):
        query_result = []
        extract_queries = []

        with open(self.TMP_ERR_PN, 'r') as f:
            data = f.read()
            results = ""
            if "Generating" in data and "quer" in data:
                results = data.split("Generating indexes...done.")[1].split(
                    "queries:")[0]
                results = results.replace("\n", "").strip()

            for x in range(len(results)):
                if results[x] == "e":
                    query_result.append("fail")
                elif results[x] == ".":
                    query_result.append("success")
                elif results[x] == "S":
                    query_result.append("syntax error")
                elif results[x] == "C":
                    query_result.append("crash server!!!")
                    os.system("cat %s >> %s/crashed" % self.TMP_QUERY_PN,
                              FUZZ_MAIN)
                elif results[x] == "t":
                    query_result.append("timeout")
                else:
                    raise Exception('Not possible!')

        with open(self.TMP_QUERY_PN, 'r') as f:
            data = f.read()
            results = data.split(";")[:-1]

            for x in range(len(results)):
                try:
                    if query_result[x] == "success":
                        extract_queries.append(results[x] + ";")

                except Exception:
                    pass

        return extract_queries

    def gen_sqlsmith_queries(self):
        self._gen_sqlsmith_queries(150, 10)  # generate query and store text
        queries = self.extract_valid_query()

        for query in queries:
            print(self.run_sqlite_query(query))

    def DBMS_specific_keyword_addition(self):
        """ insert DBMS specific keyword which should not affect to result
            e.g, venign pragma, vacuum
        """
        """
        PRAGMA reverse_unordered_selects=true;
        PRAGMA journal_mode=OFF;
        PRAGMA main.cache_size=0;
        """
        pass

    def drop_tables(self):
        """ randomly drop some table """
        pass

    def create_index(self):
        """
        e.g.,
        CREATE INDEX index_0 ON test(c1 COLLATE NOCASE);
        CREATE INDEX index_0 ON test(c0 LIKE '');
        CREATE UNIQUE INDEX index_1 ON test(c0 GLOB c1);

        CREATE UNIQUE INDEX IF NOT EXISTS index_0 ON test(c1 == FALSE);
        CREATE INDEX IF NOT EXISTS index_1 ON test(c0 || FALSE) WHERE c1;
        PRAGMA legacy_file_format=true;
        REINDEX; -- Error: UNIQUE constraint failed: index 'index_0'

        # create non existing index
        CREATE TABLE t0(c1, c2);
        INSERT INTO t0(c1, c2) VALUES  ('a', 1);
        CREATE INDEX i0 ON t0("C3");
        ALTER TABLE t0 RENAME COLUMN c1 TO c3;
        SELECT DISTINCT * FROM t0; -- fetches C3|1 rather than a|1
        """

        # should store the result to self.create_insert

        pass

    def select_with_typecast(self):
        """
        INSERT INTO t0(c0) VALUES (1);
        PRAGMA reverse_unordered_selects=true;
        SELECT * FROM t0 WHERE ((t0.c0 > 'a') OR (t0.c0 <= 'a'));
        -- fetches no row
        """
        pass

    def get_rand_data(self, typename):
        if typename == 'String':
            gendata = randoms.ret_randomdata_by_type(typename,
                                                     constraint=MAX_STRING)

        if typename == 'Integer':
            gendata = randoms.ret_randomdata_by_type(
                typename,
                min=BOUNDARY["Integer"][0],
                max=BOUNDARY["Integer"][1])

        elif typename == 'DateTime':
            gendata = randoms.ret_randomdata_by_type(typename,
                                                     min=DATE_START,
                                                     max=DATE_END)

        elif typename == 'Float':
            gendata = randoms.ret_randomdata_by_type(typename,
                                                     min=BOUNDARY["Float"][0],
                                                     max=BOUNDARY["Float"][1])

        return gendata

    @staticmethod
    def remove_idx_by_name(sqlalchemy_tbl, idx_name, engine):
        """
        remove index from the target table using index_name (String)
        """

        idx_list = list(sqlalchemy_tbl.indexes)

        out = []
        for idx in idx_list:
            if idx.name != idx_name:
                out.append(idx)
            else:
                idx.drop(engine)

        # TODO: we are not sure about the safety here
        sqlalchemy_tbl.indexes = set(out)

    @staticmethod
    def ret_column_by_name(sqlalchemy_tbl, name):
        for col in sqlalchemy_tbl._columns:
            # print(col.name)
            if col.name == name:
                return col
        assert "Column not exist in the table"

    @staticmethod
    def choose_columns_sqlalchemy(table, column_names, option):
        # table: sqlalchemy object
        # column_names: string

        if option == "one":
            num_cols = 1
            chosen_columns = random.choices(
                column_names, k=randoms.random_int_range(num_cols))

        elif option == "all":
            chosen_columns = column_names

        elif option == "wo_idx":
            # TODO: fix here (like with_idx)
            defined_indexes = set(map(lambda x: x.name, list(table.indexes)))
            chosen_columns = list(set(column_names) - defined_indexes)

        elif option == "with_idx":
            # print(str(list(list(table.indexes)[0].columns)[0]).split(".")[1])
            chosen_columns = list(
                map(lambda x: str(list(x.columns)[0]).split(".")[1],
                    list(table.indexes)))

        else:
            num_cols = len(column_names)
            chosen_columns = random.choices(
                column_names, k=randoms.random_int_range(num_cols))

        out = []
        for item in chosen_columns:
            # print(table.c)
            # print(item)
            selected_col = getattr(table.c, item)
            out.append(selected_col)

        return out

    def choose_columns_sqlalchemy_type(self, tbl, _type):
        # tableinfo: should know the name of each column

        tbl_idx = self.alc_tables.index(tbl)
        tbl_names = self.tables_stat[tbl_idx].column_name
        tbl_types = self.tables_stat[tbl_idx].column_type

        sametype_cols = []
        for x in range(len(tbl_types)):
            if tbl_types[x] == _type:
                sametype_cols.append(tbl_names[x])

        chosen_col_name = random.choice(sametype_cols)
        selected_col = getattr(tbl.c, chosen_col_name)

        return selected_col

    def choose_columns(self, table, mutable=False):
        """
        return randomly chosen column index of table
        - mutable:False ==> disregard pk/fk columns
        - mutable:True  ==> consider all columns
        """
        return_candidate = list(range(len(table.columns)))

        pk_idx = table.pk_idx
        fk_idx = table.fk_idx

        if mutable is True:
            return_candidate.remove(pk_idx)
            if fk_idx in return_candidate:
                return_candidate.remove(fk_idx)

        random.shuffle(return_candidate)
        # print ("before ret", return_candidate)

        if len(return_candidate) < 1:
            return []
        # This condition is for where
        # TODO: use more than two where conditions
        elif mutable is False:
            return [return_candidate[0]]
        else:
            return_num = randoms.random_int_range(len(return_candidate)) - 1
            return_list = return_candidate[0:return_num]

        return return_list

    def ret_set_str(self, set_dict):

        set_list = []

        for column_name in set_dict.keys():
            newdata, typename = set_dict[column_name]
            # print(newdata, typename)
            if typename in NUMERIC:
                set_list.append("\"%s\" = %s" % (column_name, newdata))
            else:
                set_list.append("\"%s\" = \"%s\"" % (column_name, newdata))

        return ',\n'.join(map(str, set_list))

    def ret_typename_from_class(self, typeclass):
        if "VARCHAR" in str(typeclass):
            typename = "String"
        else:
            typename = typeclass.__name__
        return typename

    @staticmethod
    def ret_limit_num(tblstat):
        # return limit, offset from the given TableStat class
        offset = randoms.random_int_range(tblstat.table_size - 1)
        limit = randoms.random_int_range(tblstat.table_size - offset)
        return limit, offset

    def select_tuples(self,
                      template_seed=0,
                      idx=None,
                      cur_sqlalchemy_table=None,
                      column_names=None):
        """
        * should maintain statistics when insert
        """
        CONJ = ["and", "or"]
        # sharing code for all templates:
        if (idx is None and cur_sqlalchemy_table is None
                and column_names is None):
            idx = random.choice(range(len(self.sqlalchemy_tables)))
            cur_sqlalchemy_table = self.sqlalchemy_tables[idx]
            column_names = self.tables_stat[idx].column_name
        ###############################
        # [*] NORMAL SELECT: one table
        ###############################
        if (template_seed == 0):
            print("*normal select one table")

            select_columns = CreateSequences.choose_columns_sqlalchemy(
                cur_sqlalchemy_table, column_names, option="random")

            stmt = select(select_columns)
            print(literalquery(stmt) + ";", file=sys.stderr)
            return stmt
        ###############################
        # [*] NORMAL SELECT: limit and offset
        ###############################
        elif (template_seed == 1):
            print("*normal select limit and offset")

            select_columns = CreateSequences.choose_columns_sqlalchemy(
                cur_sqlalchemy_table, column_names, option="random")

            limit, offset = CreateSequences.ret_limit_num(
                self.tables_stat[idx])
            stmt = select(select_columns).limit(limit).offset(offset)
            print(literalquery(stmt) + ";", file=sys.stderr)
            return stmt
        ###############################
        # [*] NORMAL SELECT: group by
        ###############################
        elif (template_seed == 2):
            print("*normal select group by")

            select_columns = CreateSequences.choose_columns_sqlalchemy(
                cur_sqlalchemy_table, column_names, option="random")
            group_by_columns = random.choices(select_columns,
                                              k=randoms.random_int_range(
                                                  len(select_columns)))

            stmt = select(select_columns)
            for column in group_by_columns:
                stmt = stmt.group_by(column)
            print(literalquery(stmt) + ";", file=sys.stderr)
            return stmt
        ###############################
        # [*] NORMAL SELECT: having
        ###############################
        elif (template_seed == 3):
            print("*normal select having")

            select_columns = CreateSequences.choose_columns_sqlalchemy(
                cur_sqlalchemy_table, column_names, option="random")
            group_by_columns = random.choices(select_columns,
                                              k=randoms.random_int_range(
                                                  len(select_columns)))

            # group_by first (before having)
            stmt = select(select_columns)
            for column in group_by_columns:
                stmt = stmt.group_by(column)

            # then, apply having (similar with where)
            having_col = random.choice(group_by_columns)
            # print("?")
            having_col_stat = self.tables_stat[idx].ret_stat(having_col.name)
            # print("?", having_col_stat)

            having_col_data = self.tables_stat[idx].ret_string(having_col.name)
            # print("?", having_col_data)

            having_col_cond = where_generator(having_col, None,
                                              having_col_stat, None,
                                              having_col_data)
            # print("?")

            stmt = stmt.having(having_col_cond)
            print(literalquery(stmt) + ";", file=sys.stderr)
            return stmt
        ###############################
        # [*] NORMAL SELECT + ONE WHERE CONDITION (e.g., where (A))
        ###############################
        elif (template_seed == 4):

            print("*NORMAL SELECT + ONE WHERE CONDITION")

            select_columns = CreateSequences.choose_columns_sqlalchemy(
                cur_sqlalchemy_table, column_names, option="random")
            column1 = CreateSequences.choose_columns_sqlalchemy(
                cur_sqlalchemy_table, column_names, option="one")[0]
            tablename1, columnname1 = str(column1).split(".")
            column1_stat = self.tables_stat[idx].ret_stat(columnname1)
            column1_data = self.tables_stat[idx].ret_string(columnname1)

            column1_where = where_generator(column1, None, column1_stat, None,
                                            column1_data)
            column2_where = where_generator(column1, None, column1_stat, None,
                                            column1_data)

            stmt_where1 = select(select_columns).where(column1_where)
            # stmt_where2 = select(select_columns).where(column2_where)
            # stmt_union = stmt_where1.union(stmt_where2)

            if True:
                print(literalquery(stmt_where1) + ";", file=sys.stderr)
                # print(literalquery(stmt_where2)+";",file=sys.stderr)
                # print(literalquery(stmt_union)+";",file=sys.stderr)
            return stmt_where1
        ###############################
        # [*] NORMAL SELECT + TWO WHERE CONDITIONS (e.g., where(A and B))
        ###############################
        elif (template_seed == 5):

            print("*NORMAL SELECT + two WHERE CONDITION")

            select_columns = CreateSequences.choose_columns_sqlalchemy(
                cur_sqlalchemy_table, column_names, option="random")
            column1 = CreateSequences.choose_columns_sqlalchemy(
                cur_sqlalchemy_table, column_names, option="one")[0]
            tablename1, columnname1 = str(column1).split(".")
            column1_stat = self.tables_stat[idx].ret_stat(columnname1)
            column1_data = self.tables_stat[idx].ret_string(columnname1)

            column1_where = where_generator(column1, None, column1_stat, None,
                                            column1_data)
            column2 = CreateSequences.choose_columns_sqlalchemy(
                cur_sqlalchemy_table, column_names, option="one")[0]
            tablename2, columnname2 = str(column2).split(".")
            column2_stat = self.tables_stat[idx].ret_stat(columnname2)
            column2_data = self.tables_stat[idx].ret_string(columnname2)
            column2_where = where_generator(column2, None, column2_stat, None,
                                            column2_data)

            # combine and / or
            if (column1_where is not None and column2_where is not None):
                combined_where = combine_condition(column1_where,
                                                   column2_where,
                                                   random.choice(CONJ))
                stmt_where2 = select(select_columns).where(combined_where)
                if True:
                    print(literalquery(stmt_where2) + ";", file=sys.stderr)
                return stmt_where2
        ###############################
        # [*] NORMAL SELECT + TWO MORE WHERE NESTED CONDITIONS
        # (e.g., where ((A and B) and C))
        ###############################
        elif (template_seed == 6):
            print("*NORMAL SELECT + TWO MORE WHERE NESTED CONDITIONS")

            select_columns = CreateSequences.choose_columns_sqlalchemy(
                cur_sqlalchemy_table, column_names, option="random")
            column1 = CreateSequences.choose_columns_sqlalchemy(
                cur_sqlalchemy_table, column_names, option="one")[0]
            tablename1, columnname1 = str(column1).split(".")
            column1_stat = self.tables_stat[idx].ret_stat(columnname1)
            column1_data = self.tables_stat[idx].ret_string(columnname1)

            column1_where = where_generator(column1, None, column1_stat, None,
                                            column1_data)
            column2 = CreateSequences.choose_columns_sqlalchemy(
                cur_sqlalchemy_table, column_names, option="one")[0]
            tablename2, columnname2 = str(column2).split(".")
            column2_stat = self.tables_stat[idx].ret_stat(columnname2)
            column2_data = self.tables_stat[idx].ret_string(columnname2)
            column2_where = where_generator(column2, None, column2_stat, None,
                                            column2_data)

            column3 = CreateSequences.choose_columns_sqlalchemy(
                cur_sqlalchemy_table, column_names, option="one")[0]
            tablename3, columnname3 = str(column3).split(".")
            column3_stat = self.tables_stat[idx].ret_stat(columnname3)
            column3_data = self.tables_stat[idx].ret_string(columnname3)
            column3_where = where_generator(column3, None, column3_stat, None,
                                            column3_data)
            if (column1_where is not None and column2_where is not None
                    and column3_where is not None):

                combined_where1 = combine_condition(column1_where,
                                                    column2_where,
                                                    random.choice(CONJ))
                combined_where2 = combine_parenthesis(combined_where1,
                                                      column3_where,
                                                      random.choice(CONJ))
                stmt_where3 = select(select_columns).where(combined_where2)
                if True:
                    print(literalquery(stmt_where3) + ";", file=sys.stderr)
                return stmt_where3

        ###############################
        # [*] NORMAL SELECT + LIMIT + HAVING + GROUPBY
        ###############################

        # small_s = select([student.c.studentid, student.c.name,
        # func.avg(marks.c.total_marks-5)]).limit(0)

        # s = select([student.c.studentid, student.c.name,
        #     func.avg(marks.c.total_marks-5)])\
        #     .where( and_(student.c.studentid == marks.c.studentid, \
        #         marks.c.total_marks >=\
        #     select([marks.c.total_marks]).where(marks.c.studentid == 'V003')))\
        #     .order_by(asc(student.c.studentid))\
        #     .limit(4)\
        #     .offset(0)\
        #     .group_by(student.c.name, student.c.studentid)\
        #     .having(func.avg(marks.c.total_marks) > 80)\
        #     .distinct()

        # stmt_limit = select(select_columns).where(combined_where2)
        # if False:
        #     print(literalquery(stmt_limit))

        ###############################
        # [*] JOIN
        ###############################

        # """
        # j = join(Student, StudentCourse, Student.c.roll_no
        #   == StudentCourse.c.roll_no)
        # s = select([StudentCourse.c.course_id, Student.c.name,
        #   Student.c.age]).select_from(j)
        # """

        elif (template_seed == 7):
            print("*JOIN")
            # idx = random.choice(range(len(self.sqlalchemy_tables)))
            # cur_sqlalchemy_table = self.sqlalchemy_tables[idx]

            tbl1, tbl2, tbl1_col, tbl2_col, tbl1_idx = self.choose_join_tables(
            )
            column_names = self.tables_stat[tbl1_idx].column_name
            select_columns = CreateSequences.choose_columns_sqlalchemy(
                tbl1, column_names, option="random")

            j = join(tbl1, tbl2, tbl1_col == tbl2_col)
            stmt_join1 = select(select_columns).select_from(j)
            if True:
                print(literalquery(stmt_join1) + ";", file=sys.stderr)
            return stmt_join1
        ###############################
        # [*] SUBQUERY#1: using two tables
        ###############################

        # """
        # s = select([student.c.studentid, student.c.name, marks.c.total_marks])\
        #     .where( and_(student.c.studentid == marks.c.studentid, \
        #     marks.c.total_marks > select( [func.avg(marks.c.total_marks)])\
        #    .where(marks.c.total_marks > 80)) )
        # """
        elif (template_seed == 8):

            print(" * SUBQUERY#1: using two tables")

            # select target tables for subquery
            tbl1, tbl2, tbl1_col, tbl2_col, tbl1_idx = self.choose_join_tables(
            )
            select_columns = CreateSequences.choose_columns_sqlalchemy(
                tbl1, column_names, option="random")
            tbl1_sametype_col, tbl2_sametype_col, typename = \
                self.choose_same_type_columns(tbl1, tbl2)
            # we have to use type of each column
            # 1) select two columns with same types
            stmt_sub1 = select(select_columns)\
                .where(and_(tbl1_col == tbl2_col,
                            tbl1_sametype_col > select([tbl2_sametype_col])))
            print(literalquery(stmt_sub1) + ";", file=sys.stderr)
            return stmt_sub1
        # 2) select two columns regardless of types and CAST

    def update_tuples_sqlalchemy(self):
        # update_query = ''

        for x in range(len(self.sqlalchemy_tables)):
            cur_table = self.tables[x]
            cur_sqlalchemy_table = self.sqlalchemy_tables[x]
            selected_columns = self.choose_columns(cur_table,
                                                   mutable=True)  # array
            set_candidate = {}

            for column_idx in selected_columns:
                """
                if "VARCHAR" in str(self.tables[x].columns[column_idx][1]):
                    typename = "String"
                else:
                    typename = self.tables[x].columns[column_idx][1].__name__
                """
                typename = self.ret_typename_from_class(
                    str(self.tables[x].columns[column_idx][1]))
                column_name = self.tables[x].columns[column_idx][0]

                newdata = self.get_rand_data(typename)
                set_candidate[column_name] = (newdata, typename)

            set_str = self.ret_set_str(set_candidate)
            if set_str == "":
                continue

            # print(set_candidate)
            # where_column_idx = selected_columns[0]
            where_operator = COMPARISONS[randoms.random_int_range(
                len(COMPARISONS) - 1)]
            if typename in NUMERIC:
                where_str = "\"%s\" %s %s" % (column_name, where_operator,
                                              newdata)
            else:
                where_str = "\"%s\" %s \"%s\"" % (column_name, where_operator,
                                                  newdata)

            # TODO: debugging here
            # print(where_str)
            # print(set_str)
            # print(dir(cur_table))
            stmt = cur_sqlalchemy_table.update().where(where_str).\
                value(set_str)
            if False:
                print(literalquery(stmt))

    def update_tuples(self):
        """ update template
        UPDATE table
        SET column_1 = new_value_1,
            column_2 = new_value_2
        WHERE
            search_condition
        ORDER column_or_expression
        LIMIT row_count OFFSET offset;
        """
        """
        - pick 1~2 columns (which is not pk, fk)
        - where uses data generation
        - random operators
        """
        """
        TODO:
        UPDATE OR REPLACE
        """

        update_query = ''
        for x in range(len(self.tables)):
            if randoms.prob(conf.PROB_TABLE["PROB_UPDATE"]):
                # print("Update %s table" % self.tables[x].table_name)

                # 1) Update table name
                update_query = UPDATE.replace("{table}",
                                              self.tables[x].table_name)
                selected_columns = self.choose_columns(self.tables[x],
                                                       mutable=True)  # array

                # 2) Set
                set_candidate = {}
                for column_idx in selected_columns:

                    if "VARCHAR" in str(self.tables[x].columns[column_idx][1]):
                        typename = "String"
                    else:
                        typename = self.tables[x].\
                            columns[column_idx][1].__name__
                    column_name = self.tables[x].columns[column_idx][0]

                    newdata = self.get_rand_data(typename)
                    set_candidate[column_name] = (newdata, typename)

                set_str = self.ret_set_str(set_candidate)
                if set_str == "":
                    continue
                update_query = update_query.replace("{set}", set_str)

                # 3) Where
                #  - first, we apply only one where condition
                # TODO: multiple where condition
                # TODO: add constant
                # TODO: add cast
                where_column_idx = selected_columns[0]
                where_operator = COMPARISONS[randoms.random_int_range(
                    len(COMPARISONS) - 1)]

                if "VARCHAR" in \
                        str(self.tables[x].columns[where_column_idx][1]):
                    typename = "String"
                else:
                    typename = self.tables[x].\
                        columns[where_column_idx][1].__name__
                column_name = self.tables[x].columns[where_column_idx][0]
                newdata = self.get_rand_data(typename)

                if typename in NUMERIC:
                    where_str = "\"%s\" %s %s" % (column_name, where_operator,
                                                  newdata)
                else:
                    where_str = "\"%s\" %s \"%s\"" % (column_name,
                                                      where_operator, newdata)
                update_query = update_query.replace("{where}", where_str)
                # 4) Limit
                if randoms.prob(conf.PROB_TABLE["PROB_UPDATE_LIMIT"]):
                    update_query += "\nLIMIT %d" % (
                        randoms.random_int_range(3))

                update_query += ";"

            else:
                # print("Don't update %s table" % self.tables[x].table_name)
                pass

        if "{set}" not in update_query:
            self.update = update_query

    def delete_tuples(self):
        """ template
        DELETE FROM table
        WHERE search_condition
        ORDER BY criteria,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
        LIMIT row_count OFFSET offset;
        """

        delete_query = ''

        for x in range(len(self.tables)):
            if randoms.prob(conf.PROB_TABLE["PROB_DELETE"]):

                # 1) Delete table name
                delete_query = DELETE.replace("{table}",
                                              self.tables[x].table_name)
                # table_name = self.tables[x].table_name
                selected_columns = self.choose_columns(self.tables[x],
                                                       mutable=True)  # array

                if len(selected_columns) == 0:
                    continue

                # 2) Where
                where_column_idx = selected_columns[0]
                where_operator = COMPARISONS[randoms.random_int_range(
                    len(COMPARISONS) - 1)]

                # data = self.tables[x].row_data[0][where_column_idx]

                if "VARCHAR" in \
                        str(self.tables[x].columns[where_column_idx][1]):
                    typename = "String"
                else:
                    typename = self.tables[x].\
                        columns[where_column_idx][1].__name__
                column_name = self.tables[x].columns[where_column_idx][0]
                newdata = self.get_rand_data(typename)

                if typename in NUMERIC:
                    where_str = "\"%s\" %s %s" % (column_name, where_operator,
                                                  newdata)
                else:
                    where_str = "\"%s\" %s \"%s\"" % (column_name,
                                                      where_operator, newdata)
                delete_query = delete_query.replace("{where}", where_str)

                # 3) Limit
                if randoms.prob(conf.PROB_TABLE["PROB_UPDATE_LIMIT"]):
                    delete_query += "\nLIMIT %d" % (
                        randoms.random_int_range(3))
                delete_query += ";"

        if "{where}" not in delete_query:
            self.delete = delete_query

    def mutation(self, component):
        if component == "select":
            sm = SelectMutation(self.tables_stat, self.tables,
                                self.sqlalchemy_tables)
            sm.select_mutation()

        elif component == "index":
            im = IndexMutation(self.tables_stat, self.tables,
                               self.sqlalchemy_tables, self.sqlite_engine)
            im.index_mutation()

        elif component == "where":
            wm = WhereMutation(self.tables_stat, self.tables,
                               self.sqlalchemy_tables, self.sqlite_engine)
            wm.where_mutation()


def stmt_complex(stmt, available_columns):
    # given a statement, randomly add stuff in the tail
    # available_columns: column object from the sa_table
    # group
    # print("ac", available_columns)
    print("examine project columns", type(stmt.c))
    column_list = stmt.c
    for i in column_list:
        print(type(i), i.type)
    if (random_int_range(1000) < conf.PROB_TABLE["group"]):
        chosen_groupby_columns = random.choices(
            available_columns,
            k=randoms.random_int_range(len(available_columns)))
        for column in chosen_groupby_columns:
            stmt = stmt.group_by(column)
    # distinct entire select
    if (random_int_range(1000) < conf.PROB_TABLE["distinct"]):
        stmt = stmt.distinct()
    # order
    if (random_int_range(1000) < conf.PROB_TABLE["order"]):
        chosen_orderby_columns = random.choices(available_columns, k=1)
        for column in chosen_orderby_columns:
            if (ret_typename_from_class(column.type) in ["Float", "Integer"]):
                stmt = stmt.order_by(asc(column))
    # limit
    if (random_int_range(1000) < conf.PROB_TABLE["limit"]):
        stmt = stmt.limit(random_int_range_contain_zero(20))

    if (random_int_range(1000) < conf.PROB_TABLE["offset"]):
        stmt = stmt.offset(random_int_range_contain_zero(20))
    return stmt


def rule_expandaggregatedistinct(spec, query_count):
    # this generation mechanism have already been integrated into the generator.
    select_number_of_columns = 2
    select_columns, where_clause, table_idx, selectable_columns = spec.gen_select_statement(
        select_number_of_columns)
    stmt = select(select_columns).where(where_clause)
    print(literalquery(stmt) + ";", file=sys.stderr)
    query_count += 1
    return query_count


def rule_aggregatecaserule(spec, query_count):
    # this generation mechanism have already been integrated into the generator.
    print("begin fire aggregate case rule")
    select_number_of_columns = 2
    select_columns, where_clause, table_idx, selectable_columns = spec.gen_select_statement(
        select_number_of_columns)
    stmt = select(select_columns).where(where_clause)
    print(literalquery(stmt) + ";", file=sys.stderr)
    query_count += 1
    return query_count


def rule_aggregatevaluesrule(spec, query_count):
    # this generation mechanism have already been integrated into the generator.
    print("begin fire aggregate values rule")
    select_number_of_columns = 3
    select_columns, where_clause, table_idx, selectable_columns = spec.gen_select_statement(
        select_number_of_columns)
    stmt = select(select_columns).where(where_clause)
    print(literalquery(stmt) + ";", file=sys.stderr)
    query_count += 1
    return query_count


def reproduce_bug1(spec, query_count):
    print("begin reproduce bug about reoptimize expression tree")
    # select_number_of_columns = random_int_range(3)
    select_columns, where_clause, table_idx, selectable_columns = spec.gen_select_statement(
    )
    stmt = select(select_columns).where(where_clause)
    selectable_columns = []
    sqlalchemy_tables = spec.scope.alc_tables
    table_a = spec.scope.alc_tables[table_idx]
    for item in table_a._columns:
        selectable_columns.append(item)
    stmt = stmt_complex(stmt, selectable_columns)
    print(literalquery(stmt) + ";", file=sys.stderr)
    query_count += 1
    # stmt = select(select_columns)
    # print(type(select_columns[0].type))
    if (ret_typename_from_class(select_columns[0].type) == "String"):
        select_columns_, where_clause_, table_idx_, _ = spec.gen_select_statement(
            1)
        enclosing_stmt = select(select_columns_).where(
            not_(stmt.as_scalar().is_distinct_from(conf.SCALAR_STR)))
        query_count += 1
        print(literalquery(enclosing_stmt) + ";", file=sys.stderr)

    return query_count

def top_generation(spec, query_count):
    # select_columns, where_clause, table_idx, selectable_columns = spec.gen_select_statement(
    # )
    # if(spec.joined is True):
    #     spec.getjoined()
    #     stmt = select(select_columns).select_from.where(where_clause)
    #     stmt = stmt_complex(stmt)

    # else:
    #     stmt = select(select_columns).where(where_clause)
    # if conf.PROB_TABLE["scalar"]:
    #     # choose from an existing stmt
    # if conf.PROB_TABLE["set"]:
    #     # get a previous subquery that has the same number of
    #     # for loop call
    #     return
    # print(final_stmt)
    return
def set_query_generation(spec, stmt, select_expr):
    temp_query = stmt
    success_flag = False
    for i in range(3):
        # try to create a super query that involve 10 set operations
        print("len", len(select_expr))
        try:
            select_columns, where_clause, table_idx, selectable_columns, joined_from, base_table = spec.gen_select_statement(select_column_number=len(select_expr), force_simple_from = True)
        except Exception as inst:
            # this might cause exception because joined_from does not have that many selectable columns
            print("exception in using set operations", inst)
            traceback.print_exc(file=sys.stdout)
            continue
        if (base_table is False):
            # we only use select from base table to construct set queries
            continue
        print("find set compatible column")
        # for j in range(len(select_columns_)):
        #     c=select_columns_[j].label("subc" + str(j))
        #     select_columns_[j] = c
        # rearrange the column to match type
        reordered_select_columns_ = []
        for c_ in select_expr:
            for c in select_columns:
                if ret_typename_from_class(c.type) == ret_typename_from_class(
                        c_.type):
                    reordered_select_columns_.append(c)
                    break
        reordered_select_columns_ = list(
            dict.fromkeys(reordered_select_columns_))

        if (len(reordered_select_columns_)) != len(select_columns):
            print("two query not set compatible")
            continue
        stmt_ = select(reordered_select_columns_).where(where_clause)
        stmt_ = stmt_complex(stmt_, reordered_select_columns_)
        another_query = stmt_
        try:
            query_union = run_set_operation(temp_query,
                                            another_query).alias(name="dt")
            selectable_columns = get_selectable_column(query_union)
            outside_where = spec.gen_where_clause(None, None, selectable_columns)
            print("set where", outside_where)
            temp_query = select(random.sample(selectable_columns, random.randint(1, len(selectable_columns)))).select_from(query_union).where(outside_where)
            temp_query = stmt_complex(temp_query, selectable_columns)

            success_flag = True
        except Exception as inst:
            print("exception in using set operations", inst)
            # print("q1:", literalquery(temp_query))
            # print("q2:", literalquery(another_query))
    if success_flag is True:
        return temp_query
def reproduce_bug2(spec, query_count):
    print("begin reproduce bug about rewritting union to union all")
    select_columns, where_clause, table_idx, selectable_columns = spec.gen_select_statement(
    )
    stmt = select(select_columns).where(where_clause)
    # stmt = stmt_complex(stmt, selectable_columns)
    #     # trigger set operation generation
    temp_query = stmt
    success_flag = False
    for i in range(3):
        # try to create a super query that involve 10 set operations
        select_columns_, where_clause_, table_idx_, selectable_columns_ = spec.gen_select_statement(
            select_column_number=len(select_columns))
        # for j in range(len(select_columns_)):
        #     c=select_columns_[j].label("subc" + str(j))
        #     select_columns_[j] = c
        # rearrange the column to match type
        reordered_select_columns_ = []
        for c in select_columns:
            for c_ in select_columns_:
                if ret_typename_from_class(c.type) == ret_typename_from_class(
                        c_.type):
                    reordered_select_columns_.append(c_)
                    break
        reordered_select_columns_ = list(
            dict.fromkeys(reordered_select_columns_))

        if (len(reordered_select_columns_)) != len(select_columns):
            break
        print(len(reordered_select_columns_), len(select_columns))
        stmt_ = select(reordered_select_columns_).where(where_clause_)
        stmt_ = stmt_complex(stmt_, reordered_select_columns_)
        another_query = stmt_
        try:
            random_suffix = random.randint(100,200)
            query_union = run_set_operation(temp_query,
                                            another_query).alias('d'+str(random_suffix))
            # investigate how to output a subset column of a nested selectable
            # all_column_names = self.spec_stat[table_idx].column_name
            # selectable_column = []
            # for item in all_column_names:
            #     selected_col = getattr(self.scope.alc_tables[table_idx].c, item)
            #     selectable_column.append(selected_col)
            # print("unioned query", literalquery(query_union))
            # selected_col = getattr(query_union.c, "subc0")
            # print("selected col is", selected_col)
            # temp_query = select([selected_col])
            temp_query = select([query_union])
            success_flag = True
        except Exception as inst:
            print("exception in using set operations", inst)
            # print("q1:", literalquery(temp_query))
            # print("q2:", literalquery(another_query))
    if (success_flag is True):
        query_count += 1
        print(literalquery(temp_query) + ";", file=sys.stderr)
    return query_count


def reproduce_bug4(spec):
    select_columns, where_clause, table_idx, selectable_columns, joined_from, base_table = spec.gen_select_statement(
    )
    if joined_from is not None:
        stmt = select(select_columns).select_from(joined_from).where(where_clause)
    else:
        stmt = select(select_columns).where(where_clause)
    stmt = stmt_complex(stmt, selectable_columns)
    num_sub = len(Scope.table_ref_stmt_list)
    if (num_sub < 5 and len(select_columns) == 1 and base_table):
        # only store subquery that has one column
        print("add subquery", stmt)
        Scope.table_ref_stmt_list.append(stmt)
        stmt_sub = stmt.apply_labels().alias('d'+str(num_sub))
        num_sub += 1
        Scope.table_ref_list.append(stmt_sub)
    elif (len(select_columns) == 1 and base_table):
        # if the subquery list is full, reset the list
        print("reset")
        Scope.table_ref_stmt_list = []
        Scope.table_ref_stmt_list.append(stmt)
        Scope.table_ref_list = []
        stmt_sub = stmt.apply_labels().alias('d'+str(0))
        Scope.table_ref_list.append(stmt_sub)
    if (random_int_range(1000) < conf.PROB_TABLE["set"] and len(select_columns) < 4):
        # set operation
        print("try generate set query")
        set_query = set_query_generation(spec, stmt, select_columns)
        if set_query is not None:
            print("set success")
            return set_query
        else:
            print("set failed")

    # generate three new statements
    # TODO: scalar subquery
    # pick an existing subq


    return stmt

def reproduce_bug3(spec, query_count):
    # this has merged into gen_select_statement code
    select_columns, where_clause, table_idx, selectable_columns = spec.gen_select_statement(
    )
    # if where_clause is not None:
    #     # first_ = select_columns[0]
    #     # print(type(first_))
    #     # a = first_.alias(name="demo")
    #     # select_columns[0] = a
    #     stmt = select(select_columns).where(
    #         where_clause)
    choice = random_int_range_contain_zero(1)
    sqlalchemy_tables = spec.scope.alc_tables.copy()
    table_a = spec.scope.alc_tables[table_idx]
    selectable_columns = []
    for item in table_a._columns:
        selectable_columns.append(item)
    if (choice == 1):
        try:
            # for table in sqlalchemy_tables[1:]:
            if (len(table_a.foreign_keys)):
                print("try joined using foreign key relationship")
                # print("number of referenced table is", table_a.foreign_keys)
                random.shuffle(sqlalchemy_tables)
                for fkey in table_a.foreign_keys:
                    for table_b in sqlalchemy_tables:
                        if (((fkey).references(table_b))):
                            referenced_table = fkey.target_fullname.split(
                                ".")[0]
                            if (random_int_range(1000) <
                                    conf.PROB_TABLE["inner"]):
                                j = table_a.join(table_b)
                            elif (random_int_range(1000) <
                                  conf.PROB_TABLE["outer"]):
                                j = table_a.outerjoin(table_b, full=True)
                            # left outer join
                            elif (random_int_range(1000) <
                                  conf.PROB_TABLE["left"]):
                                j = table_a.join(table_b, isouter=True)
                            # right outer join
                            else:
                                j = table_b.join(table_a, isouter=True)
                            for item in table_b._columns:
                                # print(type(item))
                                #     selected_col = getattr(table_b, item)
                                selectable_columns.append(item)
                            random_columns = random.choices(
                                selectable_columns,
                                k=min(
                                    4,
                                    randoms.random_int_range(
                                        len(selectable_columns))))
                            stmt = select(random_columns).select_from(j)
                            stmt = stmt_complex(stmt, selectable_columns)
                            print(literalquery(stmt) + ";", file=sys.stderr)
                            query_count += 1
                            return query_count
        except Exception as inst:
            print("exception in join operations", inst, sys.exc_info())
    # return query_count
    try:
        random_idx = random_int_range_contain_zero(len(select_columns) - 1)
        random_column = select_columns[random_idx]
        random.shuffle(sqlalchemy_tables)
        for table_b in sqlalchemy_tables:
            if table_b != table_a:
                for c in table_b.columns:
                    # print(c.type)
                    # if (c.type is (random_column.type)):
                    if (isinstance(c.type, type(random_column.type))):
                        if (random_int_range(1000) < conf.PROB_TABLE["true"]):
                            if (random_int_range(1000) <
                                    conf.PROB_TABLE["inner"]):
                                j = table_a.join(table_b, true())
                            elif (random_int_range(1000) <
                                  conf.PROB_TABLE["outer"]):
                                j = table_a.outerjoin(table_b,
                                                      true(),
                                                      full=True)
                            # left outer join
                            elif (random_int_range(1000) <
                                  conf.PROB_TABLE["left"]):
                                j = table_a.join(table_b, true(), isouter=True)
                            # right outer join
                            else:
                                j = table_b.join(table_a, true(), isouter=True)
                        else:
                            if (random_int_range(1000) <
                                    conf.PROB_TABLE["inner"]):
                                j = table_a.join(table_b, false())
                            elif (random_int_range(1000) <
                                  conf.PROB_TABLE["outer"]):
                                j = table_a.outerjoin(table_b,
                                                      false(),
                                                      full=True)
                            # left outer join
                            elif (random_int_range(1000) <
                                  conf.PROB_TABLE["left"]):
                                j = table_a.join(table_b,
                                                 false(),
                                                 isouter=True)
                            # right outer join
                            else:
                                j = table_b.join(table_a,
                                                 false(),
                                                 isouter=True)
                        for item in table_b._columns:
                            selectable_columns.append(item)
                        random_columns = random.choices(
                            selectable_columns,
                            k=min(
                                4,
                                randoms.random_int_range(
                                    len(selectable_columns))))
                        stmt = select(random_columns).select_from(j)
                        stmt = stmt_complex(stmt, selectable_columns)
                        query_count += 1
                        print(literalquery(stmt) + ";", file=sys.stderr)
                        return query_count
    except Exception as inst:
        print("exception in join operations", inst, sys.exc_info())
    return query_count


def main():
    signal.signal(signal.SIGINT,
                  exit_gracefully(signal.getsignal(signal.SIGINT)))

    # DEFINE PARSER (strategy)
    main_parser = argparse.ArgumentParser()
    main_parser.add_argument("-s",
                             "--strategy",
                             dest="strategy",
                             type=str,
                             default=None,
                             help="Mutation strategy",
                             required=True)
    main_parser.add_argument('--db_info',
                             type=str,
                             nargs='?',
                             default=".",
                             help='database name',
                             required=True)
    main_parser.add_argument('--output',
                             type=str,
                             nargs='?',
                             default='demo.sql',
                             help="generate queries file")
    main_parser.add_argument('--queries',
                             type=int,
                             nargs='?',
                             default=1,
                             help='number of queries generated',
                             required=True)
    main_parser.add_argument('--dialect ')
    main_parser.add_argument(
        '--prob_table',
        type=str,
        nargs='?',
        default='.',
        help='prob table for controlling the query generation',
                             required=True)

    main_parser.set_defaults(action='mutation')
    args = main_parser.parse_args()

    load_pbtable(args.prob_table)
        
    if args.db_info != ".":
        config_data = {}
        try:
            with open(args.db_info) as f:
                config_data = json.load(f)
                tables, tables_stat, sqlalchemy_tables, alc_tables = load_existing_dbschema(
                    config_data)
                cs = CreateSequences(max_table=config_data["max_table"],
                                     max_column=config_data["max_column"],
                                     db_name=config_data["name"],
                                     max_tuple=15000000)
                cs.update_from_existing_db(tables, tables_stat,
                                           sqlalchemy_tables, alc_tables)
                query_count = 0
                while (query_count < args.queries):
                    # ************ Begin doing SQLSMITH STUFF ***********
                    # ************ reproduce bug1 ***********
                    scope = Scope()
                    scope.add_alc(sqlalchemy_tables)
                    # global spec
                    spec = Query_Spec("demo", tables, tables_stat, scope)
                    stmt = reproduce_bug4(spec)
                    try:
                        stmt_string = literalquery(stmt)
                        print(stmt_string.replace("ON 1", "ON TRUE") + ";", file=sys.stderr)
                        query_count += 1
                        print("success print literalquery")
                    except:
                        traceback.print_stack()
                        traceback.print_exception()
                        print("error in printing out query")

                print("Total number of queries generated is", query_count)

        except Exception as inst:
            print('print_exc():')
            traceback.print_exc(file=sys.stdout)
            print(type(inst), inst.args, inst)
            exit(1)
            
            
    if args.strategy == 'sequence':

        drop_db = "drop database sqlalchemy"
        create_db = "create database sqlalchemy"

        run_query_pg(drop_db)
        run_query_pg(create_db)
        run_query_my(drop_db)
        run_query_my(create_db)

        cs = CreateSequences(max_table=randoms.random_int_range(5) + 1,
                             max_column=5,
                             max_tuple=10)
        cs.create_tables()
        print("finish creating database schema")
        cs.insert_tuples()
        # cs.select_tuples()
        # cs.update_tuples_sqlalchemy()
        # cs.delete_tuples()
        # cs.gen_sqlsmith_queries()

        # cs.mutation("index")
        # print("Finish creating index")
        # cs.mutation("select")
        # cs.mutation("where")
    else:
        pass


if __name__ == "__main__":
    main()



    # for c in table_b.columns:
    #     # print(c.type)
    #     # if (c.type is (random_column.type)):
    #     # j = table_a.join(table_b)
    #     # subquery 1
    #     j = table_a.outerjoin(table_b, true(), full=False)
    #     stmt = select(selectable_columns_a +
    #                   selectable_columns_b).select_from(j).alias('d1')
    #     # subquery 2
    #     j_ = table_a.outerjoin(table_b, true(), full=True)
    #     stmt_ = select(selectable_columns_a +
    #                   selectable_columns_b).select_from(j).alias('d2')

    #     j_ = stmt.outerjoin(stmt_, true())
    #     random_columns_from_stmt = random.sample(
    #         get_selectable_column(stmt) + get_selectable_column(stmt_),
    #         random_int_range(len(get_selectable_column(stmt))))

    #     stmt_ = select(random_columns_from_stmt).select_from(j_).limit(
    #         10)
    #     # print(literalquery(stmt_))



# def choose_columns_sqlalchemy(table, column_names, option):
#     # table: sqlalchemy object
#     # column_names: string

#     if option == "one":
#         num_cols = 1
#         chosen_columns = random.choices(
#             column_names, k=randoms.random_int_range(num_cols))

#     elif option == "all":
#         chosen_columns = column_names

#     elif option == "wo_idx":
#         # TODO: fix here (like with_idx)
#         defined_indexes = set(map(lambda x: x.name, list(table.indexes)))
#         chosen_columns = list(set(column_names) - defined_indexes)

#     elif option == "with_idx":
#         # print(str(list(list(table.indexes)[0].columns)[0]).split(".")[1])
#         chosen_columns = list(
#             map(lambda x: str(list(x.columns)[0]).
#                 split(".")[1], list(table.indexes)))

#     else:
#         num_cols = len(column_names)
#         chosen_columns = random.choices(
#             column_names, k=randoms.random_int_range(num_cols))

#     out = []
#     for item in chosen_columns:
#         # print(table.c)
#         # print(item)
#         selected_col = getattr(table.c, item)
#         out.append(selected_col)

#     return out
