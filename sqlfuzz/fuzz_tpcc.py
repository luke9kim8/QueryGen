#!/usr/bin/env python2

# python library and classes
import os
import sys
import re
import yaml
import time
import signal
import commands
import argparse
import sqlite3 as sqlite

# from tqdm import tqdm
from random import randint
from datetime import datetime

# own classes and global variables
from conf import *

sys.path.append("transform")
import trans_sqlite2mysql as s2m
import trans_sqlite2postgres as s2p


""" USAGE
- fuzzer
$ python fuzz_tpcc.py  fuzz -o /tmp/fuzz -r yes

- checker
$ python fuzz_tpcc.py checker -i checker_test/null.sql -c null
"""


class MyDumper(yaml.Dumper):
    def increase_indent(self, flow=False, indentless=False):
        return super(MyDumper, self).increase_indent(flow, False)


def mkdirs(pn):
    try:
        os.makedirs(pn)

    except OSError:
        pass


def does_query_modify_table(query):
    keywords = ['insert', 'update', 'alter', "delete"]
    for keyword in keywords:
        if keyword in query.lower():
            return True
    return False


def select_time_wrap(query, db):
    if db == "my":
        time_query = "select date_format(NOW(3),'%Y-%m-%d %H:%i:%s.%f') \
            as date_val"
        return "%s;%s;%s;" % (time_query, query, time_query)
    elif db == "sq":
        time_query = " select strftime('%Y-%m-%d %H:%M:%f', 'NOW');"
        return "%s;%s;%s;" % (time_query, query, time_query)
    elif db == "pg":
        return "select now();%s;select now();\n" % query
    else:
        raise NotImplemented


def print_result(fuzz_result):
    out_pg, error_pg, out_cr, error_cr, out_my, error_my, out_sq, error_sq \
        = fuzz_result
    print ("pg:", out_pg, error_pg)
    print ("my:", out_my, error_my)
    print ("sq:", out_sq, error_sq)
    print ("cr:", out_cr, error_cr)


def join(*args):
    return os.path.abspath(os.path.join(*args))


def exit_gracefully(original_sigint):
    def _exit_gracefully(signum, frame):
        signal.signal(signal.SIGINT, original_sigint)
        try:
            if raw_input("\nReally quit? (y/n)> ").lower().startswith('y'):
                sys.exit(1)
        except KeyboardInterrupt:
            print("Ok ok, quitting")
            sys.exit(1)
        signal.signal(signal.SIGINT, _exit_gracefully)
    return _exit_gracefully


def run_cmd(cmd):
    return commands.getoutput(cmd)


def to_seconds(d):
    d_in_ms = int(float(d) * 1000)
    return d_in_ms


def calctime(header, footer):
    htime = header.split("+")[0]
    htime = htime.replace("-04", "")

    ftime = footer.split("+")[0]
    ftime = ftime.replace("-04", "")

    if htime.split("-")[0] == "20":
        htime = "20" + htime
    if ftime.split("-")[0] == "20":
        ftime = "20" + ftime

    hdtime = float(datetime.strptime(
        htime, "%Y-%m-%d %H:%M:%S.%f").strftime('%s.%f'))
    fdtime = float(datetime.strptime(
        ftime, "%Y-%m-%d %H:%M:%S.%f").strftime('%s.%f'))

    elapsed = fdtime - hdtime

    if elapsed > 0:
        return elapsed
    else:
        return 0.001


def new_result_after_process(fuzz_result, outset):
    out_pg2, out_my2, out_sq2, out_cr2 = outset
    out_pg, error_pg, out_cr, error_cr, out_my, error_my, out_sq, error_sq \
        = fuzz_result

    new_result = (
        out_pg2, error_pg, out_cr2, error_cr, out_my2, error_my,
        out_sq2, error_sq)
    return new_result


def fix_err_sq(query):
    out = query
    out = out.replace("TRUE", "1")
    out = out.replace("FALSE", "0")

    return out


def has_error_code(status_col, allowed_code):
    # status_col = set
    # allowed_code = list

    status_pg, status_cr, status_my, status_sq = status_col

    for item in status_col:
        if item not in allowed_code:
            return True

    # no crash
    return False


class CorrectnessFuzzer(object):
    def __init__(self, targetdb, cleanlog=True, output=None, resume=False):

        if output is not None:
            FUZZ_MAIN = output
        else:
            FUZZ_MAIN = DEFAULT_FUZZ_MAIN

        self.DBPATH = os.path.abspath("./benchmark/test.db")
        self.TMP_ERR_PN = os.path.join(FUZZ_MAIN, "sqlsmith_err")
        self.TMP_QUERY_PN = os.path.join(FUZZ_MAIN, "sqlsmith_query")
        self.TMP_CR_QUERY = os.path.join(FUZZ_MAIN, ".cr.sql")
        self.TMP_PG_QUERY = os.path.join(FUZZ_MAIN, ".pg.sql")
        self.TMP_MY_QUERY = os.path.join(FUZZ_MAIN, ".my.sql")
        self.TMP_SQ_QUERY = os.path.join(FUZZ_MAIN, ".sq.sql")

        self.CURR_QUERY = os.path.join(FUZZ_MAIN, "curr_query")
        self.ERR_COLLECTION = os.path.join(FUZZ_MAIN, "errors")
        self.BUG_COLLECTION = os.path.join(FUZZ_MAIN, "bugs")
        self.STATUS = os.path.join(FUZZ_MAIN, "status")
        self.BUG_COLLECTION_COR = os.path.join(
            self.BUG_COLLECTION, "correctness")
        self.BUG_COLLECTION_PRECISION = os.path.join(
            self.BUG_COLLECTION, "precision")
        self.BUG_COLLECTION_NULL = os.path.join(
            self.BUG_COLLECTION, "null")
        self.BUG_COLLECTION_CRASH = os.path.join(
            self.BUG_COLLECTION, "crash")
        self.BUG_COLLECTION_PERFORMANCE = os.path.join(
            self.BUG_COLLECTION, "performance")
        self.BUG_COLLECTION_EMPTY = os.path.join(
            self.BUG_COLLECTION, "empty")
        self.BUG_COLLECTION_DIFF = os.path.join(
            self.BUG_COLLECTION, "diff")

        self.resume = resume
        self.SQLSMITH = "./sqlsmith"
        self.TMP_ERR = "%s/stderr" % (FUZZ_MAIN)
        self.TMP_OUTPUT = os.path.join(FUZZ_MAIN, "tmpout")
        self.TIMEOUT = 10  # second
        self.TMP_DIR = "%s" % (FUZZ_MAIN)

        # we don't define cockroachDB transformer (just reuse PostgreSQL)
        self.trans_s2m = s2m.Transformer(in_pn=None)
        self.trans_s2p = s2p.Transformer(in_pn=None)

        self.total_queries = 0
        self.dsn = "file:%s?mode=ro" % self.DBPATH
        self.log = Logger(targetdb)

        if cleanlog:
            self.clean_log(self.log.logfile)

        self.conn = sqlite.connect(self.DBPATH, timeout=10)
        self.cur = self.conn.cursor()

        self.total_bug = 0
        self.total_cor_bug = 0
        self.error_cnt_cr = 0
        self.error_cnt_my = 0
        self.error_cnt_pg = 0

        self.elapsed_pg = 0.0
        self.elapsed_my = 0.0
        self.elapsed_sq = 0.0
        self.elapsed_cr = 0.0

        self.performance = ()
        self.performance_ratio = 0.0

        # clean up dirs
        # FIXME: use python util to remove directory (e.g., shutil)
        if self.resume is False:
            os.system("rm -rf %s" % FUZZ_MAIN)

            # make necessary dirs
            mkdirs(FUZZ_MAIN)
            mkdirs(self.TMP_DIR)
            mkdirs(self.ERR_COLLECTION)
            mkdirs(self.BUG_COLLECTION)
            mkdirs(self.BUG_COLLECTION_COR)
            mkdirs(self.BUG_COLLECTION_PRECISION)
            mkdirs(self.BUG_COLLECTION_NULL)
            mkdirs(self.BUG_COLLECTION_CRASH)
            mkdirs(self.BUG_COLLECTION_PERFORMANCE)
            mkdirs(self.BUG_COLLECTION_DIFF)
            mkdirs(self.BUG_COLLECTION_EMPTY)

            mkdirs(os.path.join(self.ERR_COLLECTION, "postgres"))
            mkdirs(os.path.join(self.ERR_COLLECTION, "mysql"))
            mkdirs(os.path.join(self.ERR_COLLECTION, "cockroach"))

        # fix: which is necessary?
        else:
            pass

    def save_status(self, count, subcount, crash_count, fp_count, total):

        OUT = {}
        OUT["count"] = count
        OUT["subcount"] = subcount
        OUT["crash_count"] = crash_count
        OUT["fp_count"] = fp_count
        OUT["total"] = total
        OUT["gtotal"] = self.total_bug

        with open(self.STATUS, "w") as f:
            dump_str = yaml.dump(
                OUT, Dumper=MyDumper, default_flow_style=False)
            f.write(dump_str)

    def load_status(self):
        with open(self.STATUS, "r") as f:
            OUT = yaml.safe_load(f)
        return OUT["count"], OUT["subcount"], OUT["crash_count"], \
            OUT["fp_count"], OUT["total"], OUT["gtotal"]

    def setup_dbms(self):
        self.drop_and_create_db()
        self.import_db()

    def drop_and_create_db(self):
        # mysql
        run_cmd("mysql -u mysql -pmysql -e 'drop database mysqldb;'")
        run_cmd("mysql -u mysql -pmysql -e 'create database mysqldb;'")

        # postgres
        run_cmd(
            "psql -p %d -c 'drop database postgresdb;'" % (PORT['postgres']))
        run_cmd(
            "psql -p %d -c 'create database postgresdb;'" % (PORT['postgres']))

        # cockroach
        run_cmd(
            "cockroach sql --insecure --host=localhost --port=%d --execute=\" \
            DROP DATABASE cockroachdb;\"" % (PORT['cockroach']))
        run_cmd(
            "cockroach sql --insecure --host=localhost --port=%d --execute=\" \
            CREATE DATABASE IF NOT EXISTS cockroachdb;\"" % (PORT['cockroach']))

    def import_db(self):
        # mysql
        run_cmd(
            "mysql -u mysql -pmysql mysqldb < transform/sql_ex/tpcc_mysql.sql")

        # postgres
        run_cmd(
            "psql -p %d -d postgresdb -f transform/sql_ex/tpcc_postgres.sql"
            % (PORT['postgres']))

        # cockroach
        # MEMO: should copy the tpcc.sql file in node's directory
        run_cmd(
            "cockroach19 sql --insecure --host=localhost:26299  \
            --database=cockroachdb --execute=\"IMPORT PGDUMP  \
            'nodelocal:///tpcc.sql';\"")

    def extract_valid_query(self):
        extract_queries = []

        with open(self.TMP_QUERY_PN, 'r') as f:
            data = f.read()
            results = data.split(";")[:-1]

            for x in xrange(len(results)):
                try:
                    query = results[x].replace('\n', '').replace('"', '')
                    extract_queries.append(query + ';')
                except:
                    pass

        return extract_queries

    def _run_cmd(self, cmd, ret_elapsed=False):
        """ run command and return execution time """
        if ret_elapsed:
            start = time.time()
            os.system(cmd)
            end = time.time()
            return end - start
        else:
            os.system(cmd)

    def remove_stdout(self):
        if os.path.exists(self.TMP_OUTPUT):
            os.system("rm -f %s" % self.TMP_OUTPUT)

    def run_one_cmd(self, cmd, no_stdout=True):
        """ run command for one server (desinated port) """

        if os.path.exists(self.TMP_ERR):
            os.remove(self.TMP_ERR)

        if no_stdout and "/dev/null" not in cmd:
            cmd += " > /dev/null 2>> %s" % self.TMP_ERR

        self._run_cmd(cmd, ret_elapsed=False)

    def clean_log(self, LOGFILE):
        if os.path.exists(LOGFILE):
            os.remove(LOGFILE)

    def _gen_queries(self, query_num, timeout):
        """ generate  queries on postgres DB using the amoeba fuzzer """       
        cmd = "timeout %ds python3.6 ./mutator.py --prob_table prob_table.json --db_info db_conf.json -s seq --queries %d 1>%s 2>%s "%(
            timeout, query_num, self.TMP_ERR_PN, self.TMP_QUERY_PN
        )
        
        print('running command', cmd)
        self.run_one_cmd(cmd, no_stdout=False)

    def run_and_sort(self, result, cmd):
        with open(self.TMP_OUTPUT, 'w') as f:
            f.write(result)
        out = commands.getoutput(cmd)
        return out

    """
    out, elapsed, status = self.retrieve_result_and_time(
        cmd, sortcmd, 1, -1, 2, -1)
    """

    def retrieve_result_and_time(
        self, cmd, sortcmd, header_idx, footer_idx, result_start, result_end):

        status, out = commands.getstatusoutput(cmd)
        lines = out.splitlines()
        lines = [x.replace(", ", ",") for x in lines]

        try:
            # different for each DB
            header = lines[header_idx]
            footer = lines[footer_idx]
            elapsed = calctime(header, footer)

        except ValueError:
            elapsed = 0.00111

        except IndexError:
            elapsed = 0.00112

        actual_result = '\n'.join(lines[result_start:result_end])
        out = self.run_and_sort(actual_result, sortcmd)

        return out, elapsed, status

    def run_query_cr(self, query):
        error = False
        out = None

        with open(self.TMP_CR_QUERY, 'w') as f:
            f.write(query)

        cmd = "timeout 5s cockroach sql --format csv --insecure  \
            --host=localhost --port=%d --database=cockroachdb < %s " \
            % (PORT['cockroach'], self.TMP_PG_QUERY)
        sortcmd = "cat %s | sort" % self.TMP_OUTPUT
        out, elapsed, status = self.retrieve_result_and_time(
            cmd, sortcmd, 1, -1, 3, -2)

        if "Error:" in out:
            error = True

        return out, error, elapsed, status

    def run_query_pg(self, query):
        error = False
        out = None

        with open(self.TMP_PG_QUERY, 'w') as f:
            f.write(query)

        cmd = "timeout 5s psql -p %d -t -F ',' --no-align -d postgresdb -f %s"\
            % (PORT['postgres'], self.TMP_PG_QUERY)
        
        sortcmd = "cat %s | sort" % self.TMP_OUTPUT
        out, elapsed, status = self.retrieve_result_and_time(
            cmd, sortcmd, 0, -1, 1, -1)
        if 'error' in out.lower():
            error = True
            print('QUERY')
            print(query)
            print('ERROR')
            print(out)
            print
            print('-----------------------------------------------------')
        return out, error, elapsed, status

    def run_query_my(self, query):
        error = False
        out = None

        with open(self.TMP_MY_QUERY, 'w') as f:
            f.write(query.lower())

        cmd = "timeout 5s mysql -N --skip-column-names -u mysql \
            -pmysql mysqldb < %s" % self.TMP_MY_QUERY
        sortcmd = "cat %s | sort|csvcut -t" % self.TMP_OUTPUT
        out, elapsed, status = self.retrieve_result_and_time(
            cmd, sortcmd, 1, -1, 2, -1)
        
        tempout = ""
        lines = out.split("\n")
        for line in lines:
            if "password on" in line:
                continue
            tempout += line + "\n"

        if "ERROR" in out and "at" in out:
            error = True

        return tempout.strip(), error, elapsed, status

    def run_query_sq(self, query):
        error = False
        out = None

        with open(self.TMP_SQ_QUERY, 'w') as f:
            f.write(query)

        cmd = "timeout 5s sqlite3 -separator ',' %s < %s" \
            % (self.DBPATH, self.TMP_SQ_QUERY)
        sortcmd = "cat %s | sort" % self.TMP_OUTPUT
        out, elapsed, status = self.retrieve_result_and_time(
            cmd, sortcmd, 0, -1, 1, -1)
        if "Error:" in out:
            error = True

        return out, error, elapsed, status

    def dump_and_run(self, query, db):
        out = None
        error = None

        exec_query = select_time_wrap(query, db="pg")

        if db == "postgres":
            out, error, elapsed, status = self.run_query_pg(exec_query)

        elif db == "cockroach":
            out, error, elapsed, status = self.run_query_cr(exec_query)

        elif db == "sqlite":
            exec_query = select_time_wrap(query, db="sq")
            out, error, elapsed, status = self.run_query_sq(exec_query)

        elif db == "mysql":
            exec_query = select_time_wrap(query, db="my")
            out, error, elapsed, status = self.run_query_my(exec_query)

        return out, error, elapsed, status

    def dump_err(self, db, query_ori, query_trans, output, error_cnt):
        out_pn = os.path.join(self.ERR_COLLECTION, db)

        with open("%s/%d.sqlite" % (out_pn, error_cnt), 'w') as f:
            f.write(query_ori)

        with open("%s/%d.%s" % (out_pn, error_cnt, db), 'w') as f:
            f.write(query_trans)

        with open("%s/%d.out" % (out_pn, error_cnt), 'w') as f:
            f.write(output)

    def compare_output(self, out_set, query):

        pg, my, sq, cr = out_set

        # TODO: Make it more intiutive
        # Avoid false positives - TIMESTAMP
        # sq and pg contain ms but my only shows seconds
        if re.search('\\d{2}:\\d{2}:\\d{2}', my, re.M):
            return NOT_BUG

        if pg == my and pg == sq and pg == cr:
            return NOT_BUG

        return CORRECTNESS_BUG

    def remove_limit(self, query):
        query = re.sub('limit\\s[0-9]+', '', query, flags=re.I)
        query = re.sub('offset\\s[0-9]+', '', query, flags=re.I)
        return query

    def save_bug(self, fuzz_result, query_col, bugclass, storeout=True):
        self.total_bug += 1

        if bugclass == "correctness":
            self.total_cor_bug += 1
            directory = self.BUG_COLLECTION
        elif bugclass == "limit":
            directory = self.BUG_COLLECTION_COR
        elif bugclass == "null":
            directory = self.BUG_COLLECTION_NULL
        elif bugclass == "crash":
            directory = self.BUG_COLLECTION_CRASH
        elif bugclass == "performance":
            directory = self.BUG_COLLECTION_PERFORMANCE
        elif bugclass == "diff":
            directory = self.BUG_COLLECTION_DIFF
        elif bugclass == "empty":
            directory = self.BUG_COLLECTION_EMPTY
        elif bugclass == "precision":
            directory = self.BUG_COLLECTION_PRECISION
        else:
            raise NotImplemented

        out_pg, error_pg, out_cr, error_cr, out_my, error_my, out_sq, error_sq\
            = fuzz_result
        query, query_sq, query_pg, query_my, query_cr = query_col

        maxsize = max(len(out_pg), len(out_cr), len(out_my), len(out_sq))

        if storeout or maxsize < 5000000:
            with open("%s/%d.pg_out" % (directory, self.total_bug), 'w') as f:
                f.write(out_pg)
            with open("%s/%d.my_out" % (directory, self.total_bug), 'w') as f:
                f.write(out_my)
            with open("%s/%d.sq_out" % (directory, self.total_bug), 'w') as f:
                f.write(out_sq)
            with open("%s/%d.cr_out" % (directory, self.total_bug), 'w') as f:
                f.write(out_cr)

        if bugclass == "performance":
            elapsed_pg, elapsed_cr, elapsed_my, elapsed_sq = self.performance
            with open("%s/%d.ratio" % (directory, self.total_bug), 'w') as f:
                rto = "  >> PG: %4.5f, CR:  %4.5f, MY: %4.5f, SQ: %4.5f, RATIO: %4.5f " % \
                    (elapsed_pg, elapsed_cr, elapsed_my, elapsed_sq,
                     self.performance_ratio)
                print(rto)
                f.write(rto)

        with open("%s/%d.pg_query" % (directory, self.total_bug), 'w') as f:
            f.write(query_pg)
        with open("%s/%d.my_query" % (directory, self.total_bug), 'w') as f:
            f.write(query_my)
        with open("%s/%d.sq_query" % (directory, self.total_bug), 'w') as f:
            f.write(query_sq)
        with open("%s/%d.cr_query" % (directory, self.total_bug), 'w') as f:
            f.write(query_cr)

    def run_fuzz_sqlite_inv_exec(self):
        self.log.debug("[*] start fuzzing", _print=True)

        if self.resume is False:
            count = 0
            subcount = 0
            crash_count = 0
            fp_count = 0
            total = 0
        else:
            count, subcount, crash_count, fp_count, total, self.total_bug \
                = self.load_status()

        while True:

            if count % 100 == 0:
                self.log.debug("[*] Round %d" % (count / 100), _print=True)
                self.log.debug(
                    " >> total query:%d, total bug:%d"
                    % (total, self.total_bug), _print=True)
                subcount = 0

            self.log.debug(" >> gen_queries", _print=False)
            elapsed = self._gen_queries(150, 1000000)
            if elapsed < 8:
                self.log.debug(
                    " >> gen_query success within timeout", _print=False)

            # 3.2) parse query/error files and extract only the valid queries
            self.log.debug(
                " >> Try to generate random queries...", _print=False)
            queries = self.extract_valid_query()
            self.log.debug(
                " * succeed generating %d queries (%d/100)"
                % (len(queries), subcount), _print=True)
            total += len(queries)

            # 3.3) run queries (only valid)
            self.current_true = 0
            self.total_queries += len(queries)

            # for query in tqdm(queries):
            for query in queries:

                query = query + "\n"
                query = self.mutation(query)

                # discard if query changes data
                if does_query_modify_table(query):
                    continue

                with open(self.CURR_QUERY, 'w') as f:
                    f.write(query)
                fuzz_result, query_col, elapsed, status_col \
                    = self.query_run(query)
                self.triage(fuzz_result, query_col, elapsed, status_col)

            count += 1
            subcount += 1

            self.save_status(count, subcount, crash_count, fp_count, total)

        print ("Total processed: %d" % total)
        print ("Postgres error: %d" % error_cnt_pg)
        print ("Mysql error: %d" % error_cnt_my)

    def run_fuzz_sqlite(self):
        self.log.debug("[*] start fuzzing", _print=True)

        count = 0
        subcount = 0
        total = 0
        # crash_count = 0
        # fp_count = 0

        while True:

            if count % 100 == 0:
                self.log.debug("[*] Round %d" % (count / 100), _print=True)
                self.log.debug(
                    " * total query:%d, total bug:%d"
                    % (total, self.total_bug), _print=True)
                subcount = 0

            self.log.debug(" >> gen_queries", _print=False)
            elapsed = self._gen_queries(150, 10000000)
            if elapsed < 8:
                self.log.debug(
                    " >> gen_query success within timeout", _print=False)

            # 3.2) parse query/error files and extract only the valid queries
            self.log.debug(
                " >> Try to generate random queries...", _print=False)
            queries = self.extract_valid_query()
            self.log.debug(
                " >> succeed generating %d queries (%d/100)"
                % (len(queries), subcount), _print=True)
            total += 1

            # 3.3) run queries (only valid)
            self.current_true = 0
            self.total_queries += 1

            # for query in tqdm(queries):
            query = ';\n'.join(queries)
            query = self.mutation(query)

            # discard if query changes data
            if does_query_modify_table(query):
                continue

            with open(self.CURR_QUERY, 'w') as f:
                f.write(query)

            fuzz_result, query_col, elapsed, status_col = self.query_run(query)
            self.triage(fuzz_result, query_col, elapsed, status_col)

            count += 1
            subcount += 1

        print ("Total processed: %d" % total)
        print ("Postgres error: %d" % error_cnt_pg)
        print ("Mysql error: %d" % error_cnt_my)

    def query_run(self, query):

        query_san = query.replace("main.", "")
        query_sq = fix_err_sq(query)
        query_pg = self.trans_s2p._transform(query_san)
        query_my = self.trans_s2m._transform(query_san)
        query_cr = query_pg

        out_pg, error_pg, elapsed_pg, status_pg = self.dump_and_run(
            query_pg, "postgres")
        
        out_cr, error_cr, elapsed_cr, status_cr = self.dump_and_run(
            query_pg, "cockroach")
        out_my, error_my, elapsed_my, status_my = self.dump_and_run(
            query_my, "mysql")
        out_sq, error_sq, elapsed_sq, status_sq = self.dump_and_run(
            query_sq, "sqlite")
        

        if "DEBUG" in os.environ:
            print(out_pg, out_cr, out_my, out_sq)
            print("error", error_pg, error_cr, error_my, error_pg)

        # sanitize csvcut message
        out_my = out_my.replace("StopIteration:", "")
        fuzz_result = (
            out_pg, error_pg, out_cr, error_cr, out_my,
            error_my, out_sq, error_sq)
        query_col = (query, query_sq, query_pg, query_my, query_cr)
        elapsed_result = (elapsed_pg, elapsed_cr, elapsed_my, elapsed_sq)
        status_col = (status_pg, status_cr, status_my, status_sq)

        if "DEBUG" in os.environ:
            print_result(fuzz_result)

        return fuzz_result, query_col, elapsed_result, status_col

    def mutation(self, query):
        # will add more mutation rules

        out = ""
        if randint(0, 100) > 90:
            out = query.replace("is NULL", "is not NULL")
        else:
            out = query
        return out

    def roundup_line(self, line):

        out = ""
        items = line.split(",")
        for item in items:
            if "." in item:
                item = item.split(".")[0]
            out += item + ","
        return out[:-1]

    def _process_line(self, data):
        out = ""
        lines = data.split("\n")

        for line in sorted(lines):
            out += self.roundup_line(line.strip()) + "\n"

        return out.strip()

    def process_line(self, fuzz_result):
        out_pg, error_pg, out_cr, error_cr, out_my, error_my, out_sq, error_sq = fuzz_result
        out_pg = self._process_line(out_pg)
        out_my = self._process_line(out_my)
        out_sq = self._process_line(out_sq)
        out_cr = self._process_line(out_cr)

        out_set = (out_pg, out_my, out_sq, out_cr)
        return out_set

    def store_syntax_err(self, fuzz_result, query_col, status_col):

        has_error = False

        out_pg, error_pg, out_cr, error_cr, out_my, error_my, out_sq, error_sq = fuzz_result
        query, query_sq, query_pg, query_my, query_cr = query_col
        status_pg, status_cr, status_my, status_sq = status_col

        if status_pg == 256:
            self.error_cnt_pg += 1
            self.dump_err(
                "postgres", query_sq, query_pg, out_pg, self.error_cnt_pg)
            has_error = True

        if status_my == 256:
            self.error_cnt_my += 1
            self.dump_err(
                "mysql", query_sq, query_my, out_my, self.error_cnt_my)
            has_error = True

        if status_cr == 256:
            self.error_cnt_cr += 1
            self.dump_err(
                "cockroach", query_sq, query_cr, out_cr, self.error_cnt_cr)
            has_error = True

        return has_error

    def check(self, input_pn, checker):

        with open(input_pn, 'r') as f:
            query = f.read()

        if checker == "performance":
            raise NotImplemented

        elif checker == "null":
            fuzz_result, query_col, elapsed, _ = self.query_run(query)

        elif checker == "precision":
            raise NotImplemented

        elif checker == "limit":
            raise NotImplemented

        elif checker == "crash":
            raise NotImplemented

        elif checker == "empty":
            raise NotImplemented

        elif checker == "diff":
            raise NotImplemented

    def check_limit(self, query):

        self.log.debug("  >> Checking limit condition...", _print=True)

        query_without_limit = ""
        lines = query.splitlines()
        for line in lines:
            if "limit" in line and "offset" not in line:
                line = re.sub(r'limit \d+', '', line)
                query_without_limit += line + "\n"
            else:
                query_without_limit += line + "\n"

        fuzz_result, query_col, _, _ = self.query_run(query_without_limit)
        out_set = self.process_line(fuzz_result)
        result = self.compare_output(out_set, query)
        new_fuzz_result = new_result_after_process(fuzz_result, out_set)

        return result, new_fuzz_result, query_col

    def check_precision(self, query):
        return NOT_BUG

    def check_null(self, query, fuzz_result):
        out_pg, error_pg, out_cr, error_cr, out_my, error_my, out_sq, error_sq = fuzz_result

        if "NULL" in out_pg or "NULL" in out_cr:
            return CORRECTNESS_BUG

        return NOT_BUG

    def check_crash(self, fuzz_result, query_col, status_col):

        # Skip: normal exit, syntax error, timeout
        if has_error_code(status_col, [0, 256, 31744]):
            return CRASH_BUG
        else:
            return NOT_BUG

    def check_empty(self, query, fuzz_result):
        out_pg, error_pg, out_cr, error_cr, out_my, error_my, out_sq, error_sq = fuzz_result
        total_len = len(out_pg) + len(out_cr) + len(out_my) + len(out_sq)

        if total_len > 0:
            if len(out_pg) == 0 or len(out_cr) == 0 or len(out_my) == 0 \
                or len(out_sq) == 0:
                return CORRECTNESS_BUG
        return NOT_BUG

    def check_performance(self, elapsed):

        elapsed_pg, elapsed_cr, elapsed_my, elapsed_sq = elapsed

        if 0.00111 in elapsed:
            if "DEBUG" in os.environ:
                print("  >> too short execution", elapsed)

            return NOT_BUG, 0.0  # we don't consider

        min_time = min(elapsed_pg, elapsed_cr, elapsed_my, elapsed_sq)
        print(
            " |-PG:%6.3f, CR:%6.3f, MY:%6.3f, SQ:%6.3f"
            % (elapsed_pg / min_time, elapsed_cr / min_time,
               elapsed_my / min_time, elapsed_sq / min_time))

        if (elapsed_pg / min_time) > PERFORMANCE_THRESHOLD or \
           (elapsed_cr / min_time) > PERFORMANCE_THRESHOLD or \
           (elapsed_my / min_time) > PERFORMANCE_THRESHOLD or \
           (elapsed_sq / min_time) > PERFORMANCE_THRESHOLD:

            max_time = max(elapsed_pg, elapsed_cr, elapsed_my, elapsed_sq)
            self.performance = (elapsed_pg, elapsed_cr, elapsed_my, elapsed_sq)
            self.performance_ratio = float(max_time / min_time)
            return PERFORMANCE_BUG, float(max_time / min_time)

        # no performance
        return NOT_BUG, 0.0

    def triage(self, fuzz_result, query_col, elapsed, status_col):

        out_pg, error_pg, out_cr, error_cr, out_my, error_my, out_sq, error_sq = fuzz_result
        query, query_sq, query_pg, query_my, query_cr = query_col
        elapsed_pg, elapsed_cr, elapsed_my, elapsed_sq = elapsed
        status_pg, status_cr, status_my, status_sq = status_col

        # CLASS1: CHECK any DBMS crash (by process name or port)
        if self.check_crash(fuzz_result, query_col, status_col) != NOT_BUG:
            self.save_bug(fuzz_result, query_col, "crash")
            self.log.debug("  >> Oh, crash bug?", _print=True)

        # CLASS2: CHECK any performance performance
        #  - we don't check if there is error
        if status_pg != 256 and status_cr != 256 and status_my != 256:
            if "DEBUG" in os.environ:
                "[*] Checking performance bug"
            result, ratio = self.check_performance(elapsed)
            if result != NOT_BUG:
                self.log.debug(
                    "  >> Performance bug? (diff: %5.3f)" % ratio, _print=True)
                self.save_bug(
                    fuzz_result, query_col, "performance", storeout=False)

        # STORE any syntax error for improving transformer
        if self.store_syntax_err(fuzz_result, query_col, status_col):
            return

        # CLASS3: CHECK any correctness bug
        #  - True if all output are same
        out_set = self.process_line(fuzz_result)
        result = self.compare_output(out_set, query)
        if result != NOT_BUG:
            self.bug_checker(query, fuzz_result, query_col, status_col)

    def bug_checker(self, query, fuzz_result, query_col, status_col):

        # we will add many condition to filter false positives
        # TODO: resolve mysql error
        # TODO: triage previous bug condition
        # TODO: build testing env in the local machine

        Identified_bug = False
        FP_bug = False

        # CHECK NULL bug
        if Identified_bug is False and self.check_null(
            query, fuzz_result) != NOT_BUG:
            Identified_bug = True
            self.log.debug(
                "  >> Correctness bug caused by different NULL repr.",
                _print=True)
            self.save_bug(fuzz_result, query_col, "null", storeout=False)

        # CHECK precision bug
        if Identified_bug is False and self.check_precision(query) != NOT_BUG:
            Identified_bug = True
            self.save_bug(fuzz_result, query_col, "precision", storeout=True)

        # CHECK LIMIT condition
        has_bug, fuzz_result, query_col = self.check_limit(query)
        if Identified_bug is False and has_bug != NOT_BUG:
            self.save_bug(fuzz_result, query_col, "limit", storeout=False)
            self.log.debug(
                "  >> Correctness bug even after removing limit", _print=True)
            Identified_bug = True
        else:
            FP_bug = True
            self.log.debug("    >> LIMIT FP, we don't save this", _print=True)

        # CHECK if one result is empty
        if Identified_bug is False and self.check_empty(
            query, fuzz_result) != NOT_BUG:
            Identified_bug = True
            self.log.debug(
                "  >> Correctness bug, one output is empty", _print=True)
            self.save_bug(fuzz_result, query_col, "empty", storeout=True)

        # Store new correctness bug
        if Identified_bug is False and FP_bug is False:
            self.total_cor_bug += 1
            self.log.debug(
                "  >> Correct bug, just different result"
                % (self.total_cor_bug), _print=True)
            self.save_bug(fuzz_result, query_col, "diff", storeout=True)


def main():
    signal.signal(
        signal.SIGINT, exit_gracefully(signal.getsignal(signal.SIGINT)))

    # DEFINE PARSER
    parser = argparse.ArgumentParser()
    subparser = parser.add_subparsers(title='sub-parsers')

    # trace thru dynamic instrumentation
    fuzzer = subparser.add_parser(
        'fuzz', help='Fuzzing', add_help=False)
    fuzzer.add_argument(
        "-o", "--output", dest="output", type=str,
        default=None, help="output_directory", required=False)
    fuzzer.add_argument(
        "-r", "--resume", dest="resume", type=str,
        default=None, help="should we resume", required=False)
    fuzzer.set_defaults(action='fuzzer')

    # statistical debugging
    checker = subparser.add_parser(
        'checker', help='Bug checker', add_help=False)
    checker.add_argument(
        "-o", "--output", dest="output", type=str,
        default=None, help="output_directory", required=False)
    checker.add_argument(
        "-i", "--input", dest="input", type=str,
        default=None, help="Input file", required=True)
    checker.add_argument(
        "-c", "--checker", dest="checker", type=str,
        default=None, help="Which checker to use", required=True)
    checker.set_defaults(action='checker')

    args = parser.parse_args()

    if args.resume is None:
        resume = False
    else:
        resume = True

    cf = CorrectnessFuzzer(
        targetdb="sqlite", cleanlog=True, output=args.output, resume=resume)

    if args.action == "fuzzer":
        cf.run_fuzz_sqlite_inv_exec()

    # test option to validate defined checker
    elif args.action == 'checker':
        # checker: performance, limit, null,
        cf.check(args.input, args.checker)


if __name__ == "__main__":
    main()
