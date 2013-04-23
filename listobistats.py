#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
Created on Oct 2, 2012

@author: RHT
'''
import re, fileinput, os, sqlparse, optparse #@UnresolvedImport
import sys, subprocess
min_time_per_row, sep = 0, ','
# globals
options, args, usage = None, None, None
csv_header, csv_header_optimized = None, None
temp_sql_file_name = None
temp_out_file_name = None
def setOptions():
    global options, args, usage
    usage = '''usage: python %prog [options] $OBIEE_HOME/.../coreapplication_obis1/nqquery*.log
    
    results.html contains the HTML output
    
    For max benefit, your logical SQLs should contain an integer TEST ID. For example:  
    SET VARIABLE PRODUCT_NAME='SPE', QUERY_NAME='ZSP_LEADS', TEST=76: SELECT "Lead Facts"."# of Leads" Lead_Count,...
    '''
    option_parser = optparse.OptionParser(usage = usage)
    option_parser.add_option("-d", "--out_dir", default = "listobistats", action="store", type="string", dest="out_dir", 
                      help="store results in the specified output directory [default: %default]")
    option_parser.add_option("-m", "--min_elapsed_time", default = 0, action="store", type="int", dest="min_elapsed_time", 
                      help="ignore queries that took less than the min elapsed time in seconds [default: %default]")
    option_parser.add_option("-M", "--min_time_per_row", default = 0, action="store", type="int", dest="min_time_per_row", 
                      help="ignore queries that took less than the min elapsed time per row in milliseconds [default: %default]")
    option_parser.add_option("-i", "--ignore_queries_with_no_results", 
                      action="store_true", dest="ignore_queries_with_no_results", default=False,
                      help="ignore queries that do not return any results [default: %default]")
    option_parser.add_option("-o", "--optimized_queries_only", 
                      action="store_true", dest="optimized_queries_only", default=False,
                      help="ignore queries that have neither a lead status hint nor a time optimization [default: %default]")
    option_parser.add_option("-p", "--explain_plan", 
                      action="store_true", dest="explain_plan", default=False,
                      help="Generates explain Plan for physical sqls by executing them against oracle db [default: %default]")
    option_parser.add_option("-s", "--sqlplus", default = "/usr/local/redhat/OracleProd/oracle10.2/bin/sqlplus", 
                             action="store", type="string", dest="sqlplus", 
                             help="full path to Sqlplus [default: %default] ORACLE_HOME should point to /usr/local/redhat/OracleProd/oracle10.2 or equivalent")
    option_parser.add_option("-c", "--db_connection_string", default = "hr/hr@localhost:1563/orcl", 
                             action="store", type="string", dest="db_connection_string", 
                             help="db Connection string [default: %default]")
    (options, args) = option_parser.parse_args()
def setGlobals():  
    global csv_header, csv_header_optimized
    global temp_sql_file_name, temp_out_file_name
    csv_header = [
                  'test_id', 
                  'lsql_id',
                  'name', 
                  'product_name', 
                  'elapsed_time', 
                  'rows'
                  ]
    csv_header_optimized = [
                  'has_time', 
                  'has_hint', 
                  'contains_case'
                  ]
    temp_sql_file_name = 'temp.sql'
    temp_out_file_name = 'temp_out.log'
def getCsvAttrKeys():
    csv_attrs = []
    [csv_attrs.append(key) for key in csv_header]
    [csv_attrs.append(i) for i in csv_header_optimized if options.optimized_queries_only]            
    return csv_attrs        
def getCsvAttrs(query):
    csv_attrs = []
    [csv_attrs.append(str(getattr(query, key))) for key in getCsvAttrKeys()]
    return csv_attrs
class Query(object):
    def __init__(self, lsql_id='', test_id=0, attrs=None): 
        self.test_id = test_id
        self.elapsed_time = 0
        self.rows = 0
        self.has_time = False
        self.has_hint = False
        self.product_name = ''
        self.name = ''
        self.multiple_psql = False
        self.contains_case = False
        self.lsql_id = lsql_id
        self.adf_query = []
        self.lsql = []
        self.psql_id = []
        self.psql = {}        
    def setCorrupt(self):
        keys = self.psql.keys()
        for p in keys:
            try:
                if not self.psql[p]: 
                    del self.psql[p]
                    self.psql_id.remove(p)
            except:
                continue
        for p in keys:
            try:
                if re.search(r"^BEGIN ", self.psql[p][0]):
                    del self.psql[p]
                    self.psql_id.remove(p)
            except:
#                print >> sys.stderr, 'Warning: Could not process physical sql ' + p
                continue
        for p in keys:
            try:
                if re.search(r"detach_session", self.psql[p][0]) or re.search(r"attach_session", self.psql[p][0]):
                    del self.psql[p]
                    self.psql_id.remove(p)
            except:
#                print >> sys.stderr, 'Warning: Could not process physical sql ' + p
                continue
        if self.adf_query.count('</ADFQuery>') > 1:
            self.multiple_psql = True
    
    def __repr__(self): 
        s = []
        for i in getCsvAttrs(self): s.append(i)
        s.append(' '.join(self.psql_id))
        return sep.join(s)
    def __hash__(self): return hash(self.test_id)
    def __eq__(self, other): 
        return isinstance(other, Query) and self.test_id == other.test_id        
queries = set()
query = Query(test_id=0)
reading_adf_query = False
reading_lsql = False
reading_psql = False
prev_line = ''
curr_psql_id = ''
lsql_re = r'-------------------- SQL Request, logical request hash:'
lsql_detected = False
lsql_skipped = 0
def processLine(line):
    global queries, query, reading_adf_query, reading_lsql, reading_psql, curr_psql_id 
    global lsql_ctr, lsql_detected, lsql_skipped
    line = line.replace("\n", "").replace("\r", "").replace("\^M", "").replace("@", "")
    if re.search(lsql_re, line): lsql_detected = True
    if lsql_detected: lsql_skipped += 1
    if lsql_detected and (lsql_skipped == 3):
        lsql_detected = False
        lsql_skipped = 0  
        l = re.findall(r"TEST=(\d+)", line)
        tid = 0
        if l:
            tid = int(l[0])
        lid = prev_line.strip()
        query = Query(lsql_id=lid, test_id=tid)
        reading_lsql, reading_psql, reading_adf_query = True, False, False 
        queries.add(query)
    if re.search("^\]\]", line): 
        reading_lsql, reading_psql = False, False 
    if re.search('^<ADFQuery mode=\"SQLBypass\"', line): 
        reading_lsql, reading_psql, reading_adf_query = False, False, True
    if reading_lsql:
        query.lsql.append(line.strip())        
        l = re.findall(r'QUERY_NAME=\'([A-Za-z1234567890_]+)\'', line)
        if l: query.name = l[0]                
        l = re.findall(r'PRODUCT_NAME=\'([A-Za-z1234567890_]+)\'', line)
        if l: query.product_name = l[0]                
        if re.search("^FETCH FIRST \d+ ROWS ONLY", line): 
            reading_lsql = False        
    elif reading_psql:
        ls = line.strip()
        if ls: query.psql[curr_psql_id].append(ls)
        if re.search("case ", line, re.IGNORECASE):
            query.contains_case = True                            
    elif reading_adf_query: 
        query.adf_query.append(line)
        if re.search('^</ADFQuery>$', line): 
            reading_adf_query = False
    else:
        if query.lsql_id:             
            l = re.findall("logical request hash " + query.lsql_id + ", physical request hash ([a-z0-9]+):", line)
            if l and not re.search("id: SQLBypass Gateway", line):
                reading_lsql, reading_psql, reading_adf_query = False, True, False
                curr_psql_id = l[0].strip()
                query.psql_id.append(curr_psql_id) 
                query.psql[curr_psql_id] = []
        l = re.findall(r"Rows returned to Client (\d+)", line)
        if l: query.rows = int(l[0])
        l = re.findall(r"Logical Query Summary Stats: Elapsed time (\d+)", line)
        if l: query.elapsed_time = int(l[0])
def includeQuery(q):
    result = True
    if abs(q.elapsed_time) < options.min_elapsed_time: result = False
    tpr = abs(q.elapsed_time * 1000 / (q.rows or 1)) # milliseconds per row
    if tpr < options.min_time_per_row: result = False
    if options.optimized_queries_only and not q.has_time and not q.has_hint: result = False
    if options.ignore_queries_with_no_results and 0 -- q.rows: result = False    
    return result

def getTd(s):
    return "<td>" + str(s) + "</td>"
def getA(text, link):
    return "<a href=" + str(link) + ">" + str(text) + "</a>"
def getTdA(text, link):
    return getTd(getA(text, link))
def getPhysicalSqlA(query, ext):
    l = []
    for i in query.psql.keys():
        l.append(getA(i, i + ext))
    return ' '.join(l)
    
def generateReport():
    global queries
    queries = sorted(queries, reverse = True, key = lambda k : k.elapsed_time)
    print(sep.join(getCsvAttrKeys()) + ",psql_id")
    csv_file = open('results.csv', 'w')
    csv_file.write(sep.join(getCsvAttrKeys())+  ",psql_id" + '\n')
    html_file = open('results.html', 'w')
    html_file.write("""<html><head><title>List BI Statistics</title></head><body>
        <p><b>List OBI Statistics</b></p>
        <p><a href=results.csv>Download a CSV file containing this data</a></p>
        <p><a href=all_logical.sql>Download all logical sqls</a></p>
        <table border="1"><tr>
        <td>min_time (s)</td>
        <td>min_time_per_row (ms)</td>
        <td>queries_parsed</td>
        </tr> 
        """)
    html_file.write("<tr><td>" + str(options.min_elapsed_time) + "</td>" +  
                    "<td>" + str(options.min_time_per_row) + "</td>" +
                    "<td>" + str(len(queries)) + "</td>" +
                    "</tr></table>")
    html_file.write(""" <table border="1"><tr>
        <td>test_id</td>
        <td>query_name</td>
        <td>product_name</td>        
        <td>time (s)</td>
        <td>rows</td>
        <td>logical</td>
        <td>physical</td>
        <td>plan</td>
        <td>adf</td>
        </tr>""")
    all_logical_file = open('all_logical.sql', 'w')
    for q in queries:
        q.setCorrupt()
        if includeQuery(q):        
            print("%r" % q)
            csv_file.write("%r\n" % q)
            fn = q.lsql_id + '_l.sql'
            f = open(fn, 'w')
            lsql = ' '.join(q.lsql)
            all_logical_file.write(lsql + ';\n\n')
            lsql = "---------- logical sql hash=" + q.lsql_id + "\n" + lsql
            f.write(sqlparse.format(lsql, reindent=True, keyword_case='upper'))
            f.close()
            psql = []
            for p in q.psql.keys(): #8,562,565
                e = "---------- physical sql hash=" + p + "\n"
                try:
                    e += sqlparse.format(" ".join(q.psql[p]), reindent=True, keyword_case='upper')                    
                except:
                    print >> sys.stderr, 'Warning: Could not format the physical query ' + p
                    continue                
                psql.append(e)
                psqlstr = ";\n\n".join(psql) 
                fn = p + '_p.sql'
                f = open(fn, 'w')
                f.write(psqlstr)
                f.close()
                fn = p + '_adf.xml'
                f = open(fn, 'w')
                f.write('\n'.join(q.adf_query))
                f.close()
            trunc_name = q.name
            if q.name: trunc_name = (q.name[:30] + '..') 
            html_file.write("<tr>" + 
                            getTd(q.test_id) +
                            getTd(trunc_name) +
                            getTd(q.product_name) +
                            getTd(q.elapsed_time) + 
                            getTd(q.rows) +
                            getTdA(q.lsql_id, q.lsql_id + "_l.sql") +
                            getTd(getPhysicalSqlA(q, "_p.sql")) +
                            getTd(getPhysicalSqlA(q, "_p.txt")) +
                            getTd(getPhysicalSqlA(q, "_adf.xml")) +                            
                            "</tr>")        
    html_file.write("</table></body></html>")
    csv_file.close()
    html_file.close()
    all_logical_file.close()

def executeSql(sql):
    sqlplus = options.sqlplus
    dsn = options.db_connection_string
    sheader = """
    SET ECHO OFF
    SET NEWPAGE 0
    SET LINESIZE 32767
    SET PAGESIZE 0
    SET COLSEP |>#<
    SET FEEDBACK OFF
    SET HEADING OFF
    SET TRIMSPOOL ON
    SET WRAP ON
    SET RECSEP EACH
    SET LONG 32766
    WHENEVER SQLERROR EXIT -1
    WHENEVER OSERROR  EXIT -1
    """
    finalsql = sheader + 'SPOOL ' + temp_out_file_name + '\n' + sql.rstrip(';') + ';' + '\nSPOOL OFF;\nEXIT;'
    temp_sql_file = open(temp_sql_file_name, 'w')
    temp_sql_file.write(finalsql)
    temp_sql_file.close()
    p = subprocess.Popen([sqlplus, '-S' ,dsn, '@' + temp_sql_file_name],
                          stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out = p.communicate()[0]  
    return out
def writeExplainPlan(query):
    for i in query.psql.keys():
        sql = sqlparse.format(' '.join(query.psql[i]), reindent=True, keyword_case='upper')
        sql = 'SET AUTOTRACE ON \n ' + sql
        plan = executeSql(sql)
        f = open(i + '_p.txt', 'w')
        f.write(plan)
        f.close()
        f = open(i + '_p.txt', 'r')
        plan = f.readlines()
        f.close()
        clean = ['Physical SQL hash=' + i + '\n']
        fl = False
        for j in plan:
            if re.search(r'Execution Plan', j): fl = True                
            if fl: clean.append(j)
        f = open(i + '_p.txt', 'w')
        f.write(''.join(clean));
        f.close()
def writePlans():   
    for q in queries:
        writeExplainPlan(q)    
def cleanup():
    pass
def parseQueryLog():
    global prev_line
    setOptions()
    setGlobals()
    for line in fileinput.input(args):
        processLine(line)
        prev_line = line
    if not os.path.exists(options.out_dir):
        os.makedirs(options.out_dir)
    os.chdir(options.out_dir)
    generateReport()
    if options.explain_plan: writePlans()
    cleanup()

if __name__ == "__main__":
    sys.exit(parseQueryLog())

