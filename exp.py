"""Main execution module for testing anonymization algorithm (here the safety alg)"""
import os
import re
import SPARQLWrapper
import time
import csv
import pyodbc
import pyfastcopy
import shutil
import fileinput
import copy
from util import ConfigSectionMap

ENDPOINT=ConfigSectionMap("Endpoint")['url']
OLD_GRAPH=ConfigSectionMap("Graph")['uri']+ConfigSectionMap("Graph")['name']+"/"
NEW_GRAPH=OLD_GRAPH[:-1]+'_anon'
DSN=ConfigSectionMap("ODBC")['dsn']
TIMESTAMP=time.time()

def get_number_triples(sparql, graph):
    """Get the total number of triples in a graph."""
    sparql = SPARQLWrapper.SPARQLWrapper(ENDPOINT)
    sparql.setQuery("DEFINE sql:log-enable 3 WITH <"+OLD_GRAPH+"> SELECT (COUNT(?s) AS ?triples) WHERE { ?s ?p ?o }")
    sparql.setReturnFormat(SPARQLWrapper.JSON)
    sparql.queryType = SPARQLWrapper.SELECT
    results = sparql.query().convert()   
    return int(results["results"]["bindings"][0]["triples"]["value"])


def run_eval(nb_threads, nb_mutations, deg_chk, prec_chk):
    """Experimental process runner"""
    sparql = SPARQLWrapper.SPARQLWrapper(ENDPOINT)
    sparql.setTimeout(5000)
    
    if prec_chk:
        print "Reading pre-stored values..."
        # old_quantity = get_number_triples(sparql, OLD_GRAPH)
        # print "\tGetting number of IRIs in the original graph..."
        # nb_iris = get_IRIs(sparql, OLD_GRAPH)
        # print "\tGetting number of blank nodes in the original graph..."
        # nb_blanks = get_IRIs(sparql, OLD_GRAPH, True)
        nb_iris = None
        nb_blanks = None
        with open("./out/initial_iris.csv") as f:
            lines = csv.reader(f, delimiter=',')
            for l in lines:
                if l[0] == ConfigSectionMap("Graph")['name']:
                    nb_iris = l[1]
                    nb_blanks = l[2]
                    break
        if (nb_blanks == None) or (nb_iris == None):
            return

        with open("./out/results/precision.csv", "w+") as f:
            f.write("Thread,Mutation,Precision,PrecSuppr,PrecGen\n")

    for nb_th in range(0,nb_threads):
        for nb_mut in range(0, nb_mutations):
            try:
                with open("./out/ops/thr"+str(nb_th)+"_mut"+str(nb_mut)+".txt") as f: 
                    clean_file = f.read().replace('\n', '').replace('\t', '')
                    ops = clean_file.split(",")
                    # Removing starting and ending brackets
                    ops[0] = ops[0][1:]
                    ops[-1] = ops[-1][:-1]
            except:
                print "Skipping missing mutation..."
                continue

            # Cleanup query
            # print "Copying graph..."
            # sparql.setMethod(SPARQLWrapper.POST)
            # sparql.setQuery('DEFINE sql:log-enable 2 COPY GRAPH <'+OLD_GRAPH+'> to GRAPH <'+NEW_GRAPH+"_"+TIMESTAMP+'/>')
            # sparql.queryType = SPARQLWrapper.SELECT
            # sparql.query()

            print "Sequence of operations for mutation"+str(nb_mut)+" in thread "+str(nb_th)+"..."
            new_del_graph = NEW_GRAPH+"_"+str(nb_th)+"_"+str(nb_mut)+"_"+str(TIMESTAMP)+"_del"
            new_upd_graph = NEW_GRAPH+"_"+str(nb_th)+"_"+str(nb_mut)+"_"+str(TIMESTAMP)+"_upd"
            ind_op = 0
            for op in ops:
                print "\tOp." + str(ind_op+1) + " out of " + str(len(ops)) + "..."
                ind_op += 1
                (del_txt, upd_txt, where_txt) = parse_str_op(op)
                # Creating graph with deleted triples
                print "\tComputing deletion graph..."
                start = time.time()
                sparql.setMethod(SPARQLWrapper.POST)
                sparql.setQuery("DEFINE sql:log-enable 2 INSERT { GRAPH <" + new_del_graph + "> { " + del_txt + " } }  WHERE { GRAPH <" + OLD_GRAPH + "> { " + where_txt + " } }")
                sparql.setReturnFormat(SPARQLWrapper.JSON)
                sparql.queryType = SPARQLWrapper.SELECT
                sparql.query()
                end = time.time()
                print "\tDone! (Took " + str(end-start) + " seconds)"

                # Creating graph with new triples
                print "\tComputing insertion graph..."
                start = time.time()
                sparql.setMethod(SPARQLWrapper.POST)
                sparql.setQuery("DEFINE sql:log-enable 2 INSERT { GRAPH <" + new_upd_graph + "> { " + upd_txt + " } }  WHERE { GRAPH <" + OLD_GRAPH + "> { " + where_txt + " } }")
                sparql.setReturnFormat(SPARQLWrapper.JSON)
                sparql.queryType = SPARQLWrapper.SELECT
                sparql.query()
                end = time.time()
                print "\tDone! (Took " + str(end-start) + " seconds)"

                # res_check = True
                # while (res_check == True):
                #     print "Applying operation ("+str(sum_del)+" triples deleted so far)..."
                #     sparql.setMethod(SPARQLWrapper.POST)
                #     sparql.setQuery("DEFINE sql:log-enable 2 WITH <"+NEW_GRAPH+"_"+TIMESTAMP+"/> " + op + " LIMIT 200000")
                #     sparql.setReturnFormat(SPARQLWrapper.JSON)
                #     sparql.queryType = SPARQLWrapper.SELECT
                #     results = sparql.query().convert()

                #     msg = results["results"]["bindings"][0]['callret-0']['value']
                #     print msg
                #     m = re.search(r".* (?P<del>\d+) .* (?P<ins>\d+) .*", msg)
                #     del_tot = m.group('del')
                #     ins_tot = m.group('ins')
                #     if (del_tot < 200000):
                #         res_check = False
                        
                #     # Compute number of deleted and replaced triples in this sequence
                #     sum_upd += int(ins_tot)
                #     del_trips = int(del_tot) - int(ins_tot)
                #     sum_del += del_trips

            print "Fetching the number of new blanks..."
            start = time.time()
            nb_added = get_IRIs(sparql, new_upd_graph, True)
            mid = time.time()
            print "Fetching the number of existing blanks..."
            nb_existing = get_IRIs(sparql, new_del_graph, True)
            end = time.time()
            nb_new_blanks = nb_added - nb_existing
            print "\tDone! (Took " + str(mid-start) + " + " + str(end-mid) + " seconds)"

            if prec_chk:
                # Compute precision
                # print "\tGetting number of blank nodes in the new graph..."
                # nb_blanks_new = get_IRIs(sparql, NEW_GRAPH+"_"+TIMESTAMP+"/",True)

                # alpha = 0     # alpha > 0.5 <=> deletions are penalised
                # prec_suppr = 1 - (float(sum_del) / float(old_quantity))
                # print prec_suppr
                # print str(nb_blanks_new) + " blank nodes in the new graph"
                prec_gen = 1 - (float(nb_new_blanks) / float(nb_iris))
                print prec_gen
                # prec = alpha*prec_suppr + (1.0 - alpha)*prec_gen 
                # Write result line
                with open("./out/results/precision.csv", "a+") as f:
                    f.write(str(nb_th)+","+str(nb_mut)+","+str(prec_gen)+"\n") 

            if deg_chk: 
                # Compute degree 
                get_degrees(sparql, nb_th, nb_mut, NEW_GRAPH)


def parse_str_op(s):
    parts = re.split("[{}]",s)
    if len(parts) > 6:
        # INSERT clause is there
        d = parts[1]
        u = parts[3]
        w = parts[5]
    else:
        d = parts[1]
        u = None
        w = parts[3]

    return (d,u,w)


def get_IRIs(sparql, graph, blanks = False):
    s = """SELECT  COUNT ( DISTINCT
      box_hash (
       __id2in ( "s_1_1_t0"."S"),
       __id2in ( "s_1_1_t0"."P"),
       __ro2sq ( "s_1_1_t0"."O"))) AS "callret-0"
FROM DB.DBA.RDF_QUAD AS "s_1_1_t0"
WHERE
  "s_1_1_t0"."G" = __i2idn ( __bft( 'GRAPH' , 1))
  AND
  is_named_iri_id ( "s_1_1_t0"."O")
OPTION (QUIETCAST)"""

    s = s.replace("GRAPH",graph)

    if blanks:
        # check = "isBlank"
        s = s.replace("is_named_iri_id","is_bnode_iri_id")

    # Specifying the ODBC driver, server name, database, etc. directly
    cnxn =  pyodbc.connect('DSN=VM Virtuoso;UID=dba;PWD=dba')
    # Create a cursor from the connection
    cursor = cnxn.cursor()

    # print s 
    res = 0
    start = time.time()
    cursor.execute(s)
    res_int = cursor.fetchone()[0]
    res += int(res_int) 
    mid1 = time.time()
    print "\t\tTook " + str(mid1-start) + " seconds to count blank objects ("+ str(res_int) + ")."

    s = s.replace('"s_1_1_t0"."O"','"s_1_1_t0"."P"')
    cursor.execute(s)
    res_int = cursor.fetchone()[0]
    res += int(res_int) 
    mid2 = time.time()
    print "\t\tTook " + str(mid2-mid1) + " seconds to count blank predicates ("+ str(res_int) + ")."

    s = s.replace('"s_1_1_t0"."P"','"s_1_1_t0"."S"')    
    cursor.execute(s)
    res_int = cursor.fetchone()[0]
    res += int(res_int) 
    end = time.time()
    print "\t\tTook " + str(end-mid2) + " seconds to count blank subjects ("+ str(res_int) + ")."

    return res
    

def get_degrees(sparql, num_thr, num_mut, graph):
    """Computing degrees for the given graph and mutation"""

    print "Computing degrees for this mutation..."
    # shutil.copyfile("./out/initial_deg_"+ConfigSectionMap("Graph")['name']+".csv", "./out/results/degree_thr"+str(num_thr)+"_mut"+str(num_mut)+".csv")

    if (num_mut == 0):
        print "Original policy: skipping..."
        return

    print "\tComputing deletion degrees..."
    start = time.time()
    sparql.setQuery("DEFINE sql:log-enable 2 WITH <"+graph+"_"+str(num_thr)+"_"+str(num_mut)+"_"+str(TIMESTAMP)+"_del"+"> SELECT ?n (COALESCE(MAX(?out),0) as ?outDegree) (COALESCE(MAX(?in),0) as ?inDegree) WHERE{ {SELECT ?n (COUNT(?p)  AS ?out) WHERE {?n ?p ?n2.} GROUP BY ?n} UNION {SELECT ?n (COUNT(?p)  AS ?in) WHERE {?n2 ?p ?n} GROUP BY ?n}}")
    res_del = sparql.query().convert()
    end = time.time()
    print "\tDone! (Took " + str(end-start) + " seconds)"

    print "\tComputing insertion degrees..."
    start = time.time()
    sparql.setQuery("DEFINE sql:log-enable 2 WITH <"+graph+"_"+str(num_thr)+"_"+str(num_mut)+"_"+str(TIMESTAMP)+"_upd"+"> SELECT ?n (COALESCE(MAX(?out),0) as ?outDegree) (COALESCE(MAX(?in),0) as ?inDegree) WHERE{ {SELECT ?n (COUNT(?p)  AS ?out) WHERE {?n ?p ?n2.} GROUP BY ?n} UNION {SELECT ?n (COUNT(?p)  AS ?in) WHERE {?n2 ?p ?n} GROUP BY ?n}}")
    res_upd = sparql.query().convert()
    end = time.time()
    print "\tDone! (Took " + str(end-start) + " seconds)"

    with open("./out/results/degree_thr"+str(num_thr)+"_mut"+str(num_mut)+"_upd.csv", "w+") as f : 
        f.write("Node,OutDegree,InDegree\n")
    with open("./out/results/degree_thr"+str(num_thr)+"_mut"+str(num_mut)+"_del.csv", "w+") as f : 
        f.write("Node,OutDegree,InDegree\n")

    print "\tWriting partial degrees..."
    for r in res_del["results"]["bindings"]:
        n = r["n"]["value"]
        o_d = int(r["outDegree"]["value"])
        i_d = int(r["inDegree"]["value"])
        with open("./out/results/degree_thr"+str(num_thr)+"_mut"+str(num_mut)+"_del.csv", "a+") as f: 
            f.write(','.join((n,o_d,i_d)))

    for r in res_upd["results"]["bindings"]:
        n = r["n"]["value"]
        o_d = int(r["outDegree"]["value"])
        i_d = int(r["inDegree"]["value"])
        with open("./out/results/degree_thr"+str(num_thr)+"_mut"+str(num_mut)+"_upd.csv", "a+") as f: 
            f.write(','.join((n,o_d,i_d)))

# def get_deleted_triples(server, orig_number):
#     return int(orig_number) - count_triples(server, "http://localhost/"+NEW_GRAPH+"/")

# def count_triples(server, graph):
#     res = server.query("WITH <"+graph+"> SELECT (COUNT(?s) AS ?triples) WHERE { ?s ?p ?o }")
#     print res['results']['bindings'][0]['triples']['value'] + " triples found."
#     return int(res['results']['bindings'][0]['triples']['value'])

# def run_eval_alt(nb_threads, nb_mutations, deg_chk, prec_chk):
#     """Alternative experimental process (more graph creations but with small graphs)"""
#     sparql = SPARQLWrapper.SPARQLWrapper(ENDPOINT)
#     sparql.setTimeout(2000)
    
#     if prec_chk:
#         old_quantity = get_number_triples(sparql, OLD_GRAPH)
#         print "\tGetting number of IRIs in the original graph..."
#         nb_iris = get_IRIs(sparql, OLD_GRAPH)
#         print "\tGetting number of blank nodes in the original graph..."
#         nb_blanks = get_IRIs(sparql, OLD_GRAPH, True)
#         with open("./out/results/precision.csv", "w+") as f:
#             f.write("Thread,Mutation,Precision,PrecSuppr,PrecGen\n")

#     for nb_th in range(0,nb_threads):
#         for nb_mut in range(0, nb_mutations):
#             with open("./out/ops/thr"+str(nb_th)+"_mut"+str(nb_mut)+".txt") as f: 
#                 clean_file = f.read().replace('\n', '').replace('\t', '')
#                 ops = clean_file.split(",")
#                 # Removing starting and ending brackets
#                 ops[0] = ops[0][1:]
#                 ops[-1] = ops[-1][:-1]
#                 sparql.setMethod(SPARQLWrapper.POST)
#                 print "Sequence of operations for mutation"+str(nb_mut)+" in thread "+str(nb_th)+"..."
#                 sum_del = 0
#                 sum_upd = 0
#                 for op in ops:
#                     # Dropping graphs if they already exist
#                     sparql.setQuery('DEFINE sql:log-enable 3 DROP SILENT GRAPH <'+OLD_GRAPH+'_del_'+nb_th+'_'+nb_mut+'>')
#                     sparql.queryType = SPARQLWrapper.SELECT
#                     sparql.query()
#                     sparql.setQuery('DEFINE sql:log-enable 3 DROP SILENT GRAPH <'+OLD_GRAPH+'_add_'+nb_th+'_'+nb_mut+'>')
#                     sparql.queryType = SPARQLWrapper.SELECT
#                     sparql.query()

#                     # Creating annex graphs
#                     print "Creating graph to store new triples..."
#                     sparql.setQuery('DEFINE sql:log-enable 3 COPY GRAPH <'+OLD_GRAPH+'> to GRAPH <'+NEW_GRAPH+"_"+TIMESTAMP+'/>')
#                     sparql.queryType = SPARQLWrapper.SELECT
#                     sparql.query()
#                     print "Creating graph to store new triples..."
#                     sparql.setQuery('DEFINE sql:log-enable 3 COPY GRAPH <'+OLD_GRAPH+'> to GRAPH <'+NEW_GRAPH+"_"+TIMESTAMP+'/>')
#                     sparql.queryType = SPARQLWrapper.SELECT
#                     sparql.query()

#                     print "Applying operation ("+str(sum_del)+" triples deleted so far)..."
#                     sparql.setMethod(SPARQLWrapper.POST)
#                     sparql.setQuery("DEFINE sql:log-enable 3 WITH <"+NEW_GRAPH+"_"+TIMESTAMP+"/> " + op)
#                     sparql.setReturnFormat(SPARQLWrapper.JSON)
#                     sparql.queryType = SPARQLWrapper.SELECT
#                     results = sparql.query().convert()

#                     msg = results["results"]["bindings"][0]['callret-0']['value']
#                     print msg
#                     m = re.search(r".* (?P<del>\d+) .* (?P<ins>\d+) .*", msg)
#                     del_tot = m.group('del')
#                     ins_tot = m.group('ins')

#                     # Compute number of deleted and replaced triples in this sequence
#                     sum_upd += int(ins_tot)
#                     del_trips = int(del_tot) - int(ins_tot)
#                     sum_del += del_trips

#                 if prec_chk:
#                     # Compute precision
#                     print "\tGetting number of blank nodes in the new graph..."
#                     nb_blanks_new = get_IRIs(sparql, NEW_GRAPH+"_"+TIMESTAMP+"/",True)
#                     alpha = 0.5     # alpha > 0.5 <=> deletions are penalised
#                     prec_suppr = 1 - (float(sum_del) / float(old_quantity))
#                     print prec_suppr
#                     print str(nb_blanks_new) + " blank nodes in the new graph"
#                     prec_gen = 1 - (float(int(nb_blanks_new) - int(nb_blanks)) / float(nb_iris))
#                     print prec_gen
#                     prec = alpha*prec_suppr + (1.0 - alpha)*prec_gen 
#                     # Write result line
#                     with open("./out/results/precision.csv", "a+") as f:
#                         f.write(str(nb_th)+","+str(nb_mut)+","+str(prec)+","+str(prec_suppr)+","+str(prec_gen)+"\n") 

#                 if deg_chk: 
#                     # Compute degree
#                     get_degrees(sparql, nb_th, nb_mut, NEW_GRAPH+"_"+TIMESTAMP)