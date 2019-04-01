"""Main execution module for testing anonymization algorithm (here the safety alg)"""
import os
import re
import xml.etree.ElementTree as ET
import SPARQLWrapper
import time
import ConfigParser
from util import ConfigSectionMap

ENDPOINT=ConfigSectionMap("Endpoint")['URL']
OLD_GRAPH=ConfigSectionMap("Graph")['URI']+"/"+ConfigSectionMap("Graph")['Name']+"/"
NEW_GRAPH=OLD_GRAPH[:-1]+'_anon'

def get_number_triples(sparql, graph):
    """Get the total number of triples in a graph."""
    sparql = SPARQLWrapper.SPARQLWrapper(ENDPOINT)
    sparql.setQuery("DEFINE sql:log-enable 3 WITH <"+OLD_GRAPH+"> SELECT (COUNT(?s) AS ?triples) WHERE { ?s ?p ?o }")
    sparql.setReturnFormat(SPARQLWrapper.JSON)
    sparql.queryType = SPARQLWrapper.SELECT
    results = sparql.query().convert()   
    return int(results["results"]["bindings"][0]["triples"]["value"])

def run_eval(nb_threads, nb_mutations):

    sparql = SPARQLWrapper.SPARQLWrapper(ENDPOINT)
    old_quantity = get_number_triples(sparql, OLD_GRAPH)
    print "\tGetting number of IRIs in the original graph..."
    nb_iris = get_IRIs(sparql, OLD_GRAPH)
    print "\tGetting number of blank nodes in the original graph..."
    nb_blanks = get_IRIs(sparql, OLD_GRAPH, True)

    with open("./out/results/precision.csv", "w+") as f:
        f.write("Thread,Mutation,Precision,PrecSuppr,PrecGen\n")

    for nb_th in range(0,nb_threads):
        for nb_mut in range(0, nb_mutations):
            with open("./out/ops/thr"+str(nb_th)+"_mut"+str(nb_mut)+".txt") as f: 
                clean_file = f.read().replace('\n', '').replace('\t', '')
                ops = clean_file.split(",")
                # Removing starting and ending brackets
                ops[0] = ops[0][1:]
                ops[-1] = ops[-1][:-1]

            # Cleanup query
            print "Copying graph..."
            TIMESTAMP = str(time.time())
            sparql.setMethod(SPARQLWrapper.POST)
            sparql.setQuery('DEFINE sql:log-enable 3 COPY GRAPH <'+OLD_GRAPH+'> to GRAPH <'+NEW_GRAPH+"_"+TIMESTAMP+'/>')
            sparql.queryType = SPARQLWrapper.SELECT
            sparql.query()
            with open("./out/results/precision.csv", "a+") as f:
                sum_del = 0
                sum_upd = 0
                print "Sequence of operations for mutation"+str(nb_mut)+" in thread "+str(nb_th)+"..."
                for op in ops:
                    print "Applying operation ("+str(sum_del)+" triples deleted so far)..."
                    sparql.setMethod(SPARQLWrapper.POST)
                    sparql.setQuery("DEFINE sql:log-enable 3 WITH <"+NEW_GRAPH+"_"+TIMESTAMP+"/> " + op)
                    sparql.setReturnFormat(SPARQLWrapper.JSON)
                    sparql.queryType = SPARQLWrapper.SELECT
                    results = sparql.query().convert()

                    msg = results["results"]["bindings"][0]['callret-0']['value']
                    print msg
                    m = re.search(r".* (?P<del>\d+) .* (?P<ins>\d+) .*", msg)
                    del_tot = m.group('del')
                    ins_tot = m.group('ins')

                    # Compute number of deleted and replaced triples in this sequence
                    sum_upd += int(ins_tot)
                    del_trips = int(del_tot) - int(ins_tot)
                    sum_del += del_trips

                print "\tGetting number of blank nodes in the new graph..."
                nb_blanks_new = get_IRIs(sparql, NEW_GRAPH+"_"+TIMESTAMP+"/",True)

                # Computing precision measure for the sequence
                alpha = 0.5     # alpha > 0.5 <=> deletions are penalised
                prec_suppr = 1 - (float(sum_del) / float(old_quantity))
                print prec_suppr
                print str(nb_blanks_new) + " blank nodes in the new graph"
                prec_gen = 1 - (float(int(nb_blanks_new) - int(nb_blanks)) / float(nb_iris))
                print prec_gen
                prec = alpha*prec_suppr + (1.0 - alpha)*prec_gen 
                # Write result line
                f.write(str(nb_th)+","+str(nb_mut)+","+str(prec)+","+str(prec_suppr)+","+str(prec_gen)+"\n") 
               
                # Compute degree
                # get_degrees(sparql, nb_th, nb_mut, NEW_GRAPH)


def get_IRIs(sparql, graph, blanks = False):
    if blanks:
        check = "isBlank"
    else: 
        check = "isIRI"

    res = 0
    sparql.setQuery("WITH <"+graph+"> SELECT COUNT(DISTINCT *) WHERE { ?s ?p ?o FILTER "+check+"(?s)}")
    sparql.setReturnFormat(SPARQLWrapper.JSON)
    results = sparql.query().convert()
    res += int(results["results"]["bindings"][0]['callret-0']['value'])

    sparql.setQuery("WITH <"+graph+"> SELECT COUNT(DISTINCT *) WHERE {?s ?p ?o FILTER "+check+"(?p)}")
    sparql.setReturnFormat(SPARQLWrapper.JSON)
    results = sparql.query().convert()
    res += int(results["results"]["bindings"][0]['callret-0']['value'])

    sparql.setQuery("WITH <"+graph+"> SELECT COUNT(DISTINCT *) WHERE {?s ?p ?o FILTER "+check+"(?o)}")
    sparql.setReturnFormat(SPARQLWrapper.JSON)
    results = sparql.query().convert()
    res += int(results["results"]["bindings"][0]['callret-0']['value']) 

    return res


def get_degrees(sparql, num_thr, num_mut, graph):
    """Computing degrees for the given graph and mutation"""
    print "Computing degrees for this mutation..."
    sparql.setQuery("DEFINE sql:log-enable 3 WITH <"+graph+"/> SELECT ?n (COALESCE(MAX(?out),0) as ?outDegree) (COALESCE(MAX(?in),0) as ?inDegree) WHERE{ {SELECT ?n (COUNT(?p)  AS ?out) WHERE {?n ?p ?n2.} GROUP BY ?n} UNION {SELECT ?n (COUNT(?p)  AS ?in) WHERE {?n2 ?p ?n} GROUP BY ?n}}")
    results = sparql.query().convert()
    with open("./out/results/degree_thr"+str(num_thr)+"_mut"+str(num_mut)+".csv", "w+") as f:
        print "Writing results..."
        f.write("Node,OutDegree,InDegree\n")
        for r in results["results"]["bindings"]:
            f.write(r["n"]["value"]+','+r["outDegree"]["value"]+','+r["inDegree"]["value"]+"\n")

# def get_deleted_triples(server, orig_number):
#     return int(orig_number) - count_triples(server, "http://localhost/"+NEW_GRAPH+"/")

# def count_triples(server, graph):
#     res = server.query("WITH <"+graph+"> SELECT (COUNT(?s) AS ?triples) WHERE { ?s ?p ?o }")
#     print res['results']['bindings'][0]['triples']['value'] + " triples found."
#     return int(res['results']['bindings'][0]['triples']['value'])
