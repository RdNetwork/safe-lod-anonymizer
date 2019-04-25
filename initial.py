import SPARQLWrapper
import sys
import time
import os.path
from exp import get_IRIs
from util import ConfigSectionMap

def main():
    """Main execution function."""
    reload(sys)
    sys.setdefaultencoding('utf-8')
    ENDPOINT=ConfigSectionMap("Endpoint")['url']
    PREFIX=ConfigSectionMap("Graph")['uri']
    sparql = SPARQLWrapper.SPARQLWrapper(ENDPOINT)
    graph_list = ["drugbank", "tcl", "bnf", "lubm", "med", "insee", "heritage"]

    with open("./out/initial_iris.csv","w+") as f:
        f.write("Graph,IRIs,Blanks\n")

    for g in graph_list:
        with open("./out/initial_deg_"+g+".csv","w+") as f:
            f.write("Node,OutDegree,InDegree\n")
        print "Processing the " + g + " graph..."
        uri=PREFIX+g+"/"

        print "\tFetching IRIs..."
        start = time.time()
        nb_iris = get_IRIs(sparql, uri)
        end = time.time()
        print "\tDone! (Took " + str(end-start) + " seconds)"

        print "\tFetching blank nodes..."
        start = time.time()
        nb_blanks = get_IRIs(sparql, uri, True)
        end = time.time()
        print "\tDone! (Took " + str(end-start) + " seconds)"

        if not os.path.isfile("./out/initial_deg_"+g+".csv"):
            print "\tFetching degrees..."
            start = time.time()
            sparql.setQuery("DEFINE sql:log-enable 2 WITH <"+uri+"> SELECT ?n (COALESCE(MAX(?out),0) as ?outDegree) (COALESCE(MAX(?in),0) as ?inDegree) WHERE{ {SELECT ?n (COUNT(?p)  AS ?out) WHERE {?n ?p ?n2.} GROUP BY ?n} UNION {SELECT ?n (COUNT(?p)  AS ?in) WHERE {?n2 ?p ?n} GROUP BY ?n}}")
            sparql.setReturnFormat(SPARQLWrapper.JSON)
            results = sparql.query().convert()
 
            with open("./out/initial_deg_"+g+".csv","a+") as f:
                for r in results["results"]["bindings"]:
                    node = r["n"]["value"]
                    out_deg = int(r["outDegree"]["value"])
                    in_deg = int(r["inDegree"]["value"])   
                    f.write(node + "," + str(out_deg) + "," + str(in_deg) + "\n")
            end = time.time()
            print "\tDone! (Took " + str(end-start) + " seconds)"    
        else:
            print "\tDegree file already existing. Skipping!"


if __name__ == "__main__":
    main()