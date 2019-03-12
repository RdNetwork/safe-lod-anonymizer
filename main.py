"""Main execution module."""
import random
import shutil
import os
import sys
import csv
import time
from rdflib import Graph
from policy import Policy
from query import Query
from anonymization import find_safe_ops
from prefix import Prefix
from util import block_print, enable_print, average_wl_size

GMARK_QUERIES = 500

def custom_prefixes():
    "Generating RDF prefixes used in our framework."
    p = []
    p.append(Prefix("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#"))
    p.append(Prefix("rdfs", "http://www.w3.org/2000/01/rdf-schema#"))
    p.append(Prefix("owl", "http://www.w3.org/2002/07/owl#"))
    p.append(Prefix("xsd", "http://www.w3.org/2001/XMLSchema#"))
    p.append(Prefix("dc", "http://purl.org/dc/elements/1.1/"))
    p.append(Prefix("dcterms", "http://purl.org/dc/terms/"))
    p.append(Prefix("foaf", "http://xmlns.com/foaf/0.1/"))
    p.append(Prefix("geo", "http://www.w3.org/2003/01/geo/wgs84_pos#"))
    p.append(Prefix("datex", "http://vocab.datex.org/terms#"))
    p.append(Prefix("lgdo", "http://linkedgeodata.org/ontology/"))
    p.append(Prefix("tcl", "http://localhost/"))
    p.append(Prefix("gld", "http://data.grandlyon.com/"))
    p.append(Prefix("skos", "http://www.w3.org/2004/02/skos/core#"))
    p.append(Prefix("gtfs", "http://vocab.gtfs.org/terms#"))
    return p


def main():
    """Main execution function."""
    TEST = False            #Test mode: no anonymiation after algorithms execution
    DEMO = False            #Demo mode: fixed gmark policies, simple example
    DEMO_TXT = False        #Textual mode: import queries from text files rather than gmark output
    SAMEAS = False          #Safety modulo sameas : when True, also prevent inference by explicit sameAs links
    NOPRINT = False         #Print mode: if False, no console output

    if "-p" in sys.argv:
        print "Running in experiment mode: No text output..."
        NOPRINT = True
    if "-s" in sys.argv:
        print "Running in strong mode: anonymization modulo sameas..."
        SAMEAS = True
    if "-txt" in sys.argv:
        print "Running in textual mode: reading policy textfiles..."
        DEMO_TXT = True
        p_pol_size = int(sys.argv[1])
    if "-d" in sys.argv:
        print "Running in demo mode: simple fixed privacy policy used."
        DEMO = True
        p_pol_size = 2
    else:
        p_pol_size = int(sys.argv[1])
    
    if "-t" in sys.argv:
        print "Running in test mode: no graph anonymisation after computing sequences."
        TEST = True

    NB_EXPERIMENTS = 1

    if NOPRINT:
        block_print()

    # Fetching gmark queries
    # DEMO uses a simple workload with 2 short queries.
    print "Fetching query workload..."
    if DEMO:
        workload = Query.parse_gmark_queries("./conf/workloads/demo.xml")
    elif DEMO_TXT:
        workload = Query.parse_txt_queries(p_pol_size)
    else:
        workload = Query.parse_gmark_queries("./conf/workloads/star-starchain-workload.xml")

    for _ in range(0, NB_EXPERIMENTS):

        if DEMO:
            p_pol = Policy([workload[0], workload[1]], "P")
            p_pol_nums = [0, 1]
        elif DEMO_TXT:
            p_pol = Policy([workload[i] for i in (range(p_pol_size))], "P")
            p_pol_nums = range(p_pol_size-1)
        else:
            # Creating random seed...
            seed = random.randrange(sys.maxsize)
            random.seed(seed)

            print "Random generator seed: " + str(seed)
            print "Defining policies:"
            # Create privacy policies
            print "\tDefining privacy policy..."
            p_pol = Policy([], "P")
            p_pol_nums = []
            for _ in range(0, p_pol_size):
                q_num = random.randint(0, GMARK_QUERIES-1)
                p_pol.queries.append(workload[q_num])
                p_pol_nums.append(q_num)

        print "\t\tChosen privacy queries: " + str(p_pol_nums)
        p_size = 0
        for i in range(0, p_pol_size):
            p_size += len(p_pol.queries[i].where)
            print "\t\t" + str(p_pol.queries[i])

        # Run algorithm
        print "Computing candidate operations..."
        o = find_safe_ops(p_pol, SAMEAS)
        print "Set of operations found:"
        print(o)
        
        # Writing operations to result files
        with open('./out/ops.txt', 'w+') as outfile:
            outfile.write(str(o))
            outfile.close()
        
        with open('./out/stats.txt', 'a+') as outfile:
            outfile.write(str(len(o))+"\n")
            outfile.close()

        # WIP: Graph anonymization
        if o and not TEST:    
            # Import graph
            print "Importing graph..."
            g = Graph()
            with open("./conf/graphs/graph.ttl", "r") as f:
                g.parse(file=f, format="turtle")
            print str(len(g)) + " triples found"

            print "A set of " + str(len(o)) + " operations was found."
            choice = ''
            while not (choice == 'Y' or choice == 'N'):
                choice = raw_input('Apply anonymization? Y/N (case-sensitive): ')

            # Perform anonymization
            if choice == 'Y':
                if not os.path.exists("./out/"):
                    os.makedirs("./out/")
                print "Anonymizing graph..."
                g.serialize(destination='./out/output_anonymized_orig_'+time.strftime("%Y%m%d-%H%M%S")+'.ttl', 
                            format='trig')
                print "\tOperation " + str(choice) + " launched..."
                seq_step = 0
                for op in o:
                    seq_step += 1
                    op.update(g, custom_prefixes())
                    g.serialize(destination='./out/output_anonymized_step'+str(seq_step)+'_'+time.strftime("%Y%m%d-%H%M%S")+'.ttl',                         format='trig')
                print "\tLength after deletion: " + str(len(g)) + " triples"
            else:
                print "Terminating program..."

if __name__ == "__main__":
    main()
