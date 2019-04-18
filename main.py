"""Main execution module."""
import random
import shutil
import os
import sys
import csv
import time
import copy
import glob
from exp import run_eval
from rdflib import Graph
from policy import Policy
from query import Query
from anonymization import find_safe_ops
from prefix import Prefix
from util import block_print, enable_print, average_wl_size, custom_prefixes

GMARK_QUERIES = 500
NB_EXPERIMENTS = 1
NB_MUT_THREADS = 8
NB_MUTATIONS = 12

def main():
    """Main execution function."""
    reload(sys)
    sys.setdefaultencoding('utf-8')

    TEST = False            #Test mode: no anonymiation after algorithms execution
    DEMO = False            #Demo mode: fixed gmark policies, simple example
    DEMO_TXT = False        #Textual mode: import queries from text files rather than gmark output
    SAMEAS = False          #Safety modulo sameas : when True, also prevent inference by explicit sameAs links
    NOPRINT = False         #Print mode: if False, no console output
    EXP = False             #Experiments mode: creating mutations of the initial policy and running alg. for each
    SKIP = False            #Skip mode: jump directly to the experiments

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
    if "--skip" in sys.argv:
        SKIP = True
    if "-exp" in sys.argv:
        print "Running in experiments mode: creating combinations of edited policies"
        EXP = True
        TEST = True
        if not SKIP:
            for f in glob.glob("./out/ops/*.txt"):
                os.remove(f)
            for f in glob.glob("./out/results/*.csv"):
                os.remove(f)
            for f in glob.glob("./out/policies/*.txt"):
                os.remove(f)
            with open("./out/results/specificity.csv","w+") as f:
                f.write("Thread,Mutation,Specificity\n")    

    else:
        p_pol_size = int(sys.argv[1])
    
    if "-t" in sys.argv:
        print "Running in test mode: no graph anonymisation after computing sequences."
        TEST = True

    # for nb_th in range(0,NB_MUT_THREADS):
    #     open('./out/policies/thr'+str(nb_th)+'.txt', 'w').close()
    #     for nb_mut in range(0, NB_MUTATIONS):
    #         open('./out/ops/thr'+str(nb_th)+'_mut'+str(nb_mut)+'.txt', 'w').close()

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

    # Creating random seed...
    seed = 12345 # random.randrange(sys.maxsize)
    random.seed(seed)

    for _ in range(0, NB_EXPERIMENTS):

        if DEMO:
            p_pol = Policy([workload[0], workload[1]], "P")
            p_pol_nums = [0, 1]
        elif DEMO_TXT:
            p_pol = Policy([workload[i] for i in (range(p_pol_size))], "P")
            p_pol_nums = range(p_pol_size-1)
        else:

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

        if EXP:
            for q in p_pol.queries:
                print q
                q.get_consts(custom_prefixes(), NB_MUT_THREADS * NB_MUTATIONS)

        past_mutations = []
        consts = []

        if not SKIP:
            for th in range(0,NB_MUT_THREADS):
                print "Mutation thread number %d..." % th
                mutated_p = copy.deepcopy(p_pol)
                if consts != []:
                    # Updating constants already used at each turn
                    # Except on the first iteration
                    for ind_q in range(0,len(mutated_p.queries)):
                        mutated_p.queries[ind_q].const_res_set = consts[ind_q]


                mutation_nb = 0
                if EXP:
                    print "Computing original specificity..."
                    mutated_p.get_specificity(th,mutation_nb)

                while (mutated_p != None and mutation_nb < NB_MUTATIONS):
                    if not TEST and not DEMO:
                        print "\t\tChosen privacy queries: " + str(p_pol_nums)
                    p_size = 0
                    for i in range(0, p_pol_size):
                        p_size += len(mutated_p.queries[i].where)
                        print "\t\t" + str(mutated_p.queries[i])

                    # Run algorithm
                    print "\tComputing candidate operations..."
                    o = find_safe_ops(mutated_p, SAMEAS)
                    #print "Set of operations found:"
                    #print(o)
                    
                    # Writing operations to result files
                    with open('./out/ops/thr'+str(th)+'_mut'+str(mutation_nb)+'.txt', 'w+') as outfile:
                        outfile.write(str(o))
                    
                    #with open('./out/stats_mut'+str(mutation_nb)+'.txt', 'w+') as outfile:
                    #   outfile.write(str(len(o))+"\n")

                    with open('./out/policies/thr'+str(th)+'.txt', 'a+') as outfile:
                            outfile.write(str(mutated_p)+"\n")

                    if EXP:
                        # Mutating policy
                        past_mutations.append(copy.deepcopy(mutated_p))
                        print "Old policy: " + str(mutated_p)
                        tries = 0   # If after a certain number of randomly generated mutations we can't find a new policy mutation, we stop
                        while (mutated_p in past_mutations and tries < 20):
                            print "Mutation try number %d..." % tries
                            consts = mutated_p.mutate_policy()
                            tries += 1
                            if mutated_p in past_mutations:
                                print "Missed try: this mutation was already considered earlier"
                            
                        if tries == 20:
                            print "Too many unsuccessful mutation tries. Exiting mutation computing..."
                            break

                        mutation_nb += 1
                        print "Computing specificity..."
                        mutated_p.get_specificity(th,mutation_nb)
                    else:
                        break

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
                        g.serialize(destination='./out/output_anonymized_step'+str(seq_step)+'_'+time.strftime("%Y%m%d-%H%M%S")+'.ttl', format='trig')
                    print "\tLength after deletion: " + str(len(g)) + " triples"
                else:
                    print "Terminating program..."

        # Stat calculation
        if EXP:
            print "Launching experimental evaluation..."
            run_eval(NB_MUT_THREADS, NB_MUTATIONS,True,True)

if __name__ == "__main__":
    main()
