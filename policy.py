from query import Query
from util import ConfigSectionMap
import SPARQLWrapper
import random

ENDPOINT=ConfigSectionMap("Endpoint")['url']
OLD_GRAPH=ConfigSectionMap("Graph")['uri']+ConfigSectionMap("Graph")['name']+"/"

class Policy():
    """Class for handling Policy objects"""

    def __init__(self, queries, privacy_type):
        self.queries = queries
        self.privacy_type = privacy_type

    def __eq__(self, other):
        return (self.privacy_type == other.privacy_type) and (self.queries == other.queries)
        
    def policy_sat(self, graph, anon_graph, prefixes):
        """Check if a policy is satisfied on a given graph."""
        for q in self.queries:
            res = q.evaluate(anon_graph, prefixes)
            if self.privacy_type == 'P':
                if res.len > 0:
                    return False
            else:
                prev_res = q.evaluate(graph, prefixes)
                if prev_res != res:
                    return False

        return True

    def __str__(self):
        return "[\n\t" + "\n\t".join([str(q) for q in self.queries]) + "\n]"

    def get_selectivity(self, num_thr, num_mut):
        sparql = SPARQLWrapper.SPARQLWrapper(ENDPOINT)
        with open("./out/results/selectivity.csv","a+") as f:
            # Compute mutated policy's selectivity on the original graph   
            nb_res = 0
            for q in self.queries:
                q_str = "DEFINE sql:log-enable 3 WITH <"+OLD_GRAPH+"> " + q.str_count()
                sparql.setQuery(q_str)
                sparql.setReturnFormat(SPARQLWrapper.JSON)
                sparql.queryType = SPARQLWrapper.SELECT
                results = sparql.query().convert()
                nb_res += int(results["results"]["bindings"][0]["callret-0"]["value"])
            f.write(str(num_thr)+','+str(num_mut)+','+str(nb_res)+"\n")

    def mutate_policy(self):
        """Mutation launcher method"""
        # Random query
        query = self.queries[random.randint(0, len(self.queries)-1)]
        # Random mutation type
        mutation_types = ["SPECIFY_VAR"]    #["DELETE_QUERY","SPECIFY_VAR","UNIFY_VAR"]
        mutation_type = mutation_types[0]   #en attendant mieux

        if (mutation_type == "DELETE_QUERY"):
            self.queries.remove(query)
        elif (mutation_type == "SPECIFY_VAR"):
            print "Old query: " + str(query)
            old_index = self.queries.index(query)
            query.var_to_const()
            print "New query: " + str(query)
            self.queries[old_index] = query
        elif (mutation_type == "UNIFY_VAR"):
            #TODO
            return None
        else:
            return None

        print "New policy "+ str(self)

        consts = []
        for q in self.queries:
            consts.append(q.const_res_set)

        return consts