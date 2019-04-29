import xml.etree.ElementTree
import fyzz
import random
import collections
from util import decompose_triple, get_connected_group, get_all_connected_groups, var_to_str, custom_prefixes, ConfigSectionMap
from unification import var, Var
from SPARQLWrapper import SPARQLWrapper, JSON
from prefix import Prefix

ENDPOINT=ConfigSectionMap("Endpoint")['url']
GRAPH_URI=ConfigSectionMap("Graph")['uri']+ConfigSectionMap("Graph")['name']+"/"

class Query(object):
    """SPARQL query handling methods"""

    def __init__(self, select, where):
        self.select = select
        self.where = where
        self.const_res_set = dict()

    def __str__(self):
        select_str = "SELECT "
        for var in self.select:
            select_str += var + " "
        return select_str + "WHERE { " + ' '.join(self.where) +  " }"

    def __eq__(self, other):
        return (self.select == other.select) and (self.where == other.where)

    def str_count(self):
        return "SELECT COUNT (*) WHERE { " + ' '.join(self.where) +  " }"       

    def evaluate(self, graph, prefixes):
        """
            Evaluates the SPARQL query for a given Query object.
            Return an iterable query result
        """
        select_str = "SELECT "
        for var in self.select:
            select_str += var + " "
        select_str += "\n"

        return graph.query(Prefix.writePrefixes(custom_prefixes(), "SPARQL") + select_str +
                           "WHERE { " + '\n'.join(self.where) + "}")


    @staticmethod
    def parse_gmark_queries(xml_file):
        """Converts an XML Gmark query node to a Query object."""
        res = []
        e = xml.etree.ElementTree.parse(xml_file).getroot()
        for q in e.findall('query'):
            query = Query([], [])
            if q.find('head') is not None:
                for v in q.find('head').findall('var'):
                    query.select.append(v.text)
            if q.find('bodies') is not None:
                for c in q.find('bodies').find('body').findall('conjunct'):
                    s = c.find('disj').find('concat').find('symbol')
                    if s.get('inverse') == 'true':
                        query.where.append(c.get('trg') + " " + s.text + " " + c.get('src')  + " .")
                    else:
                        query.where.append(c.get('src')  + " " + s.text + " " + c.get('trg')  + " .")
            res.append(query)
        return res

    @staticmethod
    def parse_txt_queries(p_pol_size):
        """Parses textual policies files to a set of queries"""
        root_path = "./conf/workloads/policies/"+ConfigSectionMap("Graph")['name']+"/"
        queries_str = []
        for i in range(1,p_pol_size+1):
            with open(root_path+'p'+str(i)+'.rq', 'r') as f:
                queries_str.append(f.read())

        queries = []
        for q_str in queries_str:
            q = Query([], [])
            fyzz_q = fyzz.parse(q_str)
            for sel in fyzz_q.selected:
                q.select.append('?'+sel.name)
            for wh in fyzz_q.where:
                wh_str = ""
                for wh_part in range(0,3):
                    print str(wh[wh_part]) + ": " + str(type(wh[wh_part]))
                    if type(wh[wh_part]) is fyzz.ast.SparqlVar:
                        wh_str += '?' + wh[wh_part].name + ' '
                    elif type(wh[wh_part]) is fyzz.ast.SparqlLiteral:
                        wh_str += wh[wh_part].value + ' '
                    elif type(wh[wh_part]) is tuple:            #for IRIs
                        if ((wh[wh_part][0] + wh[wh_part][1]) == "a"):
                            wh_str += "a "
                        else:
                            wh_str += '<' + wh[wh_part][0] + wh[wh_part][1] + '> '
                    elif type(wh[wh_part]) is str:
                        wh_str += wh[wh_part] + ' '
                wh_str += "."
                q.where.append(wh_str)
            queries.append(q)
        return queries

    def get_connected_components(self):
        graph_dic = collections.OrderedDict()
        for t in self.where:
            (s,_,o) = decompose_triple(t)
            if s not in graph_dic:
                graph_dic[s] = set() 
            if o not in graph_dic:
                graph_dic[o] = set() 
            graph_dic[s].add(o)
            graph_dic[o].add(s)

        print graph_dic
        components = get_all_connected_groups(graph_dic)
        print components
        res_components = []
        res_vars = []
        res_ind = 0
        for c in components:
            res_vars.append(set()) 
            res_components.append([]) 
            for t in self.where:
                (s,p,o) = decompose_triple(t)
                if s in c and o in c:
                    if var_to_str(s) in self.select:
                        res_vars[res_ind].add(var_to_str(s))
                    if var_to_str(p) in self.select:
                        res_vars[res_ind].add(var_to_str(p))
                    if var_to_str(o) in self.select:
                        res_vars[res_ind].add(var_to_str(o))  
                    res_components[res_ind].append(t)
            res_ind += 1

        for i in range(len(res_vars)):
            print "Vars: " + str(res_vars[i]) + " / CC : " + str(res_components[i])

        return res_vars, res_components


    def get_consts(self, prefixes, nb_mutations):
        """Get possible values for a given query."""
        print "Fetching all values for this query's variables..."

        sparql = SPARQLWrapper(ENDPOINT)

        var_set = set()
        for t in self.where:
            (s,p,o) = decompose_triple(t)
            if (type(s) == Var):
                var_set.add(s)
            if (type(p) == Var):
                var_set.add(p)
            if (type(o) == Var):
                var_set.add(o)

        print var_set
        for v in var_set:
            v = var_to_str(v)
            print "\tRunning query for "+ str(v) + "..."
            query_str = "DEFINE sql:log-enable 3 \n" + Prefix.writePrefixes(prefixes, "SPARQL") + " " \
                "SELECT "+v+", COUNT(*) AS ?nb " + \
                "FROM <"+GRAPH_URI+"> " + \
                "WHERE { " + ' '.join(self.where) + "} " + \
                "ORDER BY DESC(?nb) " + \
                "LIMIT " + str(nb_mutations)
            
            print query_str
            sparql.setQuery(query_str)
            sparql.setReturnFormat(JSON)
            results = sparql.query().convert()
            # print results
            print "\tQuery done. Storing results..."
            v = v[1:]
            self.const_res_set[v] = []
            ind = 0
            for result in results["results"]["bindings"]:
                ind += 1
                if (ind % 100 == 0):
                    print "\t" + str(ind)
                self.const_res_set[v].append(result[v]['value'])
            print "\tDone! "+str(ind)+" values found for this variable"

        # query_str = Prefix.writePrefixes(prefixes, "SPARQL") + \
        #         " SELECT * FROM <"+GRAPH_URI+"> " + \
        #         "WHERE { " + ' '.join(self.where) + "}"
        # sparql.setQuery(query_str)
        # # print query_str
        # sparql.setReturnFormat(JSON)
        # results = sparql.query().convert()
        # # print results
        # res = dict()
        # for var in self.select:
        #     var = var[1:]
        #     res[var] = []
        #     for result in results["results"]["bindings"]:
        #         res[var].append(result[var]['value'])


    def var_to_const(self):
        """Changes a variable to a constant in the given query."""
        print self.where
        cand_vars = self.const_res_set.keys()
        print "Variables that can be replaced: " + str(cand_vars)
        var_to_replace = cand_vars[random.randint(0,len(cand_vars)-1)]
        #print res_q.const_res_set
        if self.const_res_set[var_to_replace]:
            replacement_value = self.const_res_set[var_to_replace].pop()
            if "://" in replacement_value:
                replacement_value = "<" + replacement_value + ">"
            else:
                replacement_value = '"' + replacement_value + '"'
            for ind_t in range(0,len(self.where)):
                self.where[ind_t] = self.where[ind_t].replace('?'+var_to_replace, replacement_value)

            #TODO : remplacer aussi dans le select ?


        # explored_triples = []
        # while (res_q == query):
        #     t = None
        #     if set(explored_triples) == set(res_q.where):
        #         # If we can't find anything, we return the query as is (as it should be identical to the input)
        #         return res_q
        #     while ((t is None) or (t in explored_triples)):
        #         ind = random.randint(0, len(res_q.where)-1)
        #         t = res_q.where[ind]
        #     (s,p,o) = decompose_triple(t)
        #     print "Candidate triple:" + str((s,p,o))
        #     cand = [v for v in [s,p,o] if (type(v) == Var) ]
        #     print "\tCandidate variables: " + str(cand)
        #     if cand:
        #         res = t
        #         while (res == t):
        #             res = generate_const(t, query.const_res_set)
        #             if res == None:
        #                 print "\tCannot replace anything in this triple."
        #                 # Can't find a variable to be specified in this triple
        #                 explored_triples.append(t)
        #                 res = t
        #                 break
        #         print "\tNew triple: " + str(res) 
        #         res_q.where[res_q.where.index(t)] = res
        #     else:
        #         #Nothing to do with this triple
        #         explored_triples.append(t)
