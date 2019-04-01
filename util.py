"""Utility methods."""
import sys
import os
import itertools
import random
import copy
import ConfigParser
from prefix import Prefix
from unification import var, Var


def ConfigSectionMap(section):
    """From https://wiki.python.org/moin/ConfigParserExamples"""
    Config = ConfigParser.ConfigParser()
    Config.read("CONFIG.ini")
    dict1 = {}
    options = Config.options(section)
    for option in options:
        try:
            dict1[option] = Config.get(section, option)
            if dict1[option] == -1:
                print "skip: %s" % option
        except:
            print("exception on %s!" % option)
            dict1[option] = None
    return dict1


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
    p.append(Prefix("datex", "http://vocab.datex.org/terms#"))
    p.append(Prefix("ub", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#"))
    return p

def block_print():
    """Disable printing for faster display"""
    sys.stdout = open(os.devnull, 'w')

def enable_print():
    """Enable print for debugging"""
    sys.stdout = sys.__stdout__

def str_to_var(s):
    """Transform a textual RDF variable to a 'unification' Var"""
    return var(s[1:])

def var_to_str(v):
    """Transform an 'unification' Var object to an RDF variable string"""
    return '?'+str(v)[1:]

def decompose_triple(t):
    """Extract the three parts of an RDF triple string"""
    s_str = t.split(" ")[0]
    p_str = t.split(" ")[1]
    o_str = t.split(" ")[2] # !!! doesn't work if string literal with spaces
    if s_str[0] == '?':
        s = str_to_var(s_str)
    else:
        s = s_str
    if p_str[0] == '?':
        p = str_to_var(p_str)
    else:
        p = p_str
    if o_str[0] == '?':
        o = str_to_var(o_str)
    else:
        o = o_str

    return (s,p,o)

def triple_to_string(t):
    (s,p,o) = t
    res = ""
    if type(s) == Var:
        res += var_to_str(s) + " "
    else:
        res += str(s) + " "
    if type(p) == Var:
        res += var_to_str(p) + " "
    else:
        res += str(p) + " "
    if type(o) == Var:
        res += var_to_str(o) + " ."
    else:
        res += str(o) + " ."

    return res

def replace_blank(t, ind):
    """Replace one element of an RDF triple string by a blank node"""
    t_tab = t.split(" ")
    t_tab[ind] = ("[]")
    return " ".join(t_tab)
    
def average_wl_size(workload):
    """Compute (integer) average size (in triples) of a query workload"""
    return int(sum(len(query.where) for query in workload) / float(len(workload)))


def get_all_connected_groups(graph):
    """From https://stackoverflow.com/a/50639220"""
    already_seen = set()
    result = []
    for node in graph:
        if node not in already_seen:
            connected_group, already_seen = get_connected_group(graph, node, already_seen)
            result.append(connected_group)
    return result

def get_connected_group(graph, node, already_seen):
    """From https://stackoverflow.com/a/50639220"""
    result = []
    nodes = set([node])
    while nodes:
        node = nodes.pop()
        already_seen.add(node)
        if node in graph:
            nodes.update(graph[node] - already_seen)
        result.append(node)
    return result, already_seen

def powerset(iterable):
    """powerset([1,2,3]) --> () (1,) (2,) (3,) (1,2) (1,3) (2,3) (1,2,3)"""
    s = list(iterable)
    return itertools.chain.from_iterable(itertools.combinations(s, r) for r in range(1,len(s)+1))


def generate_const(triple, query):
    """Return a triple updated where a constant replaces a variable in the given triple."""
    (s,p,o) = decompose_triple(triple)
    t_s = None
    t_p = None
    t_o = None

    consts = query.const_res_set
    
    print "Possible variable results:" + str(consts.keys())
    if (type(s) == Var):
        str_s = var_to_str(s)[1:]
        if str_s in consts.keys():
            ind_const = random.randint(0,len(consts[str_s])-1)
            res_s = consts[str_s][ind_const]
            if ":" in res_s:
                res_s = "<" + res_s + ">"
            t_s = (res_s,p,o)
    if (type(p) == Var):
        str_p = var_to_str(p)[1:]
        if str_p in consts.keys():
            ind_const = random.randint(0,len(consts[str_p])-1)
            res_p = consts[str_p][ind_const]
            if ":" in res_p:
                res_p = "<" + res_p + ">"
            t_p = (s,res_p,o)
    if (type(o) == Var):
        str_o = var_to_str(o)[1:]
        if str_o in consts.keys():
            print consts[str_o]
            ind_const = random.randint(0,len(consts[str_o])-1)
            res_o = consts[str_o][ind_const]
            print type(res_o)
            if type(res_o) == unicode:
                res_o = '"' + res_o + '"'
            elif ":" in res_o:
                res_o = "<" + res_o + ">"
             
            t_o = (s,p,res_o)   

    if (t_s, t_p, t_o) == (None,None,None):
        return None
    else:
        if t_s:
            return triple_to_string(t_s)
        elif t_p:
            return triple_to_string(t_p)
        else:
            return triple_to_string(t_o)
