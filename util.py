"""Utility methods."""
import sys
import os
from unification import var

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
    o_str = t.split(" ")[2]
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