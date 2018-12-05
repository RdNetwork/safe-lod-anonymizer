"""Methods for computing anonymization"""
from operation import Operation
from policy import Policy
from unification import unify,var,variable
from util import decompose_triple, replace_blank

def find_safe_ops(privacy_pol, sameas):
    """
        Computes the sequence of operations to create a safe anonymization.
    
        :input privacy_pol: A privacy policy (set of queris) indicating sensibile data
        :input sameas: A boolean flag to ignore or not explicit sameAs prevention
        :requires: privacy_pol.queries.len >= 1
    """
    ops = []
    for q in privacy_pol.queries:
        print "\tPrivacy policy query found..."
        ind_l = {}
        ind_v = {}
        for c in q.where:
            b_index = 0
            (s,p,o) = decompose_triple(c)
            if (type(s) == variable.Var) or (':' in s):
                if (type(s) == variable.Var):
                    s = str(s)[1:]
                if not s in ind_v:
                    ind_v[s] = set()
                ind_v[s].add(c)
            if (type(p) == variable.Var) or (':' in p):
                if (type(p) == variable.Var):
                    p = str(p)[1:]
                if not p in ind_v:
                    ind_v[p] = set()
                ind_v[p].add(c)
            if (type(o) == variable.Var) or (':' in o):
                if (type(o) == variable.Var):
                    o = str(o)[1:]
                if not o in ind_v:
                    ind_v[o] = set()
                ind_v[o].add(c)
            else:
                if not o in ind_l:
                    ind_l[o] = set()
                ind_l[o].add(c)
        # First operation: critical vars and IRIs replaced by blanks
        v_crit = set([v[1:] for v in q.select])
        for v,g in ind_v.iteritems():
            if len(g) > 1:
                v_crit.add(v)
        if v_crit:
            print("Critical variables and IRIs: " + str(v_crit))
        g_prime = q.where
        g_sec = q.where
        b_index = 0
        v_index = 0
        for v in v_crit:
            if not ":" in v:
                v = '?' + v
            print "VARIABLE " + v
            if sameas:
                g_prime_post = []
                g_sec_post = []
                for i_t in range(len(q.where)):
                    t = q.where[i_t]
                    t_prime = g_prime[i_t]
                    t_sec = g_sec[i_t]
                    # For G' : replace v by a fresh unique blank in t
                    t_int = t_prime.replace(v+" ","[] ")
                    g_prime_post.append(t_int.replace(" "+v," []"))
                    # For G'' : replace v by a fresh var in t
                    t_int = t_sec.replace(v+" ","?var"+str(v_index)+" ")
                    v_index += 1
                    g_sec_post.append(t_int.replace(" "+v," ?var"+str(v_index)))
                    v_index += 1
                g_prime = g_prime_post
                g_sec = g_sec_post
            else:
                g_prime_post = []
                for t in g_prime:
                    # Replace v by blank in t
                    t_int = t.replace(v+" ","_:b"+str(b_index)+" ")
                    g_prime_post.append(t_int.replace(" "+v," _:b"+str(b_index)))
                    g_prime = g_prime_post
            b_index += 1
        if sameas:
            ops.append(Operation(g_sec, g_prime, g_sec))
        else:
            ops.append(Operation(q.where, g_prime, q.where))
        # Second operation: triples with critical literals deleted
        g_prime = []
        l_crit = set()
        for l,g in ind_l.iteritems():
            if len(g) > 1:
                l_crit.add(l)  
        if l_crit:
            print("Critical literals: " + str(l_crit))  
        for l in l_crit:
            for t in ind_l[l]:
                (s_l,p_l,_) = decompose_triple(t)
                if s_l not in v_crit and p_l not in v_crit:
                    g_prime.append(t)
        if g_prime:
            ops.append(Operation(g_prime, None, g_prime))
        # Third operation : boolean query case
        if len(q.select) == 0:
            # case of a boolean query: pick the first triple and delete it
            ops.append(Operation([c], None, q.where))
    return ops

def find_safe_ops_sameas(privacy_pol):
    """
        Computes the sequence of operations to create a safe anonymization,
        modulo sameAs inference.
    
        :requires: privacy_pol.queries.len >= 1
    """
    ops = []
    for q in privacy_pol.queries:
        print "\tPrivacy policy query found..."
        ind_l = {}
        ind_v = {}
        for c in q.where:
            b_index = 0
            (s,p,o) = decompose_triple(c)
            if (type(s) == variable.Var) or (':' in s):
                if (type(s) == variable.Var):
                    s = str(s)[1:]
                if not s in ind_v:
                    ind_v[s] = set()
                ind_v[s].add(c)
            if (type(p) == variable.Var) or (':' in p):
                if (type(p) == variable.Var):
                    p = str(p)[1:]
                if not p in ind_v:
                    ind_v[p] = set()
                ind_v[p].add(c)
            if (type(o) == variable.Var) or (':' in o):
                if (type(o) == variable.Var):
                    o = str(o)[1:]
                if not o in ind_v:
                    ind_v[o] = set()
                ind_v[o].add(c)
            else:
                if not o in ind_l:
                    ind_l[o] = set()
                ind_l[o].add(c)
        # First operation: critical vars and IRIs replaced by blanks
        v_crit = set([v[1:] for v in q.select])
        for v,g in ind_v.iteritems():
            if len(g) > 1:
                v_crit.add(v)
        if v_crit:
            print("Critical variables and IRIs: " + str(v_crit))
        g_prime = q.where
        for v in v_crit:
            if not ":" in v:
                v = '?' + v
            g_prime_post = []
            for t in g_prime:
                # print("Replace "+v+" by blank in "+t)
                t_int = t.replace(v+" ","_:b"+str(b_index)+" ")
                g_prime_post.append(t_int.replace(" "+v," _:b"+str(b_index)))
            b_index += 1
            g_prime = g_prime_post
        ops.append(Operation(q.where, g_prime, q.where))
        # Second operation: triples with critical literals deleted
        g_prime = []
        l_crit = set()
        for l,g in ind_l.iteritems():
            if len(g) > 1:
                l_crit.add(l)  
        if l_crit:
            print("Critical literals: " + str(l_crit))  
        for l in l_crit:
            for t in ind_l[l]:
                (s_l,p_l,_) = decompose_triple(t)
                if s_l not in v_crit and p_l not in v_crit:
                    g_prime.append(t)
        if g_prime:
            ops.append(Operation(g_prime, None, g_prime))
        # Third operation : boolean query case
        if len(q.select) == 0:
            # case of a boolean query: pick the first triple and delete it
            ops.append(Operation([c], None, q.where))
    return ops   