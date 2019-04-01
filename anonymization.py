"""Methods for computing anonymization"""
from operation import Operation
from policy import Policy
from unification import unify,var,variable
from util import decompose_triple, replace_blank, var_to_str, powerset

def find_safe_ops(privacy_pol, sameas):
    """
        Computes the sequence of operations to create a safe anonymization.
    
        :input privacy_pol: A privacy policy (set of queris) indicating sensible data
        :input sameas: A boolean flag to ignore or not explicit sameAs prevention
        :requires: privacy_pol.queries.len >= 1
    """
    ops = []
    for q in privacy_pol.queries:
        print "\tPrivacy policy query found..."
        (res_vars, ccomp) = q.get_connected_components()
        print "\tResult variables for each CC: " + str(res_vars)
        print "\tConnected components: " + str(ccomp)
        cc_ind = 0
        for g_c in ccomp:
            print "\t\tConnected component found in privacy policy query..."
            ind_l = {}
            ind_v = {}
            for c in g_c:
                b_index = 0
                (s,p,o) = decompose_triple(c)
                if (type(s) == variable.Var) or (':' in s):
                    if (type(s) == variable.Var):
                        s = var_to_str(s)
                    if not s in ind_v:
                        ind_v[s] = 0
                    ind_v[s] += 1
                if (type(o) == variable.Var) or (':' in o):
                    if (type(o) == variable.Var):
                        o = var_to_str(o)
                    if not o in ind_v:
                        ind_v[o] = 0
                    ind_v[o] += 1
                else:
                    if not o in ind_l:
                        ind_l[o] = set()
                    ind_l[o].add(c)
            # First operation: critical vars and IRIs replaced by blanks
            v_crit = set(res_vars[cc_ind])
            for v,g in ind_v.iteritems():
                if g > 1:
                    v_crit.add(v)
            if v_crit:
                print("\t\tCritical variables and IRIs: " + str(v_crit))
            v_index = 0
            
            #v = var_to_str(v)
            if sameas:
                for t in g_c:
                    for v in v_crit:
                        # print v
                        t_int = t.replace(v+" ","[] ")
                        t_prime = t_int.replace(" "+v," []")
                        t_sec_int = t.replace(v+" ","?var"+str(v_index)+" ")
                        t_sec = t_sec_int.replace(" "+v," ?var"+str(v_index))
                        ops.append(Operation([t_sec], [t_prime], [t_sec], "!isBlank("+v+")"))
            else:
                g_x = list(powerset(g_c))[::-1]
                for x in g_x:
                    b_index = 0
                    # print "Considered subgraph:" + str(x)
                    x_prime = x
                    x_bar_prime = set()
                    for t in x:
                        if '"' in t:
                            (_,o,_) = t.split('"') 
                            o = '"' + o + '"'
                            s = t.split(" ")[0]
                            p = t.split(" ")[1]
                        else:
                            (s,p,o,_) = t.split(" ")
                        # (s,p,o) = decompose_triple(t)
                        if s in v_crit:
                            x_bar_prime.add(s)
                        if p in v_crit:
                            x_bar_prime.add(p)
                        if o in v_crit:
                            x_bar_prime.add(o)
                    for v in x_bar_prime:
                        x_post = []
                        for t in x_prime:
                            # Replace v by blank in t
                            t_int = t.replace(v+" ","_:b"+str(b_index)+" ")
                            x_post.append(t_int.replace(" "+v," _:b"+str(b_index)))
                        x_prime = x_post
                        b_index += 1
                    res = []
                    for v in x_bar_prime:
                        res.append("!isBlank("+v+")")
                    ops.append(Operation(x, x_prime, x, " && ".join(res)))
                

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
            cc_ind += 1
    return ops