from query import Query

class Policy():
    """Class for handling Policy objects"""

    def __init__(self, queries, privacy_type):
        self.queries = queries
        self.privacy_type = privacy_type

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

