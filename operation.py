import re
from prefix import Prefix

class Operation(object):
    """Anonymisation operations class and methods."""

    def __init__(self, del_head, upd_head, body, filter=""):
        self.del_head = del_head
        self.upd_head = upd_head
        self.body = body
        self.filter = filter

    def update(self, graph, prefixes):
        """
            Performs a SPARQL update operation for a the given parameters.
            :param self: the Operation object used to get parameters
            :param graph: Graph to be edited

            :return: The updated graph
        """
        return graph.update(Prefix.writePrefixes(prefixes, "SPARQL") + str(self))

    def __str__(self):
        res = "\n\tDELETE { " + ' '.join(self.del_head)  + "} \n"
        if self.upd_head:
            res = res + "\tINSERT { " + ' '.join(self.upd_head)  + "} \n "
        res = res + "\tWHERE { " + ' '.join(self.body)
        if self.filter:
            res = res + "\n\t\t FILTER (" + self.filter  + ")"
        res = res + " }"
        return res

    def count(self):
        res = "\n\t SELECT COUNT * \n"
        res = res + "\tWHERE { " + ' '.join(self.body)
        if self.filter:
            res = res + "\n\t\t FILTER (" + self.filter  + ")"
        res = res + " }"
        return res

    def __repr__(self):
        return "\t\t" + self.__str__() + "\n"

    def __eq__(self, other):
        return (self.del_head == other.del_head and 
            self.upd_head == other.upd_head and 
            self.body == other.body and
            self.filter == other.filter)
