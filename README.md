# Static Linked Data Anonymizer

This is a the implementation of a research project whose goal is to find ways to
anonymize Linked Open Data, notably RDF datasets. This framework anonymizes RDF
datasets before their publication in the LOD cloud and ensures **safety**, i.e.
it prevents leakages by ensuring that new results for a given set of *privacy queries*
are not created by the union with any other external dataset. This lets us use
static algorithms which do not depend on an input graph, and are efficient in
terms of runtime.  
This framework is part of submission fot the ESWC2019 conference.

This program currently works by using a previously generated query workload,
picking a fixed number of random queries in this workload to affect them in a privacy
policy (a set of queries). The code then computes a set of operations which must
performed on a graph to make it safe given the privacy constraints provided as input.
An default example with a simple, fixed privacy policy is also provided.

## Project architecture

- ```*.py``` files: framework implementation in Python.
  - ```main.py``` being the main program to run the software
- ```runtime_test.sh``` runs 20 iterations of the program with policies of cardinalty 10, and measure their runtime.
- ```plot.r```: R script used to plot graphs used in the article and the added notebook.
- ```conf``` directory: directory for configuration and input files (query workloads, test graphs, gMark configuration files).

## Setup

To reproduce the various experiments and examples from the ISWC submission, you can
execute the code yourself on our example graph schema.

### Prerequisites

This project uses Python and should work on any version of Python 3 and any
version of Python starting from Python 2.7. You must install the following Python
libraries to be able to run this program:
*[rdflib](https://github.com/RDFLib/rdflib)
*[unification](https://pypi.python.org/pypi/unification/0.2.2)
*[yapps](https://github.com/smurfix/yapps)
*[fyzz](https://pypi.org/project/fyzz/)

The query workload is created using [gMark](https://github.com/graphMark/gmark),
which uses an XML configuration file to generate graphs and queries. You can follow
the instructions on the gMark project to generate your own workload, or use the
ones provided in the ```/conf/workloads``` directory.
By default, ```/conf/workloads/starchain-workload.xml``` is used.

### Example graph configuration

For indication only, the configuration file describing the graph schema used
for the query workload generation is provied, in the file ```/conf/test.xml```.
You can reuse this file as explained in the gMark documentation to generate
your own graph and workload. This file describes a graph schema modeling a
transportation network and its users (persons, travels, lines, subscriptions),
using 12 predicates linking 13 data types, in a final graph of 20000 nodes.

## Usage

### Standard execution

To run the program and anonymize a graph, just run it as follows:

```bash
python main.py
```

**The graph anonymization feature is still a work in progress**. We advise you
perform yourself the operations we provide on your usual triple store engine.
Right now, the code uses a Turtle-formatted RDF graph, named ```graph.ttl``` in the ```/conf/graphs```
directory.

You can also use the demo mode, which is ran using a shortened workload, with 2
privacy queries and 2 utility queries used the article's examples:

```bash
python main.py -d
```

The demo mode also accepts policies written as text files. Policies have to be stored
in the ```/conf/workload/policies``` folder and named adequatly: ```p*.rq``` where * 
ranges from 1 to the number of privacy queries + 1.

To run this "textual demo mode", for example with a policy featuring two queries, run the following:

```bash
python main.py 2 -d
```

Two example policy files are provided, using a similar example to the standard
demo mode's one.

The standard execution will compute possible anonymization sequences, each one
indexed by a number. After choosing a sequence, its operations (here only deletions)
are performed on the graph, creating several output files:

- A copy file of the original graph, named ```[original graph file name]_orig.ttl```
- One output file per operation, named ```[original graph file name]_anonymized_stepX.ttl```, X being the number of the applied of the applied operation.

**/!\ WARNING:** running the program in standard mode will NOT erase previous
outputs. RDF stores files can get pretty big, so be careful!

### Running the tests

To reproduce the runtime experiment presented in the ESWC submission, you can run the
following script:

```bash
./runtime_test.sh
```

This will create a privacy policy of ten queries, randomly picked from the workload,
and compute the set of operations for this policy. This experiment is then looped
100 times. We run this test another 20 times to ensure new random seeding.