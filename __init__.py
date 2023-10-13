from neo4jtest2 import *
import time
import json
# Create the graph of nodes

# start = time.time()
# create_hetio_nodes(nodes_df,graph)
# elapsed = time.time() - start
# print(f"Create Nodes: {elapsed} seconds")

# Create the relationships

# start = time.time()
# create_edges(edges_df,graph)
# elapsed = time.time() - start
# print(f"Create Relationships: {elapsed} seconds")

#x = disease_info("Disease::DOID:263",graph)
# for compound in x["TR"]:
#     print(compound)
#print(json.dumps(x, indent=4))

missing_edges = find_compounds(graph)
compound_dict = {}
for result in missing_edges:
    if(result[0] not in compound_dict):
        compound_dict[result[0]] = []
    #print(get_node(result[1],graph).get("node_name"))
    print(result[1])
    #compound_dict[result[0]].append(get_node(result[1],graph).get("node_name"))

print(json.dumps(compound_dict, indent=4))


    #print(f"{result[0]} -> {result[1]}")