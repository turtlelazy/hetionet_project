from py2neo import Graph, Node, Relationship, NodeMatcher, Transaction
from py2neo.bulk import create_relationships, create_nodes
from read_data import nodes_df, edges_df
import json
import time
graph = Graph("bolt://localhost:7687", auth=("neo4j", "543543is"))

def create_hetio_nodes(nodes_df,graph):
    matcher = NodeMatcher(graph)
    keys = ["ID","node_name","kind"]
    nodes_batch = []
    for index, row in nodes_df.iterrows():

        ID = row["id"]
        node_name = row["name"]
        kind = row["kind"]
        # Find the Hetio node by its label and property
        hetio_node = matcher.match("HetioNode", ID=ID).first()
        # Check if the node exists, and if not, create it
        if hetio_node is None:
            # hetio_node = Node("HetioNode", ID=ID,node_name=node_name,kind=kind)
            nodes_batch.append([ID,node_name,kind])
        if (index + 1) % 5000 == 0:
            create_nodes(graph.auto(), nodes_batch, labels={"HetioNode"}, keys=keys)
            nodes_batch.clear()
            #print(index)

    create_nodes(graph.auto(), nodes_batch, labels={"HetioNode"}, keys=keys)
    nodes_batch.clear()


def create_edges(edges_df,graph):
    relationships_batch = []
    matcher = NodeMatcher(graph)
    broken_set = []
    #start = time.time()
    start_index = 0
    for index, row in edges_df.iterrows():
        if start_index > index:
            continue
        
        source = row["ource"]
        relation = row["metaedge"]
        action = "TARGETS"
        target = row["target"]

        hetio_source = matcher.match("HetioNode", ID=source).first()
        hetio_target = matcher.match("HetioNode", ID=target).first()

        #Query to check for existing relationship
        if not hetio_source or not hetio_target:
            print(source,target)
            continue

        # query = (
        #     f"MATCH (n1)-[r:{action}]->(n2) "
        #     f"WHERE ID(n1) = {hetio_source.identity} AND ID(n2) = {hetio_target.identity} "
        #     "RETURN r"
        # )
        # result = graph.run(query)
        # existing_relationship = result.data()
        # if not existing_relationship:
        relationships_batch.append([source, {"edge":relation}, target])
        #print(index)

        if (index + 1) % 50000 == 0:
            create_relationships(graph.auto(), relationships_batch, "TARGETS", start_node_key=("HetioNode", "ID"), end_node_key=("HetioNode", "ID"))
            relationships_batch.clear()
            # elapsed = time.time() - start
            #print(f"Reached index {index}: Took {elapsed} seconds from last batch")
            # start = time.time()

    create_relationships(graph.auto(), relationships_batch, "TARGETS", start_node_key=("HetioNode", "ID"), end_node_key=("HetioNode", "ID"))
    # Execute the batch operations

def disease_info(disease_id,graph):
    matcher = NodeMatcher(graph)

    disease_node = matcher.match("HetioNode", ID=disease_id).first()
    disease_name = disease_node.get("node_name")
    #print(disease_name)
    # query_treats = (
    #     f"MATCH (sourceNode)-[r:TARGETS]->(targetNode) "
    #     f"WHERE (r.edge = 'CtD' OR r.edge = 'CpD') AND targetNode.ID = '{disease_id}'"
    #     "RETURN sourceNode"
    # )
    query_treats = (
        f"MATCH (sourceNode)-[r:TARGETS {{edge: 'CtD'}}]->(targetNode) "
        f"WHERE targetNode.ID = '{disease_id}' "
        "RETURN sourceNode"
    )
    query_palliates = (
        f"MATCH (sourceNode)-[r:TARGETS {{edge: 'CpD'}}]->(targetNode) "
        f"WHERE targetNode.ID = '{disease_id}' "
        "RETURN sourceNode"
    )
    query_gene = (
        f"MATCH (diseaseNode)-[r:TARGETS {{edge: 'DaG'}}]->(geneNode)"
        f"WHERE diseaseNode.ID = '{disease_id}'"
        "RETURN geneNode"
    )
    query_anatomy = (
        f"MATCH (diseaseNode)-[r:TARGETS {{edge: 'DlA'}}]->(anatomyNode)"
        f"WHERE diseaseNode.ID = '{disease_id}'"
        "RETURN anatomyNode"
    )
    treats = graph.run(query_treats)
    palliates = graph.run(query_palliates)
    causes = graph.run(query_gene)
    anatomies = graph.run(query_anatomy)

    disease_info_dict = {}
    disease_info_dict["Disease_ID"] = disease_id

    disease_info_dict["Disease_Name"] = disease_name
    disease_info_dict["TR"] = []
    disease_info_dict["PA"] = []
    disease_info_dict["CA"] = []
    disease_info_dict["AN"] = []

    for compound in treats:
        compound = compound[0]["node_name"]
        disease_info_dict["TR"].append(compound)
        #print(compound["node_name"])

    for compound in palliates:
        compound = compound[0]["node_name"]
        disease_info_dict["PA"].append(compound)

        #print(compound["node_name"])

    for gene in causes:
        gene = gene[0]["node_name"]
        disease_info_dict["CA"].append(gene)

        #print(gene["node_name"])
    
    for anatomy in anatomies:
        anatomy = anatomy[0]["node_name"]
        disease_info_dict["AN"].append(anatomy)

        #print(anatomy["node_name"])
    

    return disease_info_dict

def find_compounds(graph):
    query_compound = (
        f"MATCH (compoundNode)-[:TARGETS {{edge: 'CdG'}}]->(commonTarget)<-[:TARGETS {{edge: 'AuG'}}]-(anatomyNode)"
        f"MATCH (diseaseNode)-[r:TARGETS {{edge: 'DlA'}}]->(anatomyNode)"
        f"RETURN compoundNode.ID, diseaseNode.ID"
        f" UNION"
        f" MATCH (compoundNode)-[:TARGETS {{edge: 'CuG'}}]->(commonTarget)<-[:TARGETS {{edge: 'AdG'}}]-(anatomyNode)"
        f"MATCH (diseaseNode)-[r:TARGETS {{edge: 'DlA'}}]->(anatomyNode)"
        f"RETURN compoundNode.ID, diseaseNode.ID"
        #f" LIMIT 10"
    )
    # query_compound_up = (
    #     f"MATCH (compoundNode)-[:TARGETS {{edge: 'CuG'}}]->(commonTarget)<-[:TARGETS {{edge: 'AdG'}}]-(anatomyNode)"
    #     f"WHERE compoundNode.ID = '{compound_id}'"
    #     f"MATCH (diseaseNode)-[r:TARGETS {{edge: 'DlA'}}]->(anatomyNode)"
    #     f"RETURN DISTINCT compoundNode.ID, diseaseNode.ID"
    #     f" LIMIT 10"
    # )
    #print(query_compound_down)
    results = graph.run(query_compound)
    #result_up = graph.run(query_compound_up)
    # for result in results:
    #     print(f"{result[0]} -> {result[1]}")
    return results
    #print(results)
def find_single_compound(compound_id,graph):
    query_compound = (
        f"MATCH (compoundNode)-[:TARGETS {{edge: 'CdG'}}]->(commonTarget)<-[:TARGETS {{edge: 'AuG'}}]-(anatomyNode)"
        f"WHERE compoundNode.ID = '{compound_id}' "
        f"MATCH (diseaseNode)-[r:TARGETS {{edge: 'DlA'}}]->(anatomyNode)"
        f"RETURN compoundNode.ID, diseaseNode.ID"
        f" UNION"
        f" MATCH (compoundNode)-[:TARGETS {{edge: 'CuG'}}]->(commonTarget)<-[:TARGETS {{edge: 'AdG'}}]-(anatomyNode)"
        f"WHERE compoundNode.ID = '{compound_id}' "
        f"MATCH (diseaseNode)-[r:TARGETS {{edge: 'DlA'}}]->(anatomyNode)"
        f"RETURN compoundNode.ID, diseaseNode.ID"
    )
    results = graph.run(query_compound)
    return results
def get_node(node_id,graph):
    query = (
        f"MATCH (node) "
        f"WHERE node.ID = '{node_id}' "
        f"RETURN node"
    )
    return graph.run(query).evaluate()

if __name__ == "__main__":
    diseases_list = []
    # for index,row in nodes_df.iterrows():
    #     if row["kind"] == "Disease":
    #         diseases_list.append(row["id"])

    # start = time.time()
    # for disease_id in diseases_list:
    #     disease_info(disease_id,graph)
    # elapses = time.time() - start

    # print(elapses)

    #print(find_compounds(graph))
    #print(find_single_compound("Compound::DB04865",graph))
    # x = disease_info("Disease::DOID:0050741",graph)
    # print(json.dumps(x, indent=4))
    print(get_node("Compound::DB04865",graph).get("node_name"))
