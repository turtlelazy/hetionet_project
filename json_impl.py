from read_data import nodes_df, edges_df
import json

def create_nodes_json(nodes_df):
    nodes_dict = {}
    for index, row in nodes_df.iterrows():
        ID = row["id"]
        node_name = row["name"]
        kind = row["kind"]
        nodes_dict[ID] = node_name
    return nodes_dict

    

def find_connections(edges_df):
    connections = {}
    for index, row in edges_df.iterrows():

        source = row["ource"]
        relation = row["metaedge"]
        target = row["target"]

        # Compound palliates disease
        if relation == "CpD":
            if target not in connections:
                connections[target] = {}
                connections[target]["CpD"] = []
                connections[target]["CtD"] = []
                connections[target]["DaG"] = []
                connections[target]["DlA"] = []
            connections[target]["CpD"].append(source)

        # Compound treats disease
        elif relation == "CtD":
            if target not in connections:
                connections[target] = {}
                connections[target]["CpD"] = []
                connections[target]["CtD"] = []
                connections[target]["DaG"] = []
                connections[target]["DlA"] = []
            connections[target]["CtD"].append(source)

        # Disease associates gene
        elif relation == "DaG":
            if source not in connections:
                connections[source] = {}
                connections[source]["CpD"] = []
                connections[source]["CtD"] = []
                connections[source]["DaG"] = []
                connections[source]["DlA"] = []
            connections[source]["DaG"].append(target)
        # Disease localizes anatomy
        elif relation == "DlA":
            if source not in connections:
                connections[source] = {}
                connections[source]["CpD"] = []
                connections[source]["CtD"] = []
                connections[source]["DaG"] = []
                connections[source]["DlA"] = []
            connections[source]["DlA"].append(target)
    return connections

if __name__ == "__main__":
    print(json.dumps(find_connections(edges_df), indent=4))

