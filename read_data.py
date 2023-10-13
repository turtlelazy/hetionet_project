import pandas as pd

nodes_file = "test/nodes_test.tsv"
edges_file = "test/edges_test.tsv"

nodes_df = pd.read_csv(nodes_file,delimiter='\t')
edges_df = pd.read_csv(edges_file, delimiter='\t')

if __name__ == "__main__":
    for index, row in edges_df.iterrows():
        source = row["ource"]
        relation = row["metaedge"]
        target = row["target"]
        print(source,relation,target)