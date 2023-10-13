import pymongo
from read_data import nodes_df, edges_df
import json
import time

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["mydatabase"]
from json_impl import find_connections
# mycol = mydb["customers"]

# mydict = { "name": "John", "address": "Highway 37" }

# x = mycol.insert_one(mydict)

# print(mydb.list_collection_names())
# print(myclient.list_database_names())
def create_nodes_mongodb():
    nodes = mydb["nodes"]
    for index, row in nodes_df.iterrows():

        ID = row["id"]
        node_name = row["name"]
        kind = row["kind"]
        curr_node = {"_id":ID,"name":node_name,"kind":kind}
        nodes.insert_one(curr_node)

def get_disease_relations_mongodb():
    connections = mydb["diseases"]
    json_con = find_connections(edges_df)
    for disease_id in json_con.keys():
        dis_dict = json_con[disease_id]
        dis_dict["_id"] = disease_id
        connections.insert_one(dis_dict)

def get_disease_info(disease_id):
    nodes_db = mydb["nodes"]
    query = {"_id": disease_id}

    disease_info_dict = {}
    disease_info_dict["Disease_ID"] = disease_id
    disease_info_dict["Disease_Name"] = nodes_db.find_one(query)["name"]
    disease_info_dict["TR"] = []
    disease_info_dict["PA"] = []
    disease_info_dict["CA"] = []
    disease_info_dict["AN"] = []

    diseases_db = mydb["diseases"]
    
    disease_collection = diseases_db.find_one(query)
    if(not disease_collection):
        return
    for compound in disease_collection["CtD"]:
        compound_query = {"_id":compound}
        disease_info_dict["TR"].append(nodes_db.find_one(compound_query)["name"])

    for compound in disease_collection["CpD"]:
        compound_query = {"_id":compound}
        disease_info_dict["PA"].append(nodes_db.find_one(compound_query)["name"])

    for gene in disease_collection["DaG"]:
        gene_query = {"_id":gene}
        disease_info_dict["CA"].append(nodes_db.find_one(gene_query)["name"])

    for anatomy in disease_collection["DlA"]:
        anatomy_query = {"_id":anatomy}
        disease_info_dict["AN"].append(nodes_db.find_one(anatomy_query)["name"])
    return disease_info_dict



if __name__ == "__main__":
    #create_nodes_mongodb()
    #nodes_db = mydb["nodes"]
    # for x in nodes_db.find():
    #     print(x)

    #get_disease_relations_mongodb()
    # diseases_db = mydb["diseases"]
    # for x in diseases_db.find():
    #     print(x)
    diseases_list = []
    for index,row in nodes_df.iterrows():
        if row["kind"] == "Disease":
            diseases_list.append(row["id"])

    start = time.time()
    for disease_id in diseases_list:
        get_disease_info(disease_id)
    elapses = time.time() - start
    print(elapses)
    # print(json.dumps(get_disease_info("Disease::DOID:0050741"), indent=4))