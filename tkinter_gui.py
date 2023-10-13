import tkinter as tk
from neo4jtest2 import disease_info,find_compounds,graph, get_node
from mongo_impl import get_disease_info
import json

missing_edges = find_compounds(graph)
print("Missing edges found")

compound_dict = {}
for result in missing_edges:
    if(result[0] not in compound_dict):
        compound_dict[result[0]] = []
    #compound_dict[result[0]].append(get_node(result[1],graph).get("node_name"))
    compound_dict[result[0]].append(result[1])

print("Compound_dict created")
def disease_lookup_UI():

    window = tk.Tk()
    window.geometry('500x500')

    greeting = tk.Label(text="Disease Lookup\n")
    greeting.grid(row=0,column=0,columnspan=2)

    disease_name = tk.Label(text="Disease Name: \n")
    disease_name.grid(row=1,column=0)

    treating_compounds = tk.Label(text="Treating Compounds: \n")
    treating_compounds.grid(row=2,column=0)

    palliating_compounds = tk.Label(text="Palliating Compounds: \n")
    palliating_compounds.grid(row=2,column=1)

    genes = tk.Label(text="Associating Genes: \n")
    genes.grid(row=3,column=0)

    occurs = tk.Label(text="Occurs In: \n")
    occurs.grid(row=3,column=1)

    palliating_compounds = tk.Label(text="Palliating Compounds: \n")
    palliating_compounds.grid(row=2,column=1)

    # Create text input
    entry = tk.Entry(window)
    entry.grid(row=4,column=0)
    # Create a button

    def grab_info():
        disease_id = entry.get()
        curr_info = get_disease_info(disease_id)
        max_size = 4

        disease_name.config(text= "Disease Name: " + curr_info["Disease_Name"] + "\n")

        treatments = "Treating Compounds:\n"
        x = 0
        for treatment in curr_info["TR"]:
            if x > max_size:
                treatments += f"+ {len(curr_info['TR']) - max_size} other compounds..."
                break
            treatments += treatment + ",\n"
            x += 1
        while x < max_size:
            treatments += "\n"
            x+=1

        treating_compounds.config(text=treatments)

        palliates = "Palliating Compounds:\n"
        x = 0
        for palliate in curr_info["PA"]:
            if x > max_size:
                treatments += f"+ {len(curr_info['PA']) - max_size} other compounds..."
                break
            palliates += palliate + ",\n"
            x += 1
        while x < max_size:
            palliates += "\n"
            x+=1

        palliating_compounds.config(text=palliates)

        assoc_genes = "Associating Genes:\n"
        x = 0
        for gene in curr_info["CA"]:
            if x > max_size:
                assoc_genes += f"+ {len(curr_info['CA']) - max_size} other genes..."
                break
            assoc_genes += gene + ",\n"
            x += 1
        while x < max_size:
            assoc_genes += "\n"
            x+=1

        genes.config(text=assoc_genes)

        anatomies = "Occurs In:\n"
        x = 0
        for anatomy in curr_info["AN"]:
            if x > max_size:
                anatomies += f"+ {len(curr_info['AN']) - max_size} other genes..."
                break
            anatomies += anatomy + ",\n"
            x += 1
        while x < max_size:
            anatomies += "\n"
            x+=1
        occurs.config(text=anatomies)
    def output_file():
        disease_id = entry.get()
        curr_info = disease_info(disease_id,graph)
        grab_info()
        f = open("disease.json", "w")
        disease_json = json.dumps(curr_info, indent=4)
        f.write(disease_json)
        f.close


    button = tk.Button(window, text="Search",command=grab_info)
    button.grid(row=4,column=1)

    button_download = tk.Button(window, text="Search and Download",command=output_file)
    button_download.grid(row=5,column=1)

    window.mainloop()

def compound_lookup_UI():

    window = tk.Tk()
    window.geometry('500x500')

    greeting = tk.Label(text="Compound Lookup\n")
    greeting.grid(row=0,column=0,columnspan=2)

    compound_name = tk.Label(text="Compound Name: \n")
    compound_name.grid(row=1,column=0)

    diseases_list = tk.Label(text="Missing CtD Edges: \n")
    diseases_list.grid(row=2,column=0)

    # Create text input
    entry = tk.Entry(window)
    entry.grid(row=4,column=0)
    # Create a button

    def grab_info():

        compound_id = entry.get()
        compound_name_entry = get_node(compound_id,graph).get("node_name")
        max_size = 10

        compound_name.config(text= "Compound Name: " + compound_name_entry + "\n")

        diseases = "Can treat:\n"
        x = 0
        for disease in compound_dict[compound_id]:
            if x > max_size:
                diseases += f"+ {len(compound_dict[compound_id]) - max_size} other diseases..."
                break
            diseases += disease + ",\n"
            x += 1
        while x < max_size:
            diseases += "\n"
            x+=1
        diseases_list.config(text=diseases)

    def output_file():
        compound_id = entry.get()
        curr_info = compound_dict
        if compound_id != "":
            curr_info = compound_dict[compound_id]
        f = open("compound.json", "w")
        grab_info()
        disease_json = json.dumps(curr_info, indent=4)
        f.write(disease_json)
        f.close

    def output_all():
        curr_info = compound_dict
        f = open("compound.json", "w")
        disease_json = json.dumps(curr_info, indent=4)
        f.write(disease_json)
        f.close

    button = tk.Button(window, text="Search",command=grab_info)
    button.grid(row=4,column=1)

    button_download = tk.Button(window, text="Search and Download",command=output_file)
    button_download.grid(row=5,column=1)

    button_download_all = tk.Button(window, text="Download All",command=output_all)
    button_download_all.grid(row=6,column=1)


    window.mainloop()

if __name__ == "__main__":
    disease_lookup_UI()
    compound_lookup_UI()

