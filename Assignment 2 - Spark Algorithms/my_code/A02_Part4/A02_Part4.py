# --------------------------------------------------------
#
# PYTHON PROGRAM DEFINITION
#
# The knowledge a computer has of Python can be specified in 3 levels:
# (1) Prelude knowledge --> The computer has it by default.
# (2) Borrowed knowledge --> The computer gets this knowledge from 3rd party libraries defined by others
#                            (but imported by us in this program).
# (3) Generated knowledge --> The computer gets this knowledge from the new functions defined by us in this program.
#
# When launching in a terminal the command:
# user:~$ python3 this_file.py
# our computer first processes this PYTHON PROGRAM DEFINITION section of the file.
# On it, our computer enhances its Python knowledge from levels (2) and (3) with the imports and new functions
# defined in the program. However, it still does not execute anything.
#
# --------------------------------------------------------

# ------------------------------------------------
# IMPORTS
# ------------------------------------------------
import codecs
import functools
import os

import pyspark
import pyspark.sql.functions

# ------------------------------------------------
# FUNCTION read_graph_from_folder
# ------------------------------------------------
def read_graph_from_folder(my_dataset_dir):

    res = ()

    num_nodes = 0

    edges_per_node = {}

    list_of_files = os.listdir(my_dataset_dir)
    if ('.DS_Store' in list_of_files):
        list_of_files.remove('.DS_Store')
    list_of_files.sort()

    for file in list_of_files:
        my_input_stream = codecs.open(my_dataset_dir + file, "r", encoding='utf-8')

        for line in my_input_stream:
            (source_node, target_node, _) = tuple(line.strip().split(" "))


            for node_name in [source_node, target_node]:
                if node_name not in edges_per_node:

                    edges_per_node[node_name] = []
                    num_nodes += 1

            edges_per_node[source_node].append( target_node )

        my_input_stream.close()

    for node_name in edges_per_node:
        neighbour_list = edges_per_node[node_name]
        edges_per_node[node_name] = (len(neighbour_list), neighbour_list)

    res = (num_nodes, edges_per_node)

    return res

# ------------------------------------------
# FUNCTION compute_page_rank
# ------------------------------------------
def compute_page_rank(edges_per_node, reset_probability, max_iterations):

    # ------------------------------------------------
    # START OF YOUR CODE:
    # ------------------------------------------------

    # Remember that the function must return a dictionary with:
    # Key => The node number
    # Value => The PageRank value computed for this node.

    # Type all your code here.
    pass
  
    print("Edges per node: ", edges_per_node)
    print("Reset Probability: ", reset_probability)
    print("Edges per node: ", max_iterations)

    # ------------------------------------------------
    # END OF YOUR CODE
    # ------------------------------------------------

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(my_dataset_dir, reset_probability, max_iterations):
    # 1. We read the graph from the file
    (num_nodes, edges_per_node) = read_graph_from_folder(my_dataset_dir)

    # 2. We compute the shortest paths to each node
    page_rank_per_node = compute_page_rank(edges_per_node, reset_probability, max_iterations)

    # 3. We sort the nodes in decreasing order in their rank
    rank_per_node = [ (round(page_rank_per_node[node], 2), node) for node in page_rank_per_node ]
    rank_per_node.sort(reverse=True)

    # 4. We print them
    for item in rank_per_node:
        print("id=" + str(item[1]) + "; pagerank=" + str(item[0]))

# --------------------------------------------------------
#
# PYTHON PROGRAM EXECUTION
#
# Once our computer has finished processing the PYTHON PROGRAM DEFINITION section its knowledge is set.
# Now its time to apply this knowledge.
#
# When launching in a terminal the command:
# user:~$ python3 this_file.py
# our computer finally processes this PYTHON PROGRAM EXECUTION section, which:
# (i) Specifies the function F to be executed.
# (ii) Define any input parameter such this function F has to be called with.
#
# --------------------------------------------------------
if __name__ == '__main__':
    # 1. We get the input values
    reset_probability = 0.15
    max_iterations = 3

    # 2. Local or Databricks
    local_False_databricks_True = False

    # 3. We set the path to my_dataset and my_result
    my_local_path = "../../../../3_Code_Examples/L15-25_Spark_Environment/"
    my_databricks_path = "/"

    my_dataset_dir = "FileStore/tables/6_Assignments/my_dataset_2/"

    if local_False_databricks_True == False:
        my_dataset_dir = my_local_path + my_dataset_dir
    else:
        my_dataset_dir = my_databricks_path + my_dataset_dir

    # 3. We call to my_main
    my_main(my_dataset_dir, reset_probability, max_iterations)
