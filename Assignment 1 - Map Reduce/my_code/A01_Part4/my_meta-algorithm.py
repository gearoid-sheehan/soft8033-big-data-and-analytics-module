#!/usr/bin/python
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

# ------------------------------------------
# IMPORTS
# ------------------------------------------
import os
import shutil
import codecs
import my_mapper
import my_reducer

# ------------------------------------------
# FUNCTION my_mapper_simulation
# ------------------------------------------
def my_mapper_simulation(input_directory, output_directory, my_mapper_input_parameters):
    # 1. We create the map_simulation folder

    # 1.1. If it already existed, then we remove it
    if os.path.exists(output_directory + "1_my_map_simulation/"):
        shutil.rmtree(output_directory + "1_my_map_simulation/")

    # 1.2. We create it again
    os.makedirs(output_directory + "1_my_map_simulation/")

    # 2. We collect the list of files we have to process
    file_names = os.listdir(input_directory)
    if (".DS_Store" in file_names):
        file_names.remove(".DS_Store")

    # 3. We process each file sequentially
    for file in file_names:
        # 3.1. We open the file to be read
        my_input_stream = codecs.open(input_directory + file, "r", encoding='utf-8')

        # 3.2. We open the file we want to write to
        my_output_stream = codecs.open(output_directory + "1_my_map_simulation/map_" + file, "w", encoding='utf-8')

        # 3.4. We process it
        my_mapper.my_map(my_input_stream, my_output_stream, my_mapper_input_parameters)

        # 3.4. We close the files
        my_input_stream.close()
        my_output_stream.close()

# ------------------------------------------
# FUNCTION get_first_half_ub
# ------------------------------------------
# Precondition: my_list has at least two keys
def get_first_half_ub(my_list):
    # 1. We create the output variable
    res = -1

    # 2. We look for the middle index and middle key
    size = len(my_list)
    index = size // 2
    middle_key = my_list[index][0]

    # 3. We set up a deviation of 1
    deviation = 1

    # 4. While we haven't found a different key and there are candidates to try
    while ((res == -1) and ((index + deviation) < size)):
        # 4.1. If we found a further position p with a different key, we set the ub to p-1
        if (my_list[index + deviation][0] != middle_key):
            res = index + (deviation - 1)
        else:
            # 4.2. If we found a previous position p with a different key, we set up ub to p
            if (my_list[index - deviation][0] != middle_key):
                res = index - deviation
            # 4.3. Otherwise we keep trying with the next position
            else:
                deviation = deviation + 1

    assert (res != -1)

    # 5. We return res
    return res

# ------------------------------------------
# FUNCTION populate_reducer_input_file
# ------------------------------------------
def populate_reducer_input_file(my_list, lb_index, ub_index, name):
    # 1. We open the file for writing
    my_output_stream = codecs.open(output_directory + "2_my_sort_simulation/" + name, "w", encoding='utf-8')

    # 2. We populate it
    for index in range(lb_index, ub_index+1):
        # 2.1. We get the item
        item = my_list[index]

        # 2.2. We generate the String and print it
        my_str = str(item[0]) + '\t' + str(item[1]) + '\n'
        my_output_stream.write(my_str)

    # 3. We close the file
    my_output_stream.close()

# ------------------------------------------
# FUNCTION my_sort_simulation
# ------------------------------------------
def my_sort_simulation(output_directory):
    # 1. We create the sort_simulation folder

    # 1.1. If it already existed, then we remove it
    if os.path.exists(output_directory + "2_my_sort_simulation/"):
        shutil.rmtree(output_directory + "2_my_sort_simulation/")

    # 1.2. We create it again
    os.makedirs(output_directory + "2_my_sort_simulation/")

    # 2. We collect the list of files we have to process
    file_names = os.listdir(output_directory + "1_my_map_simulation")

    # 3. We traverse the files to populate content
    content = []
    num_reducers = 1

    for file in file_names:
        # 3.1. We open the file for reading
        my_input_stream = codecs.open(output_directory + "1_my_map_simulation/" + file, encoding='utf-8')

        # 3.2. We read the file content
        for line in my_input_stream:
            line = line.replace('\n', '')
            words = line.split('\t')
            content.append( (words[0], words[1]) )

        # 3.3. We close the file
        my_input_stream.close()

    # 4. We sort the content
    content.sort()

    # 5. We check if we can make two reducers
    size = len(content)
    ub_index = -1

    # 5.1. If there is at least two (key, value) pairs with different keys...
    if ((size > 1) and (content[0][0] != content[size-1][0])):
        # 5.1.1. We set up the number of reducers to 2
        num_reducers = 2

        # 5.1.2. We get the ub of the first reducer
        ub_index = get_first_half_ub(content)

    # 6. We populate the reducers
    if (num_reducers == 2):
        populate_reducer_input_file(content, 0, ub_index, "sort_1.txt")
        populate_reducer_input_file(content, ub_index+1, size-1, "sort_2.txt")
    else:
        populate_reducer_input_file(content, 0, size-1, "sort_1.txt")

# ------------------------------------------
# FUNCTION my_reducer_simulation
# ------------------------------------------
def my_reducer_simulation(output_directory, my_reducer_input_parameters):
    # 1. We create the map_simulation folder

    # 1.1. If it already existed, then we remove it
    if os.path.exists(output_directory + "3_my_reduce_simulation/"):
        shutil.rmtree(output_directory + "3_my_reduce_simulation/")

    # 1.2. We create it again
    os.makedirs(output_directory + "3_my_reduce_simulation/")

    # 2. We collect the list of files we have to process
    file_names = os.listdir(output_directory + "2_my_sort_simulation/")

    # 3. We process each file sequentially
    for file in file_names:
        # 3.1. We open the file to be read
        my_input_stream = codecs.open(output_directory + "2_my_sort_simulation/" + file, "r", encoding='utf-8')

        # 3.2. We open the file we want to write to
        my_output_stream = codecs.open(output_directory + "3_my_reduce_simulation/reduce_" + file, "w", encoding='utf-8')

        # 3.3. We process it
        my_reducer.my_reduce(my_input_stream, my_output_stream, my_reducer_input_parameters)

        # 3.4. We close the files
        my_input_stream.close()
        my_output_stream.close()

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(input_directory,
            output_directory,
            my_mapper_input_parameters,
            my_reducer_input_parameters
           ):

    # 1. Map Stage: We simulate it by assuming that:
    # One my_mapper.py process is assigned to each file
    # All my_mapper.py processes are run sequentially
    # The results are written to the file my_mapper_results.txt
    my_mapper_simulation(input_directory, output_directory, my_mapper_input_parameters)

    # 2. Sort Stage: We simulate it by assuming that:
    # All results from my_map_simulation are written to the file my_mapper_results.txt
    # The results are written to the file my_sort_results.txt
    my_sort_simulation(output_directory)

    # 3. Reduce Stage: We simulate it by assuming that:
    # All results from my_sort_simulation are written to the file my_sort_results.txt
    # There is a single my_reducer.py process
    # The results are written to the file my_reducer_results.txt
    my_reducer_simulation(output_directory, my_reducer_input_parameters)

# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. We collect the input values
    input_directory = "../../my_dataset/"
    output_directory = "../../my_results/A01_Part4/"

    # 2. my_mappper.py input parameters
    # We list the parameters here

    # We create a list with them all
    my_mapper_input_parameters = []

    # 3. my_reducer.py input parameters
    # We list the parameters here
    top_n_bikes = 10

    # We create a tuple with them all
    my_reducer_input_parameters = []
    my_reducer_input_parameters.append( top_n_bikes )

    # 4. We call to my_main
    my_main(input_directory,
            output_directory,
            my_mapper_input_parameters,
            my_reducer_input_parameters
           )
