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
import sys

# ------------------------------------------
# FUNCTION process_line
# ------------------------------------------
def process_line(line):
    
    # 1. We create the output variable
    res = ()

    #Holds the station name of that row
    station = None
    
    countStart = None
    countStop = None
    
    # 2. We get the parameter list from the line
    params_list = line.strip().split("\t")
    
    station = params_list[0]
    
    split_start_stop = params_list[1].strip(")").replace(" ", "").replace("(", "").split(",")
    
    countStart = int(split_start_stop[0])
    countStop = int(split_start_stop[1])

    res = (station, countStart, countStop)
    
    # 4. We return res
    return res

# ------------------------------------------
# FUNCTION my_reduce
# ------------------------------------------
def my_reduce(my_input_stream, my_output_stream, my_reducer_input_parameters):
    
    #Empty list to hold all stations started and stopped at
    stationList = []
    
    #Set to hold all unique station names
    stationListUnique = set()
    
    #List to hold all tuples from each csv file together
    combined_data = []
    
    for row in my_input_stream:
        combined_data.append(process_line(row))

    #Add the station started at and stopped at in each row to above list
    for cd in combined_data:
        stationList.append(cd[0])
        
    #Loop over all stations mentioned and add to set, removing duplicates
    for x in stationList:
        stationListUnique.add(x)
    
    #Empty dictionary to hold key value pairs for each station and its stop and start counts
    station_dict = dict()
    
    #Loops over each unique station, loops over each tuple from csv row
    for x in stationListUnique:
        
        #Initializing stop and start counts to zero
        countStart = 0
        countStop = 0
        
        for d in combined_data:
            
            #Adds to the startCount if the station is the same as the one in that tuples start position
            if (d[0] == x):
                countStart = countStart + d[1]
                countStop = countStop + d[2]
        
        #Loop is exited for that station, its name, startCount and stopCount are recorded into the dictionary
        station_dict[x] = [countStart, countStop]
        
    #Sorts the values in the dictionary by their key alphabetically
    station_dict = sorted(station_dict.items())
    
    for x in station_dict:
        
        my_str = str(x[0]) + "\t(" + str(x[1][0]) + ", " + str(x[1][1]) + ")\n"
        
        my_output_stream.write(my_str)
        
# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. We collect the input values
    file_name = "sort_1.txt"

    # 1.1. If we call the program from the console then we collect the arguments from it
    if (len(sys.argv) > 1):
        file_name = sys.argv[1]

    # 2. Local or Hadoop
    local_False_hadoop_True = False

    # 3. We set the path to my_dataset and my_result
    my_input_stream = sys.stdin
    my_output_stream = sys.stdout

    if (local_False_hadoop_True == False):
        my_input_stream = "../../my_results/A01_Part4/2_my_sort_simulation/" + file_name
        my_output_stream = "../../my_results/A01_Part4/3_my_reduce_simulation/reduce_" + file_name[5:]

    # 4. my_reducer.py input parameters
    my_reducer_input_parameters = []

    # 5. We call to my_main
    my_map(my_input_stream, my_output_stream, my_reducer_input_parameters)
