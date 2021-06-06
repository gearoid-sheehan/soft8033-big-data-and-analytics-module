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

def process_line(line):
    
    # 1. We create the output variable
    res = ()

    #Holds the station name of that row
    station = None
    
    countDuration = None
    countTrips = None
    
    # 2. We get the parameter list from the line
    params_list = line.strip().split("\t")
    data = params_list[1].split(", ")
    station = int(data[0].replace("(", ""))
    countDuration = int(data[1])
    countTrips = int(data[2].replace(")", ""))

    res = (station, countDuration, countTrips)
    
    # 4. We return res
    return res

# ------------------------------------------
# FUNCTION my_reduce
# ------------------------------------------
def my_reduce(my_input_stream, my_output_stream, my_reducer_input_parameters):
    
    top_n_bikes = 10
    
    #Empty list to hold all bike IDs mentioned in the files
    bikeIdList = []
    
    #Set to hold all unique bike IDs
    bikeIdListUnique = set()
    
    #List to hold all tuples from each csv file together
    combined_data = []
    
    # Iterate over each row in the csv and use the process function and add result to list
    for row in my_input_stream:
        combined_data.append(process_line(row))
    
    #Add bike ID in each row to above list
    for cd in combined_data:
        bikeIdList.append(cd[0])
        
    #Loop over all bike IDs mentioned and add to set, removing duplicates
    for x in bikeIdList:
        bikeIdListUnique.add(x)
    
    #Empty list to hold Id for each bike and its duration cycled and number of trips
    bike_tuple_list = list()
    
    #Loops over each unique bike ID, loops over each tuple from csv row
    for x in bikeIdListUnique:
        
        #Initializing trip duration and trip number to zero
        totalTripDuration = 0
        totalTripNumber = 0
        
        #Loop over list of all rows combined from all datasets
        for d in combined_data:
        
            #If the bike ID is in a row, add the trip duration and increment the trip number
            if (d[0] == x):
                totalTripDuration = totalTripDuration + d[1]
                totalTripNumber = totalTripNumber + d[2]
        
        #Loop is exited for that bike ID, its ID, trip duration and trip number are recorded into a tuple   
        bike_tuple = (x, totalTripDuration, totalTripNumber)
        
        #That tuple is then added to a list of tuples, and initialized to empty for the next bike ID
        bike_tuple_list.append(bike_tuple)
        bike_tuple = ()                       
       
    
    #The list of tuples is sorted to contain only the tuples with the largest n trip durations                       
    bike_tuple_list = sorted(bike_tuple_list, key=lambda t: t[1], reverse=True)[:top_n_bikes]
    
    #Loop over the list of tuples and write each to the file as a line of text
    for x in bike_tuple_list:
        my_output_stream.write(str(x[0]) + "\t (" + str(x[1]) + ", " + str(x[2]) + ") \n")

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
    #top_n_bikes = 10

    # 1.1. If we call the program from the console then we collect the arguments from it
    if (len(sys.argv) > 1):
        file_name = sys.argv[1]

    # 2. Local or Hadoop
    local_False_hadoop_True = False

    # 3. We set the path to my_dataset and my_result
    my_input_stream = sys.stdin
    my_output_stream = sys.stdout

    if (local_False_hadoop_True == False):
        my_input_stream = "../../my_results/A01_Part5/2_my_sort_simulation/" + file_name
        my_output_stream = "../../my_results/A01_Part5/3_my_reduce_simulation/reduce_" + file_name[5:]

    # 4. my_reducer.py input parameters
    my_reducer_input_parameters = []
    #my_reducer_input_parameters.append( top_n_bikes )

    # 5. We call to my_main
    my_map(my_input_stream, my_output_stream, my_reducer_input_parameters)
