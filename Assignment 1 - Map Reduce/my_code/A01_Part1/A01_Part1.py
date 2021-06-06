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
import glob

# ------------------------------------------
# FUNCTION process_line
# ------------------------------------------
def process_line(line):
    # 1. We create the output variable
    res = ()

    # 2. We get the parameter list from the line
    params_list = line.strip().split(",")

    #(00) start_time => A String representing the time the trip started at <%d/%m/%Y %H:%M:%S>. Example: “2019/05/02 10:05:00”
    #(01) stop_time => A String representing the time the trip finished at <%d/%m/%Y %H:%M:%S>. Example: “2019/05/02 10:10:00”
    #(02) trip_duration => An Integer representing the duration of the trip. Example: 300
    #(03) start_station_id => An Integer representing the ID of the CityBike station the trip started from. Example: 150
    #(04) start_station_name => A String representing the name of the CitiBike station the trip started from. Example: “E 2 St &; Avenue C”.
    #(05) start_station_latitude => A Float representing the latitude of the CitiBike station the trip started from. Example: 40.7208736
    #(06) start_station_longitude => A Float representing the longitude of the CitiBike station the trip started from. Example:  -73.98085795
    #(07) stop_station_id => An Integer representing the ID of the CityBike station the trip stopped at. Example: 150
    #(08) stop_station_name => A String representing the name of the CitiBike station the trip stopped at. Example: “E 2 St &; Avenue C”.
    #(09) stop_station_latitude => A Float representing the latitude of the CitiBike station the trip stopped at. Example: 40.7208736
    #(10) stop_station_longitude => A Float representing the longitude of the CitiBike station the trip stopped at. Example:  -73.98085795
    #(11) bike_id => An Integer representing the id of the bike used in the trip. Example:  33882
    #(12) user_type => A String representing the type of user using the bike (it can be either “Subscriber” or “Customer”). Example: “Subscriber”.
    #(13) birth_year => An Integer representing the birth year of the user using the bike. Example:  1990
    #(14) gender => An Integer representing the gender of the user using the bike (it can be either 0 => Unknown; 1 => male; 2 => female). Example:  2.
    #(15) trip_id => An Integer representing the id of the trip. Example:  190

    # 3. If the list contains the right amount of parameters
    if (len(params_list) == 16):
        # 3.1. We set the right type for the parameters
        params_list[2] = int(params_list[2])
        params_list[3] = int(params_list[3])
        params_list[5] = float(params_list[5])
        params_list[6] = float(params_list[6])
        params_list[7] = int(params_list[7])
        params_list[9] = float(params_list[9])
        params_list[10] = float(params_list[10])
        params_list[11] = int(params_list[11])
        params_list[13] = int(params_list[13])
        params_list[14] = int(params_list[14])
        params_list[15] = int(params_list[15])

        # 3.2. We assign res
        res = tuple(params_list)

    # 4. We return res
    return res

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(input_folder, output_file):

    #Empty list to hold all stations started and stopped at
    stationList = []
    
    #Set to hold all unique station names
    stationListUnique = set()
    
    #List to hold all tuples from each csv file together
    combined_data = []
    
    #Gets each csv file from the given folder directory
    for filepath in glob.iglob(input_folder + '*.csv'):

        #Open file in read mode
        with open(filepath, 'r') as file:
            
            # Iterate over each row in the csv using reader object
            for row in file:
                
                combined_data.append(process_line(row))
            
    #Add the station started at and stopped at in each row to above list
    for cd in combined_data:
        stationList.append(cd[4])
        stationList.append(cd[8])
    
    #Loop over all stations mentioned and add to set, removing duplicates
    for x in stationList:
        stationListUnique.add(x)
        
    #Print out number of elements in each list to check how many duplicates
    print("Size of list of all stations mentioned: ", len(stationList))
    print("Size of set of all unique stations: ", len(stationListUnique))
    
    #Empty dictionary to hold key value pairs for each station and its stop and start counts
    station_dict = dict()
    
    #Loops over each unique station, loops over each tuple from csv row
    for x in stationListUnique:
        
        #Initializing stop and start counts to zero
        countStart = 0
        countStop = 0
        
        for d in combined_data:
            
            #Adds to the startCount if the station is the same as the one in that tuples start position
            if (d[4] == x):
                countStart = countStart + 1
                
            #Adds to the stopCount if the station is the same as the one in that tuples stop position
            if (d[8] == x):
                countStop = countStop + 1
        
        #Loop is exited for that station, its name, startCount and stopCount are recorded into the dictionary
        station_dict[x] = [countStart, countStop]
            
    #Sorts the values in the dictionary by their key alphabetically
    station_dict = sorted(station_dict.items())
    
    with open(output_file, 'a') as file:
        file.truncate(0)
        for x in station_dict:
            print(str(x[0]) + "\t (" + str(x[1][0]) + ", " + str(x[1][1]) + ") \n")
            file.write(str(x[0]) + "\t (" + str(x[1][0]) + ", " + str(x[1][1]) + ") \n")
        
# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. We collect the input values
    input_folder = "../../my_dataset/"
    output_file = "../../my_results/A01_Part1/result.txt"

    # 1.1. If we call the program from the console then we collect the arguments from it
    if (len(sys.argv) > 1):
        input_folder = sys.argv[1]
        output_file = sys.argv[2]

    # 2. We call to my_main
    my_main(input_folder, output_file)
