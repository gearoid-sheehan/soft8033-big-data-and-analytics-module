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
import codecs

# ------------------------------------------
# FUNCTION pass_test_single_file
# ------------------------------------------
def pass_test(my_file_1, my_file_2):
    # 1. We create the output variable
    res = True

    # 2. We open both files
    my_input_stream_1 = codecs.open(my_file_1, "r", encoding="utf-8")
    my_input_stream_2 = codecs.open(my_file_2, "r", encoding="utf-8")

    # 3. We read the full content of each file, removing any empty lines and spaces
    content_1 = [ line.strip().replace(" ", "") for line in my_input_stream_1 if line ]
    content_2 = [ line.strip().replace(" ", "") for line in my_input_stream_2 if line ]

    # 4. We close the files
    my_input_stream_1.close()
    my_input_stream_2.close()

    # 5. We check that both files are equal
    size_1 = len(content_1)

    # 5.1. If both files have the same length
    if (size_1 == len(content_2)):
        # 5.1.1. We compare them line by line
        for index in range(size_1):
            if (content_1[index] != content_2[index]):
                res = False
                break

    # 5.2. If the files have different lengths then they are definitely not equal
    else:
        res = False

    # 6. We return res
    return res

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(part, assignment_solution_folder, student_solution_folder):
    # 1. We collect the list of files to be checked
    list_of_files_to_be_checked = []

    if ((part == 1) or (part == 2) or (part == 3)):
        list_of_files_to_be_checked.append( (assignment_solution_folder + "result.txt", student_solution_folder + "result.txt") )
    elif (part == 4):
        list_of_files_to_be_checked.append((assignment_solution_folder + "2_my_sort_simulation/sort_1.txt", student_solution_folder + "2_my_sort_simulation/sort_1.txt"))
        list_of_files_to_be_checked.append((assignment_solution_folder + "2_my_sort_simulation/sort_2.txt", student_solution_folder + "2_my_sort_simulation/sort_2.txt"))
        list_of_files_to_be_checked.append((assignment_solution_folder + "3_my_reduce_simulation/reduce_sort_1.txt", student_solution_folder + "3_my_reduce_simulation/reduce_sort_1.txt"))
        list_of_files_to_be_checked.append((assignment_solution_folder + "3_my_reduce_simulation/reduce_sort_2.txt", student_solution_folder + "3_my_reduce_simulation/reduce_sort_2.txt"))
    elif ((part == 5) or (part == 6)):
        list_of_files_to_be_checked.append((assignment_solution_folder + "2_my_sort_simulation/sort_1.txt", student_solution_folder + "2_my_sort_simulation/sort_1.txt"))
        list_of_files_to_be_checked.append((assignment_solution_folder + "3_my_reduce_simulation/reduce_sort_1.txt", student_solution_folder + "3_my_reduce_simulation/reduce_sort_1.txt"))

    # 2. We traverse each of the files
    all_test_passed = True
    for file_pair in list_of_files_to_be_checked:
        # 2.1. We print the info
        print("----------------------------------------------------------\nChecking :\n" + str(file_pair[0]) + "\n" + str(file_pair[1]) + "\n")

        # 2.2. We see if the check is passed
        test_passed = pass_test(file_pair[0], file_pair[1])
        if (test_passed == True):
            print("Test passed!")
        else:
            print("Test did not pass.")
            all_test_passed = False


    # 3. Print the final outcome
    print("----------------------------------------------------------")
    if (all_test_passed == True):
        print("Congratulations, the code passed all the tests!")
    else:
        print("Sorry, the output of some files is incorrect!")
    print("----------------------------------------------------------")

# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. We get the input values
    part = 4

    # 1.1. If the program is called from console, we modify the parameters
    if (len(sys.argv) > 1):
        # 2.1. We get the student folder path
        part = int(sys.argv[1])

    # 2. We get the folders to explore
    assignment_solution_folder = "./Assignment_Solutions/A01_Part" + str(part) + "/"
    student_solution_folder = "./Student_Attempts/A01_Part" + str(part) + "/"

    # 3. We call to my_main
    my_main(part, assignment_solution_folder, student_solution_folder)
