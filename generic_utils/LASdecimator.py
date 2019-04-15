##########################################################
#                                                        #
#                .LAS File Decimator                     #
# This program reads in a number of .LAS downhole log    #
# files and reduces the number of depth samples by a     #
# specified factor. It then writes the reduced number of #
# depth samples back out to a different file for futher  #
# use in other applications.                             #
#                                                        #
# Written by Mike Barnes, April 2019                     #
# michael.barnes@ga.gov.au                               #
#                                                        #
##########################################################


# Import required packages
import os
import pandas as pd
import lasio

######################################

# USER CONFIGURATION AREA
#-------------------------------------

# This script supports two possible use cases:
# Case 1:  All .LAS files are already in one directory,
# Case 2: The las files are in subdirectories (eg project\bores\hole_name\hole.LAS

# For Case 1, enter the path to the directory as the value for the las_dir variable
# For Case 2, point to the deepest directory that contains all the .LAS files,
# which would be "project\bores" in the example path
# Enter working directory here
las_dir = r 'enter/path/here'

# Case 1: Uncomment this line if all your data is in a single directory. Comment out the subsequent block of code.
# List comprehension to create a list of all files in the directory ending with .las (case insensitive)
las_files = [las_dir + '\\' + file for file in os.listdir(las_dir) if file.lower()[-4::] == '.las']

# Case 2: Uncomment these lines if all your data is in nested directories
las_files = []
# Loop through all directories and sub directories with os.walk, unpacking the resulting tuple
for dir, sub_dir, files in os.walk(las_dir):
	# Loop through each file in each directory
	for file in files:
		if file.lower()[-4::] == '.las':
			# If the file is a .las (case insensitive), add it to a list
			las_files.append(dir + '\\' + file)

# Enter the output directory, where you'd like your decimated .LAS files saved:
out_dir = r'enter\path\here'

# Select your decimation factor. Must be > 0 and <= 1.
# For example, 0.5 will remove every second sample. 0.9 will remove 9/10 samples.
decimation_factor = 0.9

### END USER CONFIGURATION AREA
####################################

# Loop through each file
for las_file in las_files:
	# Split the entire path up just to harvest file name
	file_name = las.split('\\')[-1]
	# Add in a suffix to the file name stating that the output has been decimated and at what factor
	new_file_name = file_name.replace('.', 'decimated' + str(decimation_factor) + '.')
	
	# Read the las file into memory
	las = lasio.read(las_file)
	# pull the data into a pandas dataframe
	df = las.df()
	# conver the decimation factor to a denominator for a modulo (remainder) division
	modulo = 1 / (1 - decimation_factor)
	# List comprehension to build a list of the row names (depths) for each of the rows in which the
	# row number divided by modulo yields no remainder. Eg 10%5 = 0, 12%5 = 2.
	rows_to_keep = [row for i, row in enumerate(df.index) if i % modulo == 0]
	# Assign just the subset of rows back to the dataframe (ie remove the unwanted rows)
	df = df.loc[rows_to_keep]
	# Set the now decimated data as the values for the las object
	las.set_data(df)
	# Save the modified las data, with the original header to the requested output directory
	las.write(out_dir + '\\' + new_file_name)