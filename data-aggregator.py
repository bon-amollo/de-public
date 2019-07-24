#Code Written By: Bonventure Odhiambo, Senior Data Systems Advisor, +254 725971138
#Date Written: 2019-07-24
#Date Submitted: 2019-07-24


#This assignment aims to process raw data in a .csv format into a clean processed/aggregated dataset based on unique tuples of data elements (network/provider,product,month)
#The raw csv file has variables like id, loan provider/network, product, date and amount

#To achieve this task I used python scripting language
#The choice of my language was based on the following facts:
	#1. Open source
	#2. Robust Automation Script
	#3. easily Reusable and Scalable
	#4. Operating system independent among other reasons

#I have used two major python libraries (a) csv library and (2) numpy library. This is owing to the fact that they are very much applicable in the context.

#A. Data Ingestion/Upload
	#Firstly, the raw csv file (input.csv) is ingested/uploaded
#B. Data Cleaning and Preparation
	#Before any form of analysis is conducted on the raw data, it is paramount to perform cleansing operations to prepare the data in the right format

	#Thus, the script performs some fundamental cleansing functions e.g.
		#1. Cleaning string columns enclosed with single quotes
		#2. Generating Yearmonth from the date string variable. *This is critical in our analysis
			#This is based on the assumption that you might validate linelist client data against masterlist that cuts across 2 or more years

#Both steps A&B are achieved by the function "processCSV()" which accepts the filepath to the raw CSV.
#The function then performs the above cleansing processes on the raw data.
#The result from this function is a cleansed .csv dataset that is ready for the analysis.
#The cleansed .csv file is then passed to another function called "aggregateData()" which performs the aggregation processes.

#C. Data Processing, Aggregation and Output
#The "aggregateData()" function performs the following functions
	#i. Generates Unique tuples
	#The "aggregateData()" function first generates a tuple based on network/provider,product,month.
	#The function then uses the tuples to come up with a unique set

	#ii. Ingests the master list
	#The function uploads the master list against which the unique tuples are to be matched


	#iii. Matching process between each tuple and master list
	#using the for loop, the function iterates through the unique set, and for each unique tuple, it compares with associated dataelements from the masterlist row by row
	#the iteration results into two running variables - the running_count which sums the count of matching instances and the running_amount which sums the amounts for each matching instance

	#iv. generates an Output
	#Based on the criteria provided the function writes the generated values into an output .csv file named "Output.csv"


import csv
import numpy as np


raw_data_filepath='input.csv'
clenased_data_filepath='cleaned_data.csv'

def processCSV(filename):
	with open(clenased_data_filepath,'w',newline="") as output:
		with open(filename,'r') as csv_file:
			output_data = csv.writer(output, delimiter=',',quoting=csv.QUOTE_NONE,escapechar=None, doublequote=False)
			data=csv.reader(csv_file,delimiter=',')
			row_counter=0
			strmonth=''
			for row in data:
				if row_counter==0:
					output_data.writerow(['network','product','yearmonth','amount'])
					row_counter+=1
				else:
					strnetwork=row[1]
					strdate=row[2]
					strmonth=strdate[4:7]
					stryear=strdate[8:12]
					stryearmonth=stryear+strmonth
					strproduct=row[3]
					intamount=row[4]

					strnetwork = strnetwork.replace("'", "")
					strproduct = strproduct.replace("'", "")

					output_data.writerow([strnetwork,strproduct,stryearmonth,intamount])
					row_counter+=1

		print("The raw CSV has been cleansed successfuly, number of rows processed are %d" %(row_counter-1))

	return(clenased_data_filepath)
	output.close()
	csv_file.close()

def aggregateData(cleansedLoanDataFilePath):
	with open('Output.csv','w',newline="") as processed_output:
		csvinput=np.genfromtxt(cleansedLoanDataFilePath,delimiter=',',dtype=None,encoding=None, skip_header=1, usecols=[0,1,2,3])
		csvoutput=np.genfromtxt(cleansedLoanDataFilePath,delimiter=',',dtype=None,encoding=None, skip_header=1, usecols=[0,1,2])

		processed_output_data = csv.writer(processed_output, delimiter=',',quoting=csv.QUOTE_NONE,escapechar=None, doublequote=False)
		processed_output_data.writerow(['network','product','yearmonth','amount','count'])


		unique_set={tuple(row) for row in np.array(csvoutput)}


		network_element_tuple=''
		product_element_tuple=''
		yearmonth_element_tuple=''




		iterator=0

		for unique_tuple in unique_set:

			network_element_tuple=unique_tuple[0]
			product_element_tuple=unique_tuple[1]
			yearmonth_element_tuple=unique_tuple[2]

			running_count=0
			running_amount=0
			for row in csvinput:

				network_element_mainlist = row[0]
				product_element_mainlist = row[1]
				yearmonth_element_mainlist = row[2]

				if network_element_tuple==network_element_mainlist and product_element_tuple==product_element_mainlist and yearmonth_element_tuple==yearmonth_element_mainlist:
					running_amount=running_amount+row[3]
					running_count=running_count+1

			iterator+=1
			processed_output_data.writerow([network_element_tuple,product_element_tuple,yearmonth_element_tuple,running_amount,running_count])


	print('Loan Data Aggregated successfuly. The Processed File Has Been Saved In the Directory As "Output.csv"')
	processed_output.close()

def main():

	aggregateData(processCSV(raw_data_filepath))

main()



