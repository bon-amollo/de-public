Code Written By: Bonventure Odhiambo, Senior Data Systems Advisor, +254 725971138
Date Written: 2019-07-24
Date Submitted: 2019-07-24


The objective of this assignment is to process raw data in a .csv format into a clean processed/aggregated dataset based on unique tuples of data elements (network/provider,product,month)
The raw csv file has variables like id, loan provider/network, product, date and amount

To achieve this task I used python scripting language
The choice of my language was based on the following facts:
	1. Open source
	2. Robust Automation Script
	3. easily Reusable and Scalable
	4. Operating system independent among other reasons

I have used two major python libraries (a) csv library and (2) numpy library. This is owing to the fact that they are very much applicable in the context.

A. Data Ingestion/Upload
	Firstly, the raw csv file (input.csv) is ingested/uploaded
B. Data Cleaning and Preparation
	Before any form of analysis is conducted on the raw data, it is paramount to perform cleansing operations to prepare the data in the right format

	Thus, the script performs some fundamental cleansing functions e.g.
		1. Cleaning string columns enclosed with single quotes
		2. Generating Yearmonth from the date string variable. *This is critical in our analysis
			This is based on the assumption that you might validate linelist client data against masterlist that cuts across 2 or more years 

Both steps A&B are achieved by the function "processCSV()" which accepts the filepath to the raw CSV.
The function then performs the above cleansing processes on the raw data.
The result from this function is a cleansed .csv dataset that is ready for the analysis.
The cleansed .csv file is then passed to another function called "aggregateData()" which performs the aggregation processes.

C. Data Processing, Aggregation and Output
The "aggregateData()" function performs the following functions
	i. Generates Unique tuples
	The "aggregateData()" function first generates a tuple based on network/provider,product,month.
	The function then uses the tuples to come up with a unique set

	ii. Ingests the master list 
	The function uploads the master list against which the unique tuples are to be matched


	iii. Matching process between each tuple and master list
	using the for loop, the function iterates through the unique set, and for each unique tuple, it compares with associated dataelements from the masterlist row by row
	the iteration results into two running variables - the running_count which sums the count of matching instances and the running_amount which sums the amounts for each matching instance

	iv. generates an Output
	Based on the criteria provided the function writes the generated values into an output .csv file named "Output.csv"
