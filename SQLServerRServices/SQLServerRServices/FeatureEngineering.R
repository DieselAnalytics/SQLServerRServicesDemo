
########################################################################################################
########################################################################################################
########################################################################################################
# 1. Loads necssary packages
library(RODBC)
# RODBC package is used to interact with databases from within R

library(dplyrXdf)
# The dplryXdf package is not on CRAN. You need to install it from the author's 
# Github site. Below is the code that you need to install the package:
#
#          devtools::install_github("Hong-Revo/dplyrXdf").
#
# You need to make sure that you have it installed in both Microsoft R Client
# and in Microsoft R Services
#
# Major verbs in the dplyrXdf package
#
#	- filter and select to choose rows and columns
#	- mutate and transmute to do data transformation
#	- group_by to define groups
#	- summarise and do to carry out computations on grouped data
#	- arrange to sort by variables
#	- rename to rename columns
#	- distinct to drop duplicates
#
#   Article demostrating how to use dplyrXdf:  http://blog.revolutionanalytics.com/2015/10/using-the-dplyrxdf-package.html
#   dplyrXdf cheat sheet:  http://blog.revolutionanalytics.com/downloads/dplyrXdf_CheatSheet.pdf
#
########################################################################################################
########################################################################################################
########################################################################################################
# 2. Connect to the database
# Builds the individual components for the connection string
driverStr <- "driver={SQL Server}"
serverStr <- "Server=LAPTOP-3VQG3HOU"
database.name = "Database=NYCTaxiDB"
uidStr <- "Trusted_Connection=yes"

# Creates the connection string and connection object
connection.string = paste(driverStr, serverStr, database.name, uidStr, sep = ";")
conn <- odbcDriverConnect(connection.string)

########################################################################################################
########################################################################################################
########################################################################################################
# 3. Build dataframe based on sample data set from SQL Server database 
#
# Below is the query used to get a sample data set that we are going to use to test our dplyrXdf code.
# I used the TABLESAMPLE function to get a sample of the data. That is much more efficient 
# than sorting on a GUID created by the NEWID() function and using the TOP function to return
# the number of rows that you want. It also give you a REPEATABLE option that allows you to set 
# the seed so that your sample will return the same data set everytime. Not possible using the
# guid method.
#
# The TABLESAMPLE function accepts (n ROWS) or (n precent) as a parameter. Note that the function 
# works on pages so you may get a little more or less than what you specified in your parameter.
# Given that I suggest that you set your parameter to a number bigger than the sample that you
# want and use the TOP function to specify that exact sample size you want as illustrated below.
sampleDataQuery <-
	"
		SELECT TOP(100)
		      tipped
			, tip_amount
			, fare_amount
			, passenger_count
			, trip_time_in_secs
			, trip_distance
			, pickup_datetime
			, dropoff_datetime
			, pickup_longitude = cast(pickup_longitude as float)
			, pickup_latitude = cast(pickup_latitude as float)
			, dropoff_longitude = cast(dropoff_longitude as float)
			, dropoff_latitude = cast(dropoff_latitude as float)
			, payment_type
		FROM dbo.nyctaxi_sample
		TABLESAMPLE(150 ROWS)
		REPEATABLE(1)
	"
SQLSampleDataDF <- sqlQuery(channel = conn, sampleDataQuery)

odbcClose(conn)

########################################################################################################
########################################################################################################
########################################################################################################
# 4. Build xdf file 

# Path of the directory where we will save the R objects that we will create
ShareDirPath <- c("D:/OneDrive - Diesel Analytics/Talks/SQLServerRServices_Atlanta/SQLServerR_Atlanta/SQLServerR_Atlanta/ShareDrive")
SampleData_XdfPath <- file.path(ShareDirPath, "SampleDataXdf.xdf")

# Populates the xdf file describe by the ModeTrainlData_XdfFileName variable 
# using the information in the ModeTrainlDataDS variable
rxDataStep(inData = SQLSampleDataDF, outFile = SampleData_XdfPath, overwrite = TRUE)
SampleData_Xdf <- RxXdfData("./ShareDrive/SampleDataXdf.xdf")

########################################################################################################
########################################################################################################
########################################################################################################
# 5. Define function to create our direct_distance feature

# Define a function in open source R to calculate the direct distance between pickup and dropoff as a new feature 
# Use Haversine Formula: https://en.wikipedia.org/wiki/Haversine_formula
env <- new.env()
env$ComputeDist <- function(pickup_long, pickup_lat, dropoff_long, dropoff_lat) {
	R <- 6371 / 1.609344 #radius in mile
	delta_lat <- dropoff_lat - pickup_lat
	delta_long <- dropoff_long - pickup_long
	degrees_to_radians = pi / 180.0
	a1 <- sin(delta_lat / 2 * degrees_to_radians)
	a2 <- as.numeric(a1) ^ 2
	a3 <- cos(pickup_lat * degrees_to_radians)
	a4 <- cos(dropoff_lat * degrees_to_radians)
	a5 <- sin(delta_long / 2 * degrees_to_radians)
	a6 <- as.numeric(a5) ^ 2
	a <- a2 + a3 * a4 * a6
	c <- 2 * atan2(sqrt(a), sqrt(1 - a))
	d <- R * c
	return(d)
}

########################################################################################################
########################################################################################################
########################################################################################################
# 6. Define function to create our direct_distance feature using the dplyrXdf package along with rxDataStep
#    function for RevoScaleR
#
#
# Below are the rx functions whose arguments are available for the given dplyrXdf verb
#
# subset, filter and select:    rxDataStep
#
# mutate and transmute:         rxDataStep
#
# summarise:                    depending on the method chosen, rxCube or rxSummary
#
# arrange:                      rxSort
#
# distinct:                     rxDataStep
#
# factorise:                    depending on the data source, rxFactors(for an xdf) or 
#                               rxImport(for a non - xdf file source)
#
# doXdf:                        rxDataStep
#
# Two-table verbs:              rxMerge

# Use a simple dplyrXdf script that adds a column to the SampleData_Xdf xdf file  
SampleData <-
	SampleData_Xdf %>%
	mutate(SampleData_Xdf,
		   .rxArgs =
				list(
					transforms = list(direct_distance = ComputeDist(pickup_longitude, pickup_latitude, dropoff_longitude, dropoff_latitude)),
					transformEnvir = env
				),
		   .outFile = "./ShareDrive/SampleDataXdf.xdf"
	) # %>%
    # persist("./ShareDrive/SampleDataXdf.xdf")
