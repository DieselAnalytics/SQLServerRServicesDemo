# This model is used for demo purpose only and was borrowed from the Big Data and R course
# on EdX. The code creates a logistic model that will be added to a SQL Server database. That
# will be done in the "AddModelToSQLServer.R script. It is important that this script is ran
# first so that the model will be in memory for the "AddModelToSQLServer.R script.


# Loads dplyrXdf to the session
library(dplyrXdf)

# Builds the connection string
driverStr <- "Driver=SQL Server"
serverStr <- "Server=LAPTOP-3VQG3HOU"
dbStr <- "Database=NYCTaxiDB"
uidStr <- "Trusted_Connection=yes"
connStr <- paste(driverStr,serverStr,dbStr,uidStr, sep = ";")

# Set to true to wait for the job to complete before providing a prompt
sqlWait <- TRUE
# Set to false to prevent print stdout from the SQL Server console
sqlConsoleOutput <- FALSE

# Director where xdf files and other R objects will be stored
ShareDirPath <- c("D:/OneDrive - Diesel Analytics/Talks/SQLServerRServices_Atlanta/SQLServerR_Atlanta/SQLServerR_Atlanta/ShareDrive")

# Actually *Define* the compute context
cc <- RxInSqlServer(connectionString = connStr, shareDir = ShareDirPath,
					wait = sqlWait, consoleOutput = sqlConsoleOutput)
# Set it so that it is the engine used.
rxSetComputeContext(cc)

# Defines the data set that you want to get from SQL Server
Query <-

"
SELECT 
	 tipped
	,tip_amount
	,fare_amount
	,passenger_count
	,trip_time_in_secs
	,trip_distance 
	,pickup_datetime
	,dropoff_datetime 
	,pickup_longitude = cast(pickup_longitude as float) 
	,pickup_latitude = cast(pickup_latitude as float) 
	,dropoff_longitude = cast(dropoff_longitude as float) 
	,dropoff_latitude = cast(dropoff_latitude as float)
	,payment_type 
FROM nyctaxi_sample
WHERE pickup_datetime < '20131101'
"

# Creates a list variable that will be used to provide TrainDataDS the information needed to
# convert the payment_type variable (column) to a factor.
ptypeColInfo <- list(
  payment_type = list(
	type = "factor",
	levels = c("CSH", "CRD", "DIS", "NOC", "UNK")
  )
)

# Creates the TrainDataDS variable that is used to describe the data source that you want
# to use and the data that you want to return along with the data types you want your 
# variables (columns) to be.
TrainDataDS <- RxSqlServerData(sqlQuery = Query,
								connectionString = connStr,
								colInfo = ptypeColInfo,
								colClasses = c(trip_time_in_secs = "integer", pickup_longitude = "numeric", pickup_latitude = "numeric",
											   dropoff_longitude = "numeric", dropoff_latitude = "numeric"),
								rowsPerRead = 500)

# Defines the path that you want your xdf file to be
TrainData_xdf_FileName <- file.path(ShareDirPath, "TrainDataXdf.xdf")

# Populates the xdf file describe by the TrainData_xdf_FileName variable 
# using the information in the TrainDataDS variable
rxSetComputeContext("local")
rxDataStep(inData = TrainDataDS, outFile = TrainData_xdf_FileName, overwrite = TRUE)
TrainData_Xdf <- RxXdfData("./ShareDrive/TrainDataXdf.xdf")

#Use dyplyrXdf to convert necessary features to factors for the model
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

# I am using the dplyrXdf package to perform this step. This is an alternative to using
# the rxDataStep function. The package offers benefits such as taking care of certain file
# management tasks and being very readible. The code below works when I connect to 
# Microsoft R Client 3.3.2.0 but did not work when I connected to R Server 8.0. Given that
# R Server 9.0 is out it may be a version issue. I will continue to try to get this to work 
# on in R Server via SQL Servre R Services.
TrainData <-
	TrainData_Xdf %>%
	mutate(TrainData_Xdf,
		   .rxArgs =
				list(
					transforms = list(direct_distance = ComputeDist(pickup_longitude, pickup_latitude, dropoff_longitude, dropoff_latitude)),
					transformEnvir = env
				),
			.outFile = "./ShareDrive/TrainDataXdf.xdf"
	) # %>%
  # persist("./ShareDrive/TrainDataXdf.xdf")
	  #
      #    Previously you had to use the persist verb to persist the database to disk. Now you
      #    can use the .outfile argument which is faster because it elements the step of dplyrXdf
	 

TrainData_Xdf <- RxXdfData("./ShareDrive/TrainDataXdf.xdf")

# Model
# Build the model object
model.name = "Tip Predictor2"
model.formula <- as.formula("tipped ~ passenger_count + trip_distance + trip_time_in_secs + direct_distance")
model.object <- rxLogit(model.formula, data = TrainData_Xdf)
