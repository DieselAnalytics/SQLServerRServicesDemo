# This step adds the model to SQL Server by executing the AddModel stored procedure from R 
# using the RODBC library:
#
#	EXEC AddModel 
#		 @ModelName = '<model name>'
#		,@Model_Serialized = '<string representation of the model>'
#
# The only parameter that is absolutely required to operationalize the model is the @Model_Serialized parameter which is the parameter that contains the 
# serialized version of the model. That parameter is passed to the AddModel stored procedure in SQL Server and is converted back into a binary format then 
# inserted into the NBAModels table. The field type of the column in the NBAModels table that stores the model is varbinary(max). Two columns were added to the 
# table. The AIC field was added to hold the AIC statistic and a field was also added to hold the model's name. 
#
# The model is operationalized via the "PredictGameBatchMode" stored procedure. This stored procedure passes the model that you want to use to score the last quarter 
# of the season from the NBAModels table to a R script. The R script uses the rxPredict function to score the data. The output of rxPredict function is normally a 
# vector but rxPredict gives you the option to return additional columns. The game_id is returned so that we can join the scored data with the other data in the database.

library(RODBC)

# Connects to the database
server.name = "LAPTOP-3VQG3HOU"
db.name = "NYCTaxiDB"
connection.string = paste("driver={SQL Server}", ";", "server=", server.name, ";", "database=", db.name, ";", "trusted_connection=true", sep = "")
conn <- odbcDriverConnect(connection.string)

# Dynamically builds the stored procedure needed to add the models to the database for each model. The first model name
# is "model.1" and subsequent model names are incremented by 1. You have to tell the "for" loop how big your range is.


# Searialize the model
expression <- c("serialize(model.object, NULL)")
# the eval(parse()) combo enables you to execute dynamically generated code
modelbin <- eval(parse(text = expression)) 
modelbinstr = paste(modelbin, collapse = "")

# Builds the SQL Statement and execute it using the sqlQuery command
sql.code <- paste0("EXEC AddModel ", "@ModelName='", model.name, "', ", "@Model_Serialized='", modelbinstr, "'")
sqlQuery(conn, sql.code)

odbcClose(conn)