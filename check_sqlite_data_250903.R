setwd("/cloud/project/cleaning_script/read_SQL/clean_contracts_efficient_250820/cleaned_contracts")
list.files()

# Load required libraries
library(DBI)       # Provides a unified interface for database access
library(RSQLite)   # SQLite backend for DBI

# Define the path to your SQLite database
db_path <- "database.sqlite3"  # Adjust if the file is in a subfolder

# Connect to the SQLite database
con <- dbConnect(RSQLite::SQLite(), dbname = db_path)

# List all tables in the database
tables <- dbListTables(con)

# Print the table names
cat("Tables found in the database:\n")
print(tables)

# Disconnect from the database
dbDisconnect(con)

#read the first 10 rows
# Load required libraries
library(DBI)        # Database interface
library(RSQLite)    # SQLite backend
library(dplyr)      # Tidyverse data manipulation
library(dbplyr)     # dplyr backend for databases

# Connect to the SQLite database
con <- dbConnect(RSQLite::SQLite(), "database.sqlite3")

# Reference the Municipalities table as a lazy tbl
municipalities_tbl <- tbl(con, "Municipalities")
municipalities_tbl1 <- municipalities_tbl
#print(dim(as.data.frame(municipalities_tbl1)))
str(municipalities_tbl)
#View(municipalities_tbl1)

# Use dplyr to preview the first 10 rows
municipalities_preview <- municipalities_tbl %>%
  head(10) %>%
  collect()  # Pull data into R for viewing

# Print the result
#print(municipalities_preview)

# Reference the sqlite_sequence table as a lazy tbl
sequence_tbl <- tbl(con, "sqlite_sequence")

# Use dplyr to preview the first 10 rows
sequence_preview <- sequence_tbl %>%
  head(10) %>%
  collect()  # Pull data into R for viewing

# Print the result
#print(sequence_preview)

# Run a direct SQL query
#dbGetQuery(con, "SELECT * FROM sqlite_sequence")
#result
#This returns an empty data frame, so the table truly has no rows.
# Convert municipalities_tbl to an R data frame
municipalities_df <- municipalities_tbl %>%
  collect()  # Pull data into R as a data frame

# Print the data frame
print(dim(municipalities_df))
View(municipalities_df)
# Disconnect from the database
dbDisconnect(con)

# read in grc service contracts
df <- read.csv(file = "grc_service_contracts.csv")
names(df)
length(unique(df$vendor_postal_code))
# Count the number of NA values in the vendor_postal_code column
na_count <- sum(is.na(df$vendor_postal_code))
# Print the result
print(na_count)
dim(df)
table(is.na(df$vendor_postal_code))
str(df$vendor_postal_code)
