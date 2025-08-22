cat("\014")
rm(list = ls())
set.seed(1234)
setwd("/cloud/project/cleaning_script/read_SQL/clean_contracts_efficient_250820")
library("RPostgres")
library("dplyr")
library("dbplyr")
library("readr")
library("knitr")
library("data.table")
library("lubridate")
library("DBI")
library("pacman")
library("R.huge")
# unlink("/cloud/lib/x86_64-pc-linux-gnu-library/4.4/00LOCK*", recursive = TRUE)
# install.packages("duckdb")
library("duckdb")

sessionInfo()
getwd()
file_site = "https://open.canada.ca/data/dataset/d8f85d91-7dec-4fd1-8055-483b77225d8b/resource/fac950c0-00d5-4ec1-a4d3-9cbebf98a305/download/contracts.csv"
test <- fread(file_site, nrows = 1)
#View(test)
names(test)
c("reference_number", "procurement_id", "vendor_name", "vendor_postal_code", 
  "buyer_name", "contract_date", "economic_object_code", "description_en", 
  "description_fr", "contract_period_start", "delivery_date", "contract_value", 
  "original_value", "amendment_value", "comments_en", "comments_fr", 
  "additional_comments_en", "additional_comments_fr", "agreement_type_code", 
  "trade_agreement", "land_claims", "commodity_type", "commodity_code", 
  "country_of_vendor", "solicitation_procedure", "limited_tendering_reason", 
  "trade_agreement_exceptions", "indigenous_business", "indigenous_business_excluding_psib", 
  "intellectual_property", "potential_commercial_exploitation", 
  "former_public_servant", "contracting_entity", "standing_offer_number", 
  "instrument_type", "ministers_office", "number_of_bids", "article_6_exceptions", 
  "award_criteria", "socioeconomic_indicator", "reporting_period", 
  "owner_org", "owner_org_title")

#edit(names(test))

x_w <- c("reference_number", "vendor_name", "vendor_postal_code", "contract_date", 
         "economic_object_code", "contract_period_start", "delivery_date", 
         "contract_value", "original_value", "amendment_value", "commodity_type", 
         "commodity_code", "contracting_entity", "ministers_office", 
         "reporting_period", "owner_org", "owner_org_title", "procurement_id")


ID_name <- c(813, 812, 499, 473, 491, 474, 475)

dd <- fread(file_site,select=x_w)[economic_object_code %in% ID_name] 
csv <- "/cloud/project/cleaning_script/read_SQL/clean_contracts_efficient_250820/raw_contracts250820a.csv"
y <- write.csv(dd, file ="/cloud/project/cleaning_script/read_SQL/clean_contracts_efficient_250820/raw_contracts250820a.csv", row.names = TRUE)
rm(dd)
# Read with fread (handles NA properly)
#dt <- fread(csv, na.strings = c("NA", ""))
dt <- fread(csv, na.strings = c(NA, ""))

# Method 3: Using base R (works with regular data.frames too)
dt[dt == "" | trimws(dt) == ""] <- NA

# # Method 4: Most comprehensive - handles multiple spaces, tabs, etc.
# dt[, names(dt) := lapply(.SD, function(x) {
#   ifelse(grepl("^\\s*$", x), NA, x)
# })]

y <- write.csv(dt, file ="/cloud/project/cleaning_script/read_SQL/clean_contracts_efficient_250820/raw_contracts250820a.csv", row.names = TRUE)

dt[8834,]
length(is.na(dt$vendor_postal_code)) #139092
names(dt)
#View(dt)

getwd()
#vignette(package = "R.huge")

con <- DBI::dbConnect(duckdb::duckdb(), "contracts.duckdb") # creates an empty file 'your.duckdb'
DBI::dbWriteTable(con, "contracts_tbl240403", dt, overwrite = TRUE)
# gives summary of memory used
gc()
rm(dt)
#show all tables in the dB
# Method 1: More detailed table information
tables_info <- DBI::dbGetQuery(con, "SHOW TABLES")
print(tables_info) #contracts_tbl240403
# Method 2: Get table info with dbplyr syntax
tables_tbl <- tbl(con, sql("SHOW TABLES"))
tables_tbl %>% collect() #contracts_tbl240403

#load the table as df from dB
# Method 1: Using dbplyr with tbl() and collect() (recommended)
df <- tbl(con, "contracts_tbl240403") %>% collect()

# Method 2: Using DBI directly (also works)
df <- DBI::dbReadTable(con, "contracts_tbl240403")
View(df)
#filtering out the data
list <- c("K1P")
# filter out items from the list
clean_loaded_data <- df %>%
  #dplyr::select(vendor_postal_code) %>%
  dplyr::filter(vendor_postal_code %in% list) %>%
  collect()
#View(clean_loaded_data)
rm(clean_loaded_data)
############################################################################################################################################################
#dbDisconnect()
############################################################################################################################################################
#run an external cleaning script
#source("/cloud/project/cleaning_script/read_SQL/clean_contracts_efficient_2250820/second_cleaning_250821.R")
source("/cloud/project/cleaning_script/read_SQL/clean_contracts_efficient_250820/second_cleaning_250821.R")
