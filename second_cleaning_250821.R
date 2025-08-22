rm(list=ls())
setwd("/cloud/project/cleaning_script/read_SQL/clean_contracts_efficient_250820")
library("RPostgres")
library("sqldf")
library("dplyr")
library("stringr")
library("readr")
library("zoo")
library("knitr")
library("stringdist")
library("data.table")
library("tidyverse")
library("lubridate")
library("data.table")
sessionInfo()

a <- cols(reference_number = col_character(),
          vendor_name = col_character(),
          vendor_postal_code = col_character(),
          buyer_name = col_character(),
          contract_date = col_character(),
          economic_object_code = col_character(),
          contract_period_start = col_character(),
          delivery_date = col_character(),
          contract_value = col_double(),
          original_value = col_double(),
          amendment_value = col_double(),
          commodity_type = col_character(),
          commodity_code = col_character(),
          solicitation_procedure = col_character(),
          indigenous_business = col_character(),
          indigenous_business_excluding_psib = col_character(),
          contracting_entity = col_character(),
          standing_offer_number = col_character(),
          ministers_office = col_character(),
          reporting_period = col_character(),
          owner_org = col_character(),
          owner_org_title = col_character(),
          procurement_id = col_character(),
          ...1 = col_double()
)

# Note: for excluded columns, see bottom of script

file_site = "/cloud/project/cleaning_script/read_SQL/clean_contracts_efficient_250820/raw_contracts250820a.csv"
dataz <- read_csv(file = file_site,trim_ws = TRUE,skip = 0, col_types = a, locale = readr::locale(encoding = "UTF-8"))
str(dataz)
dataz <- as.data.frame(dataz)
dataz$vendor_name <- trimws(dataz$vendor_name)
unique(dataz$economic_object_code)

# Get the dimensions of the data 'before' & 'after' filtering out 'NAs'
dim(dataz) #111190     23
gfz <- dataz
gfz <- gfz %>% drop_na(vendor_name)
dim(gfz) #111189     23
gfz <- as.data.frame(gfz)

# Filter dates only after March 31, 2019
gfz$contract_date <- as.Date(gfz$contract_date)
gfz$contract_period_start <- as.Date(gfz$contract_period_start)
gfz$delivery_date <- as.Date(gfz$delivery_date)
str(gfz)

gf1z <- filter(gfz, gfz$contract_date > as.Date("2019-03-31"))
dim(gf1z )  #53859    23
gf1z $year_mon <- as.yearmon(gf1z $contract_date)
summary(gf1z $contract_date)

# Bring in Alex's cleaned names
ddtest <- read_csv(file = "/cloud/project/cleaning_script/alexs_fixed_vendor_names.csv")
dd <- fread(file = "/cloud/project/cleaning_script/alexs_fixed_vendor_names.csv", select=c("vendor_name","A/J Tag","Vendor name - 12 left characters","contract_date"))

dd$year_mon <- as.yearmon(dd$contract_date)
dd <- dd %>% filter(year_mon > "Mar 2019")
dim(dd) #39169    57
# Change a particular column name
names(dd)[names(dd) == 'Vendor name - 12 left characters'] <- 'digit_12_name'
hf <- as.data.frame(dd)
dim(hf) #39169    57
head(hf)
str(hf)

# Fix some names
hf$digit_12_name <- ifelse(hf$digit_12_name == "BDO Canada L", gsub("BDO Canada L", "BDO CANADA L", hf$digit_12_name), hf$digit_12_name)
hf$digit_12_name <- ifelse(hf$digit_12_name == "Bdo Canada L", gsub("Bdo Canada L", "BDO CANADA L", hf$digit_12_name), hf$digit_12_name)

hf$digit_12_name <- ifelse(hf$digit_12_name == "Altis HR", gsub("Altis HR", "ALTIS HR", hf$digit_12_name), hf$digit_12_name)

hf$digit_12_name <- ifelse(hf$digit_12_name == "Adga Group C", gsub("Adga Group C", "ADGA GROUP C", hf$digit_12_name), hf$digit_12_name)
hf$digit_12_name <- ifelse(hf$digit_12_name == "ADGA Group C", gsub("ADGA Group C", "ADGA GROUP C", hf$digit_12_name), hf$digit_12_name)
hf$digit_12_name <- ifelse(hf$digit_12_name == "Cache Comput", gsub("Cache Comput", "CACHE COMPUT", hf$digit_12_name), hf$digit_12_name)
hf$digit_12_name <- ifelse(hf$digit_12_name == "Calian", gsub("Calian", "CALIAN", hf$digit_12_name), hf$digit_12_name)
hf$digit_12_name <- ifelse(hf$digit_12_name == "Cistel Techn", gsub("Cistel Techn", "CISTEL TECHN", hf$digit_12_name), hf$digit_12_name)
hf$digit_12_name <- ifelse(hf$digit_12_name == "Cofomo", gsub("Cofomo", "COFOMO", hf$digit_12_name), hf$digit_12_name)
hf$digit_12_name <- ifelse(hf$digit_12_name == "Donna Cona", gsub("Donna Cona", "DONNA CONA", hf$digit_12_name), hf$digit_12_name)
hf$digit_12_name <- ifelse(hf$digit_12_name == "Eagle Profes", gsub("Eagle Profes", "EAGLE PROFES", hf$digit_12_name), hf$digit_12_name)
hf$digit_12_name <- ifelse(hf$digit_12_name == "Equasion Bus", gsub("Equasion Bus", "EQUASION BUS", hf$digit_12_name), hf$digit_12_name)
hf$digit_12_name <- ifelse(hf$digit_12_name == "Ibiska", gsub("Ibiska", "IBISKA", hf$digit_12_name), hf$digit_12_name)

hf$digit_12_name <- ifelse(hf$digit_12_name == "It/Net Ottaw", gsub("It/Net Ottaw", "IT/NET OTTAW", hf$digit_12_name), hf$digit_12_name)
hf$digit_12_name <- ifelse(hf$digit_12_name == "IT/Net Ottaw", gsub("IT/Net Ottaw", "IT/NET OTTAW", hf$digit_12_name), hf$digit_12_name)
hf$digit_12_name <- ifelse(hf$digit_12_name == "IT/NET Ottaw", gsub("IT/NET Ottaw", "IT/NET OTTAW", hf$digit_12_name), hf$digit_12_name)

hf$digit_12_name <- ifelse(hf$digit_12_name == "T E S CONTRACT SERVICES INC.", gsub("T E S CONTRACT SERVICES INC.", "T.E.S. Contrac Services Inc.", hf$digit_12_name), hf$digit_12_name)
# Fix names under 'A_J_Tag' = Other
hf$digit_12_name <- ifelse(hf$digit_12_name == "2Keys Corpor", gsub("2Keys Corpor", "2KEYS CORPOR", hf$digit_12_name), hf$digit_12_name)
hf$digit_12_name <- ifelse(hf$digit_12_name == "Alika Intern", gsub("Alika Intern", "ALIKA INTERN", hf$digit_12_name), hf$digit_12_name)
hf$digit_12_name <- ifelse(hf$digit_12_name == "Colliers Pro", gsub("Colliers Pro", "COLLIERS PRO", hf$digit_12_name), hf$digit_12_name)
hf$digit_12_name <- ifelse(hf$digit_12_name == "Englobe Corp", gsub("Englobe Corp", "ENGLOBE CORP", hf$digit_12_name), hf$digit_12_name)
hf$digit_12_name <- ifelse(hf$digit_12_name == "King Hoe Exc", gsub("King Hoe Exc", "KING HOE EXC", hf$digit_12_name), hf$digit_12_name)
hf$digit_12_name <- ifelse(hf$digit_12_name == "Mcw Custom E", gsub("Mcw Custom E", "MCW Custom E", hf$digit_12_name), hf$digit_12_name)
hf$digit_12_name <- ifelse(hf$digit_12_name == "Qm Gp Inc. ,", gsub("Qm Gp Inc. ,", "QM GP INC. ,", hf$digit_12_name), hf$digit_12_name)

dd <- hf
rm("hf")

# To get unique vendor and 'A J tag'
# Subset only 2 columns from the data from Alex (where Vendor_names fixed)
# Take the unique rows of 'A/J Tag' & vendor_name
dff <- as.data.frame(table(dd$`A/J Tag`, dd$vendor_name))
str(dff) #54252 3
dff <- dff %>% filter(Freq != 0)
dff <- dff %>% select(-Freq)
# Change column names and order them as follows:
colnames(dff) <- c("A_J_tag","vendor")
dff <- dff %>% arrange(A_J_tag,vendor)
str(dff) #9042 2

names(dd)
ddp <- dd %>% select(digit_12_name,vendor_name)
names(ddp)
dim(ddp) #39169     2
ddp1 <- unique(ddp) ; dim(ddp1)  #9057 2
unique(ddp1)
dft <- as.data.frame(ddp1)
str(dft)
colnames(dft) <- c("digit_12_name","vendor")
dft <- dft %>% arrange(digit_12_name,vendor)
str(dft)
head(dft) ; head(dff)

# Join dft and dff above, to have unique vendor, 'A J tag' and '12 digit name' table
dft_dff <- sqldf::sqldf('SELECT * FROM dft w LEFT JOIN dff e ON w.vendor = e.vendor ORDER BY w.vendor')
dft_dff <- dft_dff[,c(1:3)]
head(dft_dff)
str(dft_dff)

# Join the original dataset 'gf1z' (from Open Canada) with dft_dff (above), so the 'raw data' is mapped to the '12 digit name' and 'A J tag'
dt <- sqldf::sqldf('SELECT * FROM gf1z w LEFT JOIN dft_dff e1 ON w.vendor_name = e1.vendor ORDER BY w.vendor_name')
dt_z <- sqldf::sqldf('SELECT * FROM gf1z w FULL JOIN dft_dff e1 ON w.vendor_name = e1.vendor ORDER BY w.vendor_name')

dt <- dt %>% arrange(year_mon)
dt_z <- dt_z %>% arrange(year_mon)
dim(dt) #53964    27
dim(dt_z) #55112    27

# Send the '12 character name' and corresponding vendor_name to the db
vendor_name_key <- dt_z %>% select(vendor_name,digit_12_name,`A_J_tag`)
dim(vendor_name_key) #55112     3
vendor_name_key<- unique(vendor_name_key)
dim(vendor_name_key) #11870     3
vendor_name_key <- vendor_name_key %>% arrange(vendor_name)
names_names1 <- c("vendor_name", "digit_12_name",  "A_J_tag")
head(vendor_name_key)

# Generate a 9 character alphanumeric string to join on later
# Create a nine digit unique identifier column
set.seed(645)
myFun <- function(n = 500000) {
  a <- do.call(paste0, replicate(5, sample(LETTERS, n, TRUE), FALSE))
  paste0(a, sprintf("%04d", sample(9999, n, TRUE)), sample(LETTERS, n, TRUE))
}

yy <- nrow(dt)
rand_val <- as.data.frame(myFun(yy))
colnames(rand_val) <- c("join_key")

# Add a column with 9 random digits and add it to the 'nearly raw' data i.e. dt
dgd <- cbind(rand_val,dt)
length(unique(dgd$join_key)) #53964    
dim(dgd) #53964 
names(dgd)

# From here will add:
# a) 4 rules to filter 
# b) add the MAGS designation under 'A/J Tag'
# Filter out only these columns from dgd (most recent cleaning)
d_filter <- dgd %>% select(join_key,procurement_id,contract_date,contract_value,original_value,amendment_value)
d_filter <- d_filter %>% arrange(contract_date,procurement_id)
head(d_filter)
# Make the rownames equal to the 9 digit random string
row.names(d_filter) <- d_filter$join_key
cat("\014")
dim(d_filter) #53962     6
# 1st simple remove duplicates when including these
# Remove the join_key column and then take the unique values -> NOT sure yet if this is needed
d_filter <- unique(d_filter) 
str(d_filter)
dim(d_filter) #53962     6
head(d_filter, n=10)
specify_decimal <- function(x, k) trimws(format(round(x, k), nsmall=k))
specify_decimal(1234, 5)
specify_decimal(1234, 0)
str(d_filter)
# Now must remove duplicate procurement id's according to certain rules

d_filter$amendment_value <- specify_decimal(d_filter$amendment_value,0)
str(d_filter)
d_filter$amendment_value <- as.numeric(d_filter$amendment_value)
str(d_filter)
# Within the the 'amendment value' from smallest to largest as grouped by 'procurement id'
d_f <- d_filter %>% 
  dplyr::group_by(procurement_id) %>%
  dplyr::mutate(r = rank(desc(amendment_value), ties.method = "first"))
# Then this ranks the contract date from smallest to largest as grouped by 'procurement id'
d_fa <- d_f %>% 
  dplyr::group_by(procurement_id) %>%
  dplyr::mutate(r_date = rank(desc(contract_date), ties.method = "first"))

d_fa <- as.data.frame(d_fa)
dim(d_fa) #53962     8
str(d_fa)

d_fa_sub <- d_fa %>% select(join_key, procurement_id,amendment_value)
d_fa_sub <- as.data.frame(d_fa_sub)

d_fa_sub1 <- d_fa_sub %>% 
  dplyr::group_by(procurement_id) %>% mutate(All_below0 = 
                                               ifelse(any(amendment_value > 0, na.rm = TRUE), "above", 
                                                      ifelse(any(amendment_value <= .9 & amendment_value >= 0.8, na.rm = TRUE), "yellow","below0")))

# Code to change  Na values if necessary
# Merge d_fa_sub1 & d_fa on join_key
d_fa_sub1 <- as.data.frame(d_fa_sub1)
d_fa <- as.data.frame(d_fa)

d_f_pre <- sqldf::sqldf('SELECT * FROM d_fa w LEFT JOIN d_fa_sub1 e1 ON w.join_key = e1.join_key ORDER BY w.join_key')

d_f_pre <- d_f_pre[c("join_key", "procurement_id", "contract_date", "contract_value", 
                     "original_value", "amendment_value", "r", "r_date",  
                     "All_below0")]

d_f <- d_f_pre %>% 
  dplyr::group_by(procurement_id) %>% 
  mutate(group_id = cur_group_id()) %>% 
  add_count(group_id, sort = T) %>% 
  arrange( desc(All_below0),procurement_id)

dim(d_f) #53962    11
# Check -> these 2 below should be the same
length(d_f$group_id %>% unique()) #36368
length(d_f$procurement_id %>% unique()) #36368

rm(list = c("a",  "d_f_pre", "d_fa", "d_fa_sub", "d_fa_sub1", "d_filter", 
            "dataz", "dd", "ddp", "ddp1", "ddtest", "dff", "dft", "dft_dff", 
            "dt", "dt_z", "file_site", "gf1z", "gfz", "myFun", "names_names1", 
            "rand_val", "specify_decimal", "vendor_name_key", "yy"))

d_f1_pre <- d_f %>%  group_by(group_id) %>% mutate(same_date = n_distinct(contract_date) ) %>% ungroup
d_f1_pre$All_below0 <- ifelse(d_f1_pre$All_below0 == "below0" & d_f1_pre$same_date == 1, gsub("below0","below0_sameDate", d_f1_pre$All_below0), d_f1_pre$All_below0 )
table(d_f1_pre$All_below0)

d_f1 <- d_f1_pre %>% group_by(procurement_id) %>%
  mutate(to_keep = if_else(r_date == 1  & any(All_below0 == "below0", na.rm = TRUE), "keep", 
                           if_else(r == 1  & any(All_below0 == "below0_sameDate", na.rm = TRUE), "keep",
                                   if_else(r == 1  & any(All_below0 == "above", na.rm = TRUE), "keep", "discard"))))

table(d_f1$to_keep) #36368 keep ; 17594 discard

dtt <- d_f1 %>% filter(to_keep == "keep") %>% select(join_key,procurement_id,contract_date,contract_value,original_value,amendment_value)
names(dtt)
dim(dtt)

length(unique(dtt$procurement_id))
dtt <- as.data.frame(dtt)
dim(dtt) #36368     6

dtt1 <- sqldf::sqldf('SELECT * FROM dtt w LEFT JOIN dgd e ON w.join_key = e.join_key ORDER BY w.procurement_id')
names(dtt1)[names(dtt1) == '...1'] <- 'index'
#edit(names(dtt1))
c("join_key", "procurement_id", "contract_date", "contract_value", 
  "original_value", "amendment_value", "join_key", "index", "V1", 
  "reference_number", "vendor_name", "vendor_postal_code", "contract_date", 
  "economic_object_code", "contract_period_start", "delivery_date", 
  "contract_value", "original_value", "amendment_value", "commodity_type", 
  "commodity_code", "contracting_entity", "ministers_office", "reporting_period", 
  "owner_org", "owner_org_title", "procurement_id", "year_mon", 
  "digit_12_name", "vendor", "A_J_tag")



dtt2 <- dtt1[c("join_key", "procurement_id", "contract_date", "contract_value", 
               "original_value", "amendment_value", "join_key", "index", "V1", 
               "reference_number", "vendor_name", "vendor_postal_code", "contract_date", 
               "economic_object_code", "contract_period_start", "delivery_date", 
               "contract_value", "original_value", "amendment_value", "commodity_type", 
               "commodity_code", "contracting_entity", "ministers_office", "reporting_period", 
               "owner_org", "owner_org_title", "procurement_id", "year_mon", 
               "digit_12_name", "vendor", "A_J_tag")]

dtt2  <- dtt2 %>% arrange(procurement_id)
dim(dtt2) #36368     31

dt <- dtt2
rm(dtt2,dtt1)

# Classify the type of account PA (Priority account), GA (Growth account) or OA (Opportunistic account) creating PA_GA_CA
pa <- str_detect(dt$owner_org, fixed("cbsa-", ignore_case=TRUE)) | 
  str_detect(dt$owner_org, fixed("cmhc", ignore_case=TRUE)) |   
  str_detect(dt$owner_org, fixed("esdc", ignore_case=TRUE)) | 
  str_detect(dt$owner_org, fixed("esdc-", ignore_case=TRUE)) | 
  str_detect(dt$owner_org, fixed("cic", ignore_case=TRUE)) |
  str_detect(dt$owner_org, fixed("ps-sp", ignore_case=TRUE)) |
  str_detect(dt$owner_org, fixed("rcmp-", ignore_case=TRUE))   

ga <- str_detect(dt$owner_org, fixed("csc-", ignore_case=TRUE)) | 
  str_detect(dt$owner_org, fixed("dnd-", ignore_case=TRUE)) | 
  str_detect(dt$owner_org, fixed("ised", ignore_case=TRUE)) | 
  str_detect(dt$owner_org, fixed("phac-", ignore_case=TRUE))  

innov_can <- dt$owner_org == "ic"

opport_acc <- str_detect(dt$owner_org, fixed("cra-arc", ignore_case=TRUE)) | 
  str_detect(dt$owner_org, fixed("jus", ignore_case=TRUE)) | 
  str_detect(dt$owner_org, fixed("isc-sac", ignore_case=TRUE)) | 
  str_detect(dt$owner_org, fixed("nrcan-rncan", ignore_case=TRUE)) |
  str_detect(dt$owner_org, fixed("pwgsc-tpsgc", ignore_case=TRUE)) |
  str_detect(dt$owner_org, fixed("ssc-spc", ignore_case=TRUE)) |
  str_detect(dt$owner_org, fixed("tbs-sct", ignore_case=TRUE)) 

transp_can <- dt$owner_org == "tc"

dt$PA_GA_OA <- ifelse(pa, "Priority accounts", 
                      ifelse( ga, "Growth accounts",
                              ifelse( innov_can, "Growth accounts", 
                                      ifelse( opport_acc, "Opportunistic Accounts", 
                                              ifelse( transp_can, "Opportunistic Accounts",
                                                      "Other accounts")))))

dt <- dt %>% mutate(PA_GA_OA = ifelse(grepl("cics-scic",owner_org),"Other accounts",PA_GA_OA))

# Add description for economic object code
str(dt$economic_object_code)
dt$economic_object_code <- as.character(dt$economic_object_code)
unique(dt$economic_object_code)
df <- dt

# Add code descriptions
df$code_descr <-        ifelse(df$economic_object_code == "499", "Under Other services - Other business services not elsewhere specified",
                               ifelse(df$economic_object_code == "491", "Management consulting",
                                      ifelse(df$economic_object_code == "473", "Information technology and telecommunications consultants",  
                                             ifelse(df$economic_object_code == "492", "Research contracts", 
                                                    ifelse(df$economic_object_code == "859", "Under Business services - Other business services not elsewhere specified",
                                                           ifelse(df$economic_object_code == "446", "Training consultants",
                                                                  ifelse(df$economic_object_code == "448", "Purchase of training packages and courses",  
                                                                         ifelse(df$economic_object_code == "1285", "Application software (including COTS) and application development and delivery software", 
                                                                                ifelse(df$economic_object_code == "813", "Temporary help services",
                                                                                       ifelse(df$economic_object_code == "430", "Scientific services",
                                                                                              ifelse(df$economic_object_code == "423", "Engineering consultants - Other",  
                                                                                                     ifelse(df$economic_object_code == "1282", "Computer equipment related to Production and Operations (P&O) environment - All servers, storage, printers, etc. (includes all related parts and peripherals)", 
                                                                                                            ifelse(df$economic_object_code == "362", "Data and database access services",
                                                                                                                   ifelse(df$economic_object_code == "823", "Conference fees",
                                                                                                                          ifelse(df$economic_object_code == "812", "Computer services (includes IT solutions/deliverables as well as IT managed services)",  
                                                                                                                                 ifelse(df$economic_object_code == "475", "Information technology services",  
                                                                                                                                        ifelse(df$economic_object_code == "474", "Information management services",  
                                                                                                                                               ifelse(df$economic_object_code == "472", "under Informatics services - NO other description provided",                   
                                                                                                                                                      
                                                                                                                                                      "nother"))))))))))))))))))

# Recreate df from dt
# Create the 'Fiscal year' column
df = df %>% 
  mutate(date = ymd(contract_date)) %>% 
  mutate_at(vars(date), funs(year, month, day))
df$year <- as.character(df$year)

# Criteria
# Jan 1st 2019 - March 31st 2019 = FY 18-19
# April 1st 2019-March 31st 2020 = FY 19-20
# April 1st 2020-March 31st 2021 = FY 20-21
# April 1st 2021-March 31st 2022 = FY 21-22

df$fiscal_year <- ifelse(df$year == "2019" & df$month < 4, "FY 18-19", 
                         ifelse(df$year == "2019" & df$month >= 4, "FY 19-20",  
                                ifelse(df$year == "2020" & df$month < 4, "FY 19-20",
                                       ifelse(df$year == "2020" & df$month >= 4, "FY 20-21",
                                              ifelse(df$year == "2021" & df$month < 4, "FY 20-21",
                                                     ifelse(df$year == "2021" & df$month >= 4, "FY 21-22",
                                                            ifelse(df$year == "2022" & df$month < 4, "FY 21-22",
                                                                   ifelse(df$year == "2022" & df$month >= 4, "FY 22-23",
                                                                          ifelse(df$year == "2023" & df$month < 4, "FY 22-23",
                                                                                 ifelse(df$year == "2023" & df$month >= 4, "FY 23-24",
                                                                                        ifelse(df$year == "2024" & df$month < 4, "FY 23-24",
                                                                                               ifelse(df$year == "2024" & df$month >= 4, "FY 24-25",
                                                                                                      ifelse(df$year == "2025" & df$month < 4, "FY 24-25",
                                                                                                             ifelse(df$year == "2025" & df$month >= 4, "FY 25-26",
                                                                                               "other"))))))))))))))

unique(df$fiscal_year)
table(df$fiscal_year)
names(df)
df_reserve <- df

# Create new designation under "A_J_tag" for MAGS
# This designation includes:
# 1 - Microsoft  * previously under 'Tech'
# 2 - SAP CANADA I * previously under 'Tech'
# 3 - SAP CANADA I * previously under 'Tech'
# 4 - Google -> must be grep searched in the vendor_name column; A_J_Tag must be changed from 'Not Tagged' to MAGS
# 5 - Salesforce -> must be grep searched in the vendor_name column; A_J_Tag must be changed from 'Not Tagged' to MAGS

df$`A_J_tag` <- ifelse(df$digit_12_name == "Microsoft", gsub("Tech", "MAGS", df$`A_J_tag`), df$`A_J_tag`)
df$`A_J_tag` <- ifelse(df$digit_12_name == "SAP CANADA I", gsub("Tech", "MAGS", df$`A_J_tag`), df$`A_J_tag`)
df$`A_J_tag` <- ifelse(df$digit_12_name == "Amazon Web Services", gsub("Tech", "MAGS", df$`A_J_tag`), df$`A_J_tag`)

df$`A_J_tag` <- ifelse(df$digit_12_name == "GOOGLE CLOUD", gsub("Not Tagged", "MAGS", df$`A_J_tag`), df$`A_J_tag`)
df$`A_J_tag` <- ifelse(df$digit_12_name == "SALESFORCE.C", gsub("Not Tagged", "MAGS", df$`A_J_tag`), df$`A_J_tag`)
# Add amendment for 'KYNDRYL CANA' -> add them to '4+1', instead of 'Other' under the A_J_tag (amended 230201)
df$`A_J_tag` <- ifelse(df$digit_12_name == "KYNDRYL CANA", gsub("Other", "4 + 1", df$`A_J_tag`), df$`A_J_tag`)

# Fix some names
df$digit_12_name <- ifelse(df$digit_12_name == "BDO Canada L", gsub("BDO Canada L", "BDO CANADA L", df$digit_12_name), df$digit_12_name)
df$digit_12_name <- ifelse(df$digit_12_name == "Bdo Canada L", gsub("Bdo Canada L", "BDO CANADA L", df$digit_12_name), df$digit_12_name)

df$digit_12_name <- ifelse(df$digit_12_name == "Altis HR", gsub("Altis HR", "ALTIS HR", df$digit_12_name), df$digit_12_name)

df$digit_12_name <- ifelse(df$digit_12_name == "Adga Group C", gsub("Adga Group C", "ADGA GROUP C", df$digit_12_name), df$digit_12_name)
df$digit_12_name <- ifelse(df$digit_12_name == "ADGA Group C", gsub("ADGA Group C", "ADGA GROUP C", df$digit_12_name), df$digit_12_name)

df$digit_12_name <- ifelse(df$digit_12_name == "Cache Comput", gsub("Cache Comput", "CACHE COMPUT", df$digit_12_name), df$digit_12_name)
df$digit_12_name <- ifelse(df$digit_12_name == "Calian", gsub("Calian", "CALIAN", df$digit_12_name), df$digit_12_name)
df$digit_12_name <- ifelse(df$digit_12_name == "Cistel Techn", gsub("Cistel Techn", "CISTEL TECHN", df$digit_12_name), df$digit_12_name)
df$digit_12_name <- ifelse(df$digit_12_name == "Cofomo", gsub("Cofomo", "COFOMO", df$digit_12_name), df$digit_12_name)
df$digit_12_name <- ifelse(df$digit_12_name == "Donna Cona", gsub("Donna Cona", "DONNA CONA", df$digit_12_name), df$digit_12_name)
df$digit_12_name <- ifelse(df$digit_12_name == "Eagle Profes", gsub("Eagle Profes", "EAGLE PROFES", df$digit_12_name), df$digit_12_name)
df$digit_12_name <- ifelse(df$digit_12_name == "Equasion Bus", gsub("Equasion Bus", "EQUASION BUS", df$digit_12_name), df$digit_12_name)
df$digit_12_name <- ifelse(df$digit_12_name == "Ibiska", gsub("Ibiska", "IBISKA", df$digit_12_name), df$digit_12_name)

df$digit_12_name <- ifelse(df$digit_12_name == "It/Net Ottaw", gsub("It/Net Ottaw", "IT/NET OTTAW", df$digit_12_name), df$digit_12_name)
df$digit_12_name <- ifelse(df$digit_12_name == "IT/Net Ottaw", gsub("IT/Net Ottaw", "IT/NET OTTAW", df$digit_12_name), df$digit_12_name)
df$digit_12_name <- ifelse(df$digit_12_name == "IT/NET Ottaw", gsub("IT/NET Ottaw", "IT/NET OTTAW", df$digit_12_name), df$digit_12_name)

df$digit_12_name <- ifelse(df$digit_12_name == "T E S CONTRACT SERVICES INC.", gsub("T E S CONTRACT SERVICES INC.", "T.E.S. Contrac Services Inc.", df$digit_12_name), df$digit_12_name)
#fix names under 'A_J_Tag' = Other
df$digit_12_name <- ifelse(df$digit_12_name == "2Keys Corpor", gsub("2Keys Corpor", "2KEYS CORPOR", df$digit_12_name), df$digit_12_name)
df$digit_12_name <- ifelse(df$digit_12_name == "Alika Intern", gsub("Alika Intern", "ALIKA INTERN", df$digit_12_name), df$digit_12_name)
df$digit_12_name <- ifelse(df$digit_12_name == "Colliers Pro", gsub("Colliers Pro", "COLLIERS PRO", df$digit_12_name), df$digit_12_name)
df$digit_12_name <- ifelse(df$digit_12_name == "Englobe Corp", gsub("Englobe Corp", "ENGLOBE CORP", df$digit_12_name), df$digit_12_name)
df$digit_12_name <- ifelse(df$digit_12_name == "King Hoe Exc", gsub("King Hoe Exc", "KING HOE EXC", df$digit_12_name), df$digit_12_name)
df$digit_12_name <- ifelse(df$digit_12_name == "Mcw Custom E", gsub("Mcw Custom E", "MCW Custom E", df$digit_12_name), df$digit_12_name)
df$digit_12_name <- ifelse(df$digit_12_name == "Qm Gp Inc. ,", gsub("Qm Gp Inc. ,", "QM GP INC. ,", df$digit_12_name), df$digit_12_name)
###################################################################################################################################
###################################################################################################################################
#edit(names(df))
df$year_mon <- as.character(df$year_mon)
y <- nrow(df)
df$index <- 1:y
##View(df)
#check -> these 2 below should be the same
length(df$group_id %>% unique()) #28500
length(df$procurement_id %>% unique()) #36433
#edit(names(df))

df1 <- df[c("join_key", "procurement_id", "contract_date", "contract_value", 
            "original_value", "amendment_value", "join_key.1", "index", "V1", 
            "reference_number", "vendor_name", "vendor_postal_code", "contract_date.1", 
            "economic_object_code", "contract_period_start", "delivery_date", 
            "contract_value.1", "original_value.1", "amendment_value.1", 
            "commodity_type", "commodity_code", "contracting_entity", "ministers_office", 
            "reporting_period", "owner_org", "owner_org_title", "procurement_id.1", 
            "year_mon", "digit_12_name", "vendor", "A_J_tag", "PA_GA_OA", 
            "code_descr", "date", "year", "month", "day", "fiscal_year")]

#copy_to(con, df1, "cleaned_contracts_230210",temporary = FALSE, overwrite = TRUE, append = FALSE)
#copy_to(con1, df, "processed_data_flow_data_with_AJtag_2nd",temporary = FALSE, overwrite = TRUE, append = FALSE)
##View(df)

#RPostgres::dbListTables(con)
#edit(ls())
rm( list = c("d_f", "d_f1", "d_f1_pre", "df", "df_reserve",  "dgd", 
             "dt", "dtt", "ga", "innov_can", "opport_acc", "pa", "transp_can", 
             "y"))

df1 <- df1 %>% arrange(vendor_name, desc(contract_date))

con <- DBI::dbConnect(duckdb::duckdb(), "contracts.duckdb")
DBI::dbWriteTable(con, "cleaned_contracts_tbl250821", df1, overwrite = TRUE)
yy <- write.csv(df1, file="/cloud/project/cleaning_script/read_SQL/clean_contracts_efficient_250820/cleaned_contract_data_250821.csv", row.names = FALSE)
rm(list = setdiff(ls(), "con"))

tables_info <- DBI::dbGetQuery(con, "SHOW TABLES")
print(tables_info)
# Method: Using dbplyr with tbl() and collect() (recommended)
df <- tbl(con, "cleaned_contracts_tbl250821") %>% collect()
#View(df)



# unique(df$A_J_tag)
# df_sub <- df %>% select(digit_12_name, A_J_tag) %>% filter(df$A_J_tag != "Not Tagged")
# unique(df_sub$A_J_tag)
# df_sub1 <- unique(df_sub)
# #View(df_sub1)
# df_sub1a <- df_sub1 %>% arrange(A_J_tag, digit_12_name)
# #View(df_sub1a)

#copy to AWS database
#Connect to AWS 'postgresaws'
#x <- system("ifconfig", intern=TRUE)
library(devtools)
#install.packages("devtools")
#install_github("gregce/ipify")
# library(ipify)
# get_ip()

#conz <- RPostgres::dbConnect(
# RPostgres::Postgres(),
##  # without the previous and next lines, some functions fail with bigint data 
##  #   so change int64 to integer
#  bigint = "integer",  
# host = "postgresaws.cntnevvp6jpo.ca-central-1.rds.amazonaws.com",
# port = 5432,
# user = "postgres",
#password = rstudioapi::askForPassword("Database password"),
# password = "lopes123",
# dbname = "postgres"
#)


#list the tables in the DB called postgres_R
#RPostgres::dbListTables(conz)
#copy_to(conz, df1, "processed_data_flow_data_with_AJtag_2nd",temporary = FALSE, overwrite = TRUE, append = FALSE)
#View(df1)
#check - each of the 3 lines of code below should give the same number
length(unique(df$join_key)) #45888    38
length(unique(df$procurement_id)) #45888    38
dim(df) #45888    38

#cp cleaning_backup2302010_amended.R ~/airflow/scripts/clean_contracts_v2_230210.R

bfive <-  df %>% select(vendor_name,digit_12_name, A_J_tag, owner_org, contract_date, year_mon, fiscal_year, contract_period_start, delivery_date, contract_value) #%>% filter(df$A_J_tag != "Not Tagged")
#View(bfive)
print("Script2 done!")


