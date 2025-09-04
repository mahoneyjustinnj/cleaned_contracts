setwd("/cloud/project/cleaning_script/read_SQL/clean_contracts_efficient_250820/cleaned_contracts")
list.files()
rm(list=ls())
library("dplyr")
library("stringr")

# read in grc service contracts
df <- read.csv(file = "grc_service_contracts.csv")
names(df)
length(unique(df$vendor_postal_code))
# Count the number of NA values in the vendor_postal_co<- column
na_count <- sum(is.na(df$vendor_postal_code))
# Print the result
print(na_count)
dim(df)
table(is.na(df$vendor_postal_code))
str(df$vendor_postal_code)



# Use clustering or similarity to group and fix names

c("vendor_name", "vendor_postal_code", "owner_org", "owner_org_title", 
  "contract_value", "commodity_code", "commodity_type", "economic_object_code", 
  "contract_date", "contract_period_start", "delivery_date", "reporting_period", 
  "contracting_entity", "commodity_type_describe", "commodity_type1", "GSIN.code", "GSIN.Description", "Commodity.Type")

dt <- df %>% select(
  "vendor_name",
  "contract_date",
  "vendor_postal_code",
  "contract_value",
  "commodity_code",
  "GSIN.Description",
  "economic_object_code",
  "contract_period_start",
  "delivery_date",
  "reporting_period",
  "GSIN.Description"
)
names(dt)
dt <- dt %>% rename(commodity_code_descn = GSIN.Description) %>% arrange(vendor_name,desc( contract_date))
#View(dt)
#operate on economic_object_code to make a character string and add a 0 at front
dt$economic_object_code <- as.character(dt$economic_object_code)
dt$economic_object_code <- str_c("0", dt$economic_object_code)
unique(dt$economic_object_code)

library(readr)
econ_obj_code <- read_csv("econ_obj_code.csv") %>% select("Code_OBJ-ART", "Name-Nom_OBJ-ART")  #%>% View()
colnames(econ_obj_code) <- c("economic_object_code", "economic_object_code_descn")
y <- write.csv(econ_obj_code, file ="eco_obj_code_TO_USE.csv", row.names=FALSE)
#View(econ_obj_code)
names(econ_obj_code)
#left join
dt_joined <- dt %>% left_join(econ_obj_code, by = "economic_object_code")
cols <- c("vendor_name", "contract_date", "vendor_postal_code", "contract_value", 
          "commodity_code", "commodity_code_descn", "economic_object_code", "economic_object_code_descn",
          "contract_period_start", "delivery_date", "reporting_period")
dt_joined <- dt_joined[ , cols]
#View(dt_joined)
#edit(names(dt_joined))
#fix misspelled vendor_name
dd <- dt_joined
dd <- dd %>%
  mutate(Vendor = case_when(
    str_detect(vendor_name, "^ACCENTURE") ~ "Accenture",
    str_detect(vendor_name, "^ALTIS RECRUITMENT") ~ "Altis Recruitment",
    str_detect(vendor_name, "^COFOMO") ~ "Cofomo",
    str_detect(vendor_name, "^DALIAN") ~ "Dalian",
    str_detect(vendor_name, "^DR. ROBERT J. DAIGLE") ~ "Dr. Robert Daigle",
    str_detect(vendor_name, "^GARTNER") ~ "Gartner Canada",
    str_detect(vendor_name, "^KAREN MICHELLE BURKE") ~ "Michelle Burke",
    str_detect(vendor_name, "^MICHELLE BURKE") ~ "Michelle Burke",
    str_detect(vendor_name, "^S. I. SYSTEMS") ~ "S. I. Systems",
    str_detect(vendor_name, "^S.I. SYSTEMS") ~ "S. I. Systems",
    str_detect(vendor_name, "^S.I. SYSTEMS PARTNERSHIP") ~ "S. I. Systems Partnership",
    TRUE ~ vendor_name
  )) %>% arrange(Vendor)

dd <- dd %>%
  select(
    vendor_name,
    Vendor,
    everything()
  )

dd <- dd %>% select(-vendor_name)
#View(dd)

# Step 1: Compute the most frequent postal code per Vendor
postal_mode <- dd %>%
  filter(!is.na(vendor_postal_code)) %>%
  group_by(Vendor, vendor_postal_code) %>%
  summarise(n = n(), .groups = "drop") %>%
  group_by(Vendor) %>%
  slice_max(n, n = 1, with_ties = FALSE) %>%
  select(Vendor, postal_mode = vendor_postal_code)
#View(postal_mode)

# Step 2: Left join the mode back to the original data
de <- dd %>%
  left_join(postal_mode, by = "Vendor") %>%
  mutate(vendor_postal_code = if_else(
    is.na(vendor_postal_code),
    postal_mode,
    vendor_postal_code
  )) #%>% select(-postal_mode)
names(de)

de <- de %>%
  select(
    Vendor,
    postal_mode,
    vendor_postal_code,
    everything()
  ) %>% select(-vendor_postal_code)
View(de)

de <- de %>% rename(postal_code_vendor = postal_mode)

#fill in missing postal codes
de <- de %>%
  mutate(postal_code_vendor = case_when(
    Vendor == "ARCHITECTURE49 INC" ~ "R3T",
    Vendor == "ASSOCIATED AMBULANCE & SERVICES" ~ "T0A",
    Vendor == "ASSOCIATION OF CANADIAN FINANCIAL" ~ "K1Z",
    Vendor == "BABEL STREET" ~ "20190",
    Vendor == "BLOOMBERG FINANCE L.P." ~ "M5J",
    Vendor == "CAMPION COLLEGE" ~ "S4S",
    Vendor == "CEB INC." ~ "M5A",
    Vendor == "COGENT SYSTEMS INC." ~ "N5V",
    Vendor == "D4IS SOLUTIONS INC" ~ "H3N",
    Vendor == "DR TERENCE FOGWILL" ~ "A1A",
    Vendor == "DR. BRAD KELLN" ~ "B3J",
    Vendor == "DR. JAMES PAUL HICKEY" ~ "A1B",
    Vendor == "DRS. JAMES CONSULTING LLC" ~ "99223-6223",
    Vendor == "ESRI CANADA LIMITED" ~ "M3C",
    Vendor == "EVISION INC." ~ "H3J",
    Vendor == "HARRIS CANADA SYSTEM INC." ~ "V7X",
    Vendor == "HUBSTREAM LLC" ~ "98007",
    Vendor == "IBI GROUP PROFESSIONAL SERVICES" ~ "M7A",
    Vendor == "LANSDOWNE TECHNOLOGIES INC" ~ "K1P",
    Vendor == "LEXIPOL LLC" ~ "75034",
    Vendor == "MINISTER OF FINANCE" ~ "K1A",
    Vendor == "OPENFRAME TECHNOLOGIES INC" ~ "H9H",
    Vendor == "OTTAWA VALLEY VETERINARY" ~ "K0C",
    Vendor == "PHIRELIGHT SECURITY SOLUTIONS INC" ~ "K1P",
    Vendor == "S. MARGARET GRANT" ~ "B2N",
    Vendor == "SAP CANADA INC." ~ "M2P",
    Vendor == "SCHMALZ CONSULTING LTD" ~ "T2P",
    Vendor == "UNIVERSITY OF REGINA" ~ "S4P",
    Vendor == "WESTOWER COMMUNICATIONS LTD." ~ "R3P",
    Vendor == "TECHNOLOGY AND BUSINESS CONSULTANTS" ~ NA,
    TRUE ~ postal_code_vendor
  ))
table(is.na(de$postal_code_vendor))

y <- write.csv(de, file ="grc_cleaned_final_final_250904.csv", row.names=FALSE)

de_missing <- de %>% filter(if_any(everything(), is.na) ) #%>% View(de_missing)

# install.packages("text", repos = "https://cloud.r-project.org")
# install.packages("textmineR", repos = "https://cloud.r-project.org")

# library("textmineR")
# library(text)
# 
# str(dt)
# library(text2vec)
# library(dplyr)
# library(stringr)
# 
# unlink("/cloud/lib/x86_64-pc-linux-gnu-library/4.4/00LOCK-rsparse", recursive = TRUE)
# 
# install.packages("rsparse", repos = "https://cloud.r-project.org")
