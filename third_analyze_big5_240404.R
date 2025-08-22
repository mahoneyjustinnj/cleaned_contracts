rm(list=ls())
#.rs.restartR()  #restarts R and clears memory RAM
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
library("ggplot2")
library("plotly")
sessionInfo()

file_site = "/cloud/project/cleaning_script/read_SQL/clean_contracts_efficient_250820/cleaned_contract_data_250821.csv"
df <- read_csv(file = file_site,trim_ws = TRUE,skip = 0,  locale = readr::locale(encoding = "UTF-8"))

head(df)

bfive <-  df %>% select(procurement_id,vendor_name,vendor,digit_12_name, A_J_tag, owner_org, contract_date, year_mon, fiscal_year, contract_period_start, delivery_date, contract_value) #%>% filter(df$A_J_tag != "Not Tagged")
#View(bfive)

bfive1 <- bfive %>% select(procurement_id,vendor_name,vendor,digit_12_name, A_J_tag, owner_org, contract_date, year_mon, fiscal_year, contract_period_start, delivery_date, contract_value) %>% filter(df$A_J_tag == "4 + 1")
#View(bfive1)

check <- bfive %>% select(procurement_id,vendor_name,vendor,digit_12_name, A_J_tag, owner_org, contract_date, year_mon, fiscal_year, contract_period_start, delivery_date, contract_value) %>% filter(df$A_J_tag == "Other" | is.na(df$A_J_tag) )
#View(check)
# fix Accenture INC. to 
unique(bfive$A_J_tag)

data <- bfive1
str(data)

data <- data %>% group_by(digit_12_name,fiscal_year) %>% mutate(sum_byFYear = sum(contract_value))
data <- data %>% mutate(vendor_fy = paste(digit_12_name,  fiscal_year)) 
#names(data)
#table(data$vendor_fy, data$sum_byFYear)
org_Fyear <- data %>% select(vendor_fy,sum_byFYear) %>% unique() %>% arrange(vendor_fy) ; org_Fyear
#sort(unique(data$vendor_fy))
###################################################################################################################################

accen <- data %>% filter(digit_12_name == "Accenture") %>%  group_by(as.factor(sum_byFYear)) %>% select(fiscal_year, sum_byFYear) %>%  unique() ; accen
names(accen)
names(data)
unique(data$digit_12_name)

library(ggplot2)
library(dplyr)

p <- data %>%
  ggplot(aes(x=as.factor(fiscal_year),
             y=contract_value,
             #size = contract_value,
             color=digit_12_name,
              )) +
  geom_boxplot(color = "black") +
  geom_point(aes(size = contract_value), position = position_jitter(width = 0.2), alpha = 0.5) +  # Add points with size based on Relative_Value
  scale_size_continuous(range = c(1, 5)) +  # Adjust the range of point sizes
  stat_summary(fun.y = "mean", geom = "point", shape = 2, size = 2, color = "black",
               position = position_dodge(width = 0.75), show.legend = FALSE,
               aes(label = round(..y.., 2))) +
  theme_minimal() + # Optional:
  facet_wrap(~digit_12_name, scales = "free_y") +
  xlab("Fiscal Year") +    # Adding x-axis label
  ylab("Contract Value") +  # Adding y-axis label 
  theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +  # Rotate x-axis text vertically
  labs(color = NULL, size = "Contractor") 
  #ggplotly(p)

p <- ggplotly(p) 
p


  
  
   
  # geom_boxplot() +
# geom_point() +
# scale_color_manual(values = c("Accenture" = "black", "Deloitte" = "red","Ernst & Youn" = "turquoise", "IBM CANADA" = "lightblue",
#                               "KPMG"="yellow" , "KYNDRYL CANA" = "orange"  , "PRICEWATERHOUSE COOPERS" = "lightgreen")) +
#+
#geom_hline(yintercept=accen[[3,3]], linetype='dotted', col = 'red') +
#geom_hline(data = org_Fyear, aes(yintercept = sum_byFYear))
# geom_boxplot() + 
  # geom_point() +
  # scale_color_manual(values = c("Accenture" = "black", "Deloitte" = "red","Ernst & Youn" = "turquoise", "IBM CANADA" = "lightblue", 
  #                               "KPMG"="yellow" , "KYNDRYL CANA" = "orange"  , "PRICEWATERHOUSE COOPERS" = "lightgreen")) +
   #+
  #geom_hline(yintercept=accen[[3,3]], linetype='dotted', col = 'red') +
  #geom_hline(data = org_Fyear, aes(yintercept = sum_byFYear))

library(ggplot2)
dummy1 <- expand.grid(X = factor(c("A", "B")), Y = rnorm(10))
dummy1$D <- rnorm(nrow(dummy1))  ; dummy1
dummy2 <- data.frame(X = c("A", "B"), Z = c(1, 0)) ; dummy2
ggplot(dummy1, aes(x = D, y = Y)) + geom_point() + facet_grid(~X) + 
  geom_hline(data = dummy2, aes(yintercept = Z))
  
  
#########  
# Sample data
set.seed(123)




p <- data %>%
  ggplot(aes(x = as.factor(fiscal_year),
             y = contract_value,
             color = digit_12_name
             )) +
  geom_boxplot(color = "black") +
  geom_point(aes(size = contract_value), position = position_jitter(width = 0.2), alpha = 0.5) +
  scale_size_continuous(range = c(1, 5)) +
  stat_summary(fun.y = "mean", geom = "point", shape = 4, size = 3, color = "red",
               position = position_dodge(width = 0.75), show.legend = FALSE,
               aes(label = round(..y.., 2))) +
  theme_minimal() +
  facet_wrap(~digit_12_name, scales = "free_y") +
  xlab("Fiscal Year") +
  ylab("Contract Value") +
  theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust = 1)) +  # Rotate x-axis text vertically
  labs(color = NULL, size = "Contract Value")  # Remove color legend title, specify size legend title

p <- ggplotly(p, tooltip = c("Fiscal Year: %{x}", "Contract Value: %{y}")) %>%
  style(hoverlabel = list(bgcolor = "white", font = list(color = "black"),
                          bordercolor = "black", font = list(family = "Arial", size = 12),
                          namelength = -1))

print(p)


######

p <- data %>%
  ggplot(aes(x = as.factor(fiscal_year),
             y = contract_value,
             color = digit_12_name,
  )) +
  geom_boxplot(color = "black") +
  geom_point(aes(size = contract_value), position = position_jitter(width = 0.2), alpha = 0.5) +
  stat_summary(fun.y = "mean", geom = "point", shape = 4, size = 3, color = "red",
               position = position_dodge(width = 0.75), show.legend = FALSE,
               aes(label = round(..y.., 2))) +
  geom_text(data = data %>% group_by(digit_12_name, fiscal_year) %>% summarize(mean_value = mean(contract_value)), 
            aes(x = as.factor(fiscal_year), y = mean_value, label = round(mean_value, 0)), 
            vjust = -0.5, hjust = -0.5, color = "red", size = 3) +
  theme_minimal() +
  facet_wrap(~digit_12_name, scales = "free_y") +
  xlab("Fiscal Year") +
  ylab("Contract Value") +
  theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust = 1)) +  # Rotate x-axis text vertically
  labs(color = NULL, size = "Contract Value")  # Remove color legend title, specify size legend title

p <- ggplotly(p, tooltip = c("Fiscal Year: %{x}", "Contract Value: %{y}")) %>%
  style(hoverlabel = list(bgcolor = "white", font = list(color = "black"),
                          bordercolor = "black", font = list(family = "Arial", size = 12),
                          namelength = -1))

print(p)


#####show contract value on hover
library(plotly)

p <- data  %>% 
  ggplot(aes(x = as.factor(fiscal_year),
             y = contract_value,
             color = digit_12_name,
             text = paste("Fiscal Year:", as.factor(fiscal_year), "<br>",
                          "Contract Value:", round(contract_value, 2), "<br>",
                          "Client:", owner_org))) +
  geom_boxplot(color = "black")   +
  geom_point(aes(size = contract_value), position = position_jitter(width = 0.2), alpha = 0.5) +
   xlab("Fiscal Year") +
  ylab("Contract Value") +
  theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust = 1)) +  # Rotate x-axis text vertically
  labs(color = NULL, size = "Contract Value") ; ggplotly(p) 

# p <- ggplotly(p, tooltip = c("Fiscal Year: %{x}", "Contract Value: %{y}", "Client: %{text}")) %>%
#   style(hoverlabel = list(bgcolor = "white", font = list(color = "black"),
#                           bordercolor = "black", font = list(family = "Arial", size = 12),
#                           namelength = -1))

print(p)
