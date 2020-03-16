library(dplyr)
library(readr)
library(tidyr)
library(rvest)
library(reshape2)
library(tidyverse)


# Consider replacing pipeline with: https://cowid.netlify.com/data/new_deaths.csv

# Datasets at https://github.com/CSSEGISandData/COVID-19
if(!dir.exists('jhu')){
  dir.create('jhu')
}

# Confirmed cases
download.file(url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Confirmed.csv',
              destfile = 'jhu/confirmed_cases.csv')

# Deaths
download.file(url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Deaths.csv',
              destfile = 'jhu/deaths.csv')

# Recovered
download.file(url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Recovered.csv',
              destfile = 'jhu/recovered.csv')




# Read in
confirmed_cases <- read_csv('jhu/confirmed_cases.csv')
deaths <- read_csv('jhu/deaths.csv')
recovered <- read_csv('jhu/recovered.csv')

# Define function for cleaning up
clean_up <- function(ts, value_name = 'deaths'){

    # Make wide
  ts <- ts %>%
    gather(key = date,
           value = value_name,
           names(ts)[!is.na(as.Date(names(ts), format = '%m/%d/%y'))])

  # Clean up columns and names
  names(ts) <- c('district', 'country', 'lat', 'lng', 'date', value_name)
  ts$date <- as.Date(ts$date, format = '%m/%d/%y')
  return(ts)

}

# Clean up the datasets
deaths <- clean_up(deaths, value_name = 'deaths')
confirmed_cases <- clean_up(confirmed_cases, value_name = 'confirmed_cases')
recovered <- clean_up(recovered, value_name = 'recovered')

# Join all together
df <- full_join(x = confirmed_cases,
                y = deaths) %>%
  full_join(recovered)

# # Keep only States for the US
# # (otherwise, double-counts certain things)
# library(maps)
# states <- map('state')$names
# states <- unlist(lapply(states, function(x){strsplit(x, ':', fixed = TRUE)[1]}))
# df <- df %>%
#   filter(country != 'US' |
#            tolower(district) %in% states)
# Beginning on March 10, the data format changes for the US - reporting states and
# sub-state entities
# we want to keep all US entries through March 9 and then beginning on the 10th,
# only keep those with commas (the states)
df <- df %>%
  filter(country != 'US' |
           date < '2020-03-10' |
           !grepl(', ', district))

# Manual update for Spain (JHU data behind)
df$confirmed_cases[df$country == 'Spain' & df$date == '2020-03-12'] <- 3050
df$deaths[df$country == 'Spain' & df$date == '2020-03-12'] <- 84
# df$recovered[df$country == 'Spain' & df$date == '2020-03-12']

# # Add a Spain row for March 14 (updating manually)
# if(length(df$confirmed_cases[df$country == 'Spain' & df$date == '2020-03-14']) == 0){
#   new_row <- tibble(district = NA,
#                     date = as.Date('2020-03-14'),
#                     country = 'Spain',
#                     lat = df$lat[df$country == 'Spain'][1],
#                     lng = df$lng[df$country == 'Spain'][1],
#                     confirmed_cases = 5753,
#                     deaths = 136,
#                     recovered = 517)
#   df <- df %>% bind_rows(new_row)
# }

# Decumulate too
df <- df %>%
  ungroup %>%
  arrange(country, district, date) %>%
  group_by(country, district, lat, lng) %>%
  mutate(confirmed_cases_non_cum = confirmed_cases - lag(confirmed_cases, default = 0),
         deaths_non_cum = deaths - lag(deaths, default = 0),
         recovered_non_cum = recovered - lag(recovered, default = 0)) %>%
  ungroup


# Join all together but by country
df_country <- df %>%
  group_by(country, date) %>%
  summarise(lat = mean(lat),
            lng = mean(lng),
            confirmed_cases = sum(confirmed_cases, na.rm = TRUE),
            deaths = sum(deaths, na.rm = TRUE),
            recovered = sum(recovered, na.rm = TRUE)) %>%
  ungroup %>%
  # # # Weird March 11 correction
  # mutate(confirmed_cases = ifelse(country == 'US' & date == '2020-03-10', 1050, confirmed_cases)) %>%
  # mutate(deaths = ifelse(country == 'US' & date == '2020-03-10', 29, deaths)) %>%
  # mutate(recovered = ifelse(country == 'US' & date == '2020-03-10', 7, recovered)) %>%
  group_by(country, lat, lng) %>%
  mutate(confirmed_cases_non_cum = confirmed_cases - lag(confirmed_cases, default = 0),
         deaths_non_cum = deaths - lag(deaths, default = 0),
         recovered_non_cum = recovered - lag(recovered, default = 0)) %>%
  ungroup

########
## CANADA
########


cad_url <- "https://en.wikipedia.org/wiki/2020_coronavirus_pandemic_in_Canada"
html_tables <- cad_url %>%
  html %>%
  html_nodes("table")

dat <- html_table(html_tables, header = TRUE, fill = TRUE)[[4]]

# remove first row
dat <- dat[-1,]
names(dat) <- as.character(dat[1,])
names(dat) <- c('date', 'bc', 'Ab','SK', 'MB', 'ON', 'QC', 'NB', 'PE', 'NS', 'NL')
dat <- dat[-1,]
dat <- dat[, 1:11]
start_index <- which(dat$date == 'Total confirmed')
dat <- dat[1:(start_index-1),]
dat$date <- as.Date(dat$date, format = '%B%d')

all_dates = seq(min(dat$date), max(dat$date), 1)


dates0 = all_dates[!(all_dates %in% dat$date)]
data0 = data.frame(date = dates0, bc = 0, Ab = 0,
                   SK=0, MB=0, ON=0, QC=0, NB=0, PE=0, NS=0, NL=0)

dat <- rbind(dat,data0)
dat <- melt(dat, id.vars = 'date')
dat$value[dat$value ==''] <- 0
dat$date <- gsub(' ', '', dat$date)



# loop through province and get cumsum 
prov_names <- unique(dat$variable)
result_list <- list()
for(i in 1:length(prov_names)){
  this_name <- prov_names[i]
  sub_dat <- dat %>% filter(variable == this_name) %>% arrange(date)
  sub_dat$cumulative_cases <- cumsum(sub_dat$value)
  result_list[[i]] <- sub_dat
}

canada <- do.call(rbind, result_list)
names(canada) <- c('date', 'location', 'daily_cases', 'cumulative_cases')
canada$location <- toupper(as.character(canada$location))

rm(dat, data0, html_tables, result_list, sub_dat, all_dates, cad_url, dates0,
   prov_names, i, start_index, this_name)

