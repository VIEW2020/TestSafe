
# TestSafe Total/HDL Ratio Extraction v3

# Last updated, September 2019

# Uses the 2019 TestSafe extract from 2004-2018
# Uses VSIMPLE_INDEX_MASTER

# For each lipid type
# NB:   Apply range check [reference http://www.scymed.com/en/smnxpb/pbhhr154.htm]
#       All values need to be greater than 1
#       All units are mmol/l but TCHDL Ratio has varying unit types - for this, treat all units as a ratio

library(data.table)
library(fst)

test.desc <- toupper(c("CHOL/HDL RATIO", "CHOL/HDL RATIO (CALC)", "CHOLESTEROL (TOTAL/HDL)", 
                       "CHOLESTEROL / HDL RATIO", "	CHOLESTEROL HDL RATIO", "CHOLESTEROL/HDL RATIO", 
                       "CHOLESTEROL:HDL RATIO", "TOTAL/HDL CHOL RATIO", "TOTAL / HDL RATIO", "TOTAL/HDL RATIO"))

# Params
files.list <- list.files("V:/source_data/R/TESTSAFE/SIMPLE/")

for(file in files.list){
  
  # Import Total Population
  DATA <- read.fst(paste0("source_data/R/TESTSAFE/SIMPLE/", file), as.data.table = T)
  
  # Filter by Description
  DATA <- DATA[DESC_UPPER %in% test.desc]
  
  # Remove missing numeric results
  DATA <- DATA[!is.na(OBSR_RESULT_NUM)]
  
  # Combine
  ALL_DATA <- if(file == files.list[1]){
    DATA
  } else {
    rbind(ALL_DATA, DATA)
  }
  
  print(paste0(file, " completed"))
  
}

write.fst(ALL_DATA, "source_data/R/TESTSAFE/Working/ALL_TCHDL_2004_2018.fst", 75)


# Enrich Data
ALL_DATA[, EN_OBSR_UNITS := copy(tolower(OBSR_UNITS))]
ALL_DATA[, EN_OBSR_RESULT_NUM := copy(as.numeric(OBSR_RESULT_NUM))]

ALL_DATA <- ALL_DATA[EN_OBSR_RESULT_NUM >= 1 & EN_OBSR_RESULT_NUM <= 30] # -614 records

ALL_DATA <- ALL_DATA[OBRQ_REPORT_STATUS=="F" | OBRQ_REPORT_STATUS=="FC"] # -21216 records


# Date
ALL_DATA[, EN_RESULT_DATETIME := as.character(paste(RESULT_DATE, RESULT_DATE_TIMESTAMP, sep=" "), 
                                              format="%Y-%m-%d %H:%M:%S")]

# Remove Duplicates: Same person, sameday, sametime, same result
ALL_DATA <- ALL_DATA[, by=list(VSIMPLE_INDEX_MASTER, EN_RESULT_DATETIME, EN_OBSR_RESULT_NUM)
                     , index := seq_len(.N)][index==1, -"index", with=F] # -8769

# Reduce down to one record per day
# If there are multiple records on the same day, then keep on the last one of the day
ALL_DATA <- ALL_DATA[order(EN_RESULT_DATETIME, decreasing = T)
                     , by=list(VSIMPLE_INDEX_MASTER, RESULT_DATE)
                     , index:=seq_len(.N)][index==1, -"index", with=F] # -19587

# NHI DOB Match
NHI <- read.fst("V:/source_data/R/NHI/VSIMPLE_NHI_LOOKUP_AUG2019.fts", as.data.table = T)

ALL_DATA <- ALL_DATA[, NHI_DOB := NHI$EN_DOB[match(VSIMPLE_INDEX_MASTER, NHI$VSIMPLE_INDEX_2NDARY)]]
ALL_DATA[, PAT_DOB := as.Date(PAT_DOB)]

DateIdentityCheck <- function(x, y){
  
  library(lubridate, quietly = T)
  
  mapply(function(x, y){
    
    date.1 <- c(year(x), month(x), day(x))
    date.2 <- c(year(y), month(y), day(y))
    
    +(sum(date.1 %in% date.2)>=2)
    
  }, x=x, y=y)
}

ALL_DATA[PAT_DOB != NHI_DOB, 
         passed := DateIdentityCheck(x = PAT_DOB, y = NHI_DOB)]

ALL_DATA <- ALL_DATA[passed == 1 | is.na(passed)] # -5163

ALL_DATA <- ALL_DATA[,.(VSIMPLE_INDEX_MASTER, FACILITY_CODE, OBRQ_FILLER_FCTY, OBRQ_ABNORMAL, OBRQ_REPORT_TYPE, 
                        EN_OBSR_RESULT_NUM, EN_OBSR_UNITS, RESULT_DATE)]

# Save
for(year in 2004:2018){
  
  write.fst(ALL_DATA[year(RESULT_DATE) == year],
            paste0("source_data/R/TESTSAFE/LIPIDS/VSIMP_TCHDL_", year, ".fst"), 75)
  
}

write.fst(ALL_DATA, "source_data/R/TESTSAFE/LIPIDS/VSIMP_TCHDL_2004_2018.fst", 75)



