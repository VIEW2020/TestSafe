
# TestSafe Albumin/Creatinine Ratio Extraction v1

# Last updated, January 2020

# Uses the 2019 TestSafe extract from 2004-2018
# Uses VSIMPLE_INDEX_MASTER

library(data.table)
library(fst)
# DESC <- read.fst("common_lookups/CURRENT_TSDESC_LOOKUP.fst")

test.desc <- c("ALBUMIN / CREATININE RATIO", "ALBUMIN CREATININE RATIO", "ALBUMIN/CREATININE RATIO", "ALBUMIN:CREATININE RATIO URINE",
               "MICROALBUMIN/CREAT URINE", "ALBUMIN CREATININE RATIO URINE", "MICROALBUMIN / CREATININE URINE", "MICROALBUMIN / CREATININE RATIO",
               "ALBUMIN / CREATININE RATIO", "ALBUMIN CREATININE RATIO", "ALBUMIN/CREATININE RATIO", "ALBUMIN/CREAT RATIO", "ALBUMIN/CREAT RATIO URINE", 
               "ALBUMIN:CREATININE RATIO URINE", "MICROALBUMIN/CREAT URINE", "ALB/CREAT RATIO")

test.desc <- unique(test.desc)

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

write.fst(ALL_DATA, "source_data/R/TESTSAFE/Working/ALL_ACR_2004_2018.fst", 75)


#  Clean
ALL_DATA <- read.fst("source_data/R/TESTSAFE/Working/ALL_ACR_2004_2018.fst",
                     as.data.table = T)

ALL_DATA[, EN_OBSR_UNITS := copy(tolower(OBSR_UNITS))]
ALL_DATA[, EN_OBSR_RESULT_NUM := copy(as.numeric(OBSR_RESULT_NUM))]

# The unit should be mg/mmol. >83% of records have no units. If missing, assume to be mg/mmol.
# There are 7 records using mg/mol, 17, ratio and 517 records using ug/mmol. 
# Upon closer inspection of distribution and mean values, inconsistencies were found. 
# As such, the decision was made to remove these records.
ALL_DATA <- ALL_DATA[EN_OBSR_UNITS == "mg/mmol" | is.na(EN_OBSR_UNITS)]

# Apply range Check
# Reference: http://www.scymed.com/en/smnxps/psdjb222.htm  
# ACR: 50000 mg/g * 0.113 = 5650 mg/mmol
ALL_DATA <- ALL_DATA[OBSR_RESULT_NUM>0.1 & OBSR_RESULT_NUM<5650]

# Enesure OBRQ_RESULT_TYPE is NA [ie. all results usuable]
ALL_DATA <- ALL_DATA[is.na(OBRQ_RESULT_TYPE)]

ALL_DATA <- ALL_DATA[OBRQ_REPORT_STATUS %in% c("F", "FD")]

# Date
ALL_DATA[, EN_RESULT_DATETIME := as.character(paste(RESULT_DATE, RESULT_DATE_TIMESTAMP, sep=" "), 
                                              format="%Y-%m-%d %H:%M:%S")]

# Remove Pure Duplicates: Same person, sameday, sametime, same result, same results facility
ALL_DATA <- ALL_DATA[, by=list(VSIMPLE_INDEX_MASTER, EN_RESULT_DATETIME, EN_OBSR_RESULT_NUM, OBSR_OBSID_SCH)
                     , index := seq_len(.N)][index==1, -"index", with=F]

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

ALL_DATA <- ALL_DATA[passed == 1 | is.na(passed)]

ALL_DATA <- ALL_DATA[,.(VSIMPLE_INDEX_MASTER, VIS_PAT_CLASS, OBSC_DESC, FACILITY_CODE, OBRQ_FILLER_FCTY, OBRQ_ABNORMAL, OBRQ_REPORT_TYPE, OBRQ_REPORT_STATUS,
                        OBSR_OBSID_SCH, EN_OBSR_RESULT_NUM, EN_OBSR_UNITS, OBSR_RANGE, RESULT_DATE, EN_RESULT_DATETIME)]

# Save
for(year in 2004:2018){
  
  write.fst(ALL_DATA[year(RESULT_DATE) == year],
            paste0("source_data/R/TESTSAFE/ACR/VSIMP_ACR_", year, ".fst"), 75)
  
}

write.fst(ALL_DATA, "source_data/R/TESTSAFE/ACR/VSIMP_ACR_2004_2018.fst", 75)




