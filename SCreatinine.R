
# TestSafe Serum Creatinine Extraction v4

# Last updated, September 2019

# Uses the 2019 TestSafe extract from 2004-2018
# Uses VSIMPLE_INDEX_MASTER

library(data.table)
library(fst)

test.desc <- toupper(c("CREATININE", "CREATININE (ISTAT)", "CREATININE - PLASMA", "SERUM CREATININE", "SERUM CREATININE FOR CLEARANCE",
                         "CREATININE SERUM"))

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

write.fst(ALL_DATA, "source_data/R/TESTSAFE/Working/ALL_SERUM_CREATININE_2004_2018.fst", 75)


# Enrich Data
ALL_DATA[, EN_OBSR_UNITS := copy(tolower(OBSR_UNITS))]
ALL_DATA[, EN_OBSR_RESULT_NUM := copy(as.numeric(OBSR_RESULT_NUM))]

# Some results (very few) have no units; remove records
ALL_DATA <- ALL_DATA[EN_OBSR_UNITS == "mmol/l" | EN_OBSR_UNITS == "umol/l"]  # -2105

# Unit conversion: mmol/l to umol/l
# nb. Only apply conversion if result is <5
ALL_DATA[EN_OBSR_UNITS == "mmol/l", 
         c("EN_OBSR_RESULT_NUM", "EN_OBSR_UNITS") := list(
           ifelse(EN_OBSR_RESULT_NUM <= 5, 1000 * EN_OBSR_RESULT_NUM, EN_OBSR_RESULT_NUM),
           "umol/l")]

# Apply range Check
# Remove out of range results <= 20umol/l or >=5000 umol/l
ALL_DATA <- ALL_DATA[EN_OBSR_RESULT_NUM > 20 & EN_OBSR_RESULT_NUM < 5000] # -30964

# Enesure OBRQ_RESULT_TYPE is NA [ie. all results usuable]
ALL_DATA <- ALL_DATA[is.na(OBRQ_RESULT_TYPE)] # -339

ALL_DATA <- ALL_DATA[OBRQ_REPORT_STATUS %in% c("F", "FC", "FCD", "FD", "FDC")] # -37098 records

# Date
ALL_DATA[, EN_RESULT_DATETIME := as.character(paste(RESULT_DATE, RESULT_DATE_TIMESTAMP, sep=" "), 
                                              format="%Y-%m-%d %H:%M:%S")]

# Remove Duplicates: Same person, sameday, sametime, same result
ALL_DATA <- ALL_DATA[, by=list(VSIMPLE_INDEX_MASTER, EN_RESULT_DATETIME, EN_OBSR_RESULT_NUM)
                     , index := seq_len(.N)][index==1, -"index", with=F] # -43234

# Reduce down to one record per day
# If there are multiple records on the same day, then keep on the last one of the day
ALL_DATA <- ALL_DATA[order(EN_RESULT_DATETIME, decreasing = T)
                     , by=list(VSIMPLE_INDEX_MASTER, RESULT_DATE)
                     , index:=seq_len(.N)][index==1, -"index", with=F] # -672620

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

ALL_DATA <- ALL_DATA[passed == 1 | is.na(passed)] # -9361

# Capture eGFR-ckdepi
egfr.demo <- c("NHI_GENDER", "TEST_AGE", "NULL_ETH")

ALL_DATA[, (egfr.demo) := list(
  +(NHI$GENDER[match(VSIMPLE_INDEX_MASTER, NHI$VSIMPLE_INDEX_2NDARY)]=="M"),
  as.numeric(floor((RESULT_DATE - NHI_DOB) / 365.25)),
  0
)]

ALL_DATA <- ALL_DATA[TEST_AGE >= 0 & TEST_AGE <= 110] #-43

ALL_DATA <- ALL_DATA[, EN_OBSR_RESULT_EGFR := round(
  nephro::CKDEpi.creat(EN_OBSR_RESULT_NUM * 0.0113, NHI_GENDER, TEST_AGE, NULL_ETH),
  2)]

ALL_DATA <- ALL_DATA[,.(VSIMPLE_INDEX_MASTER, VIS_PAT_CLASS, OBSC_DESC, FACILITY_CODE, OBRQ_FILLER_FCTY, OBRQ_ABNORMAL, OBRQ_REPORT_TYPE, OBRQ_REPORT_STATUS,
                        EN_OBSR_RESULT_NUM, EN_OBSR_UNITS, OBSR_RANGE, RESULT_DATE, EN_OBSR_RESULT_EGFR)]


# Save
for(year in 2004:2018){
  
  write.fst(ALL_DATA[year(RESULT_DATE) == year],
            paste0("source_data/R/TESTSAFE/CREATININE/VSIMP_SCR_", year, ".fst"), 75)
  
}

write.fst(ALL_DATA, "source_data/R/TESTSAFE/CREATININE/VSIMP_SCR_2004_2018.fst", 75)










