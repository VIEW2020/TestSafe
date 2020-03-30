
# TestSafe HBA1C Extraction v4

# Last updated, September 2019

# Uses the 2019 TestSafe extract from 2004-2018
# Uses VSIMPLE_INDEX_MASTER

library(data.table)
library(fst)

test.desc <- toupper(c("GLYCOSYLATED HBA1C", "HBA1C", "HBA1C (IFCC)", "HBA1C.", "POCT HBA1C", "POCT HBA1C -IFCC", 
                       "HAEMOGLOBIN A1C", "HAEMOGLOBIN A1C (%)", "HAEMOGLOBIN A1C (DCCT)", "HAEMOGLOBIN A1C (IFCC)",
                       "HAEMOGLOBIN A1C (MMOL/MOL)", "GLYCOSYLATED HAEMOGLOBIN", "GLYCOSYLATED HAEMOGLOBIN %"))

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
  ALL_HBA1C <- if(file == files.list[1]){
    DATA
  } else {
    rbind(ALL_HBA1C, DATA)
  }
  
  print(paste0(file, " completed"))
  
}

write.fst(ALL_HBA1C, "source_data/R/TESTSAFE/Working/ALL_HBA1C_2004_2018.fst", 75)
  
# Enrich Data
ALL_HBA1C[, EN_OBSR_UNITS := copy(tolower(OBSR_UNITS))]
ALL_HBA1C[, EN_OBSR_RESULT_NUM := copy(as.numeric(OBSR_RESULT_NUM))]

# Units: % [DCCT] need to be convered to IFCC in mmol/mol
# - Where units are missing, use range to determine corect unit
# - Where both units and range are missing, use result to determine correct unit
ALL_HBA1C[is.na(EN_OBSR_UNITS) & OBSR_RANGE=="3.5-6.0",
          EN_OBSR_UNITS := "%"]

ALL_HBA1C[is.na(EN_OBSR_UNITS) & (OBSR_RANGE=="20-42" | OBSR_RANGE=="20-42 mmol/mol"),
          EN_OBSR_UNITS := "mmol/mol"]

ALL_HBA1C[is.na(EN_OBSR_UNITS) & is.na(OBSR_RANGE) & EN_OBSR_RESULT_NUM<20, 
          EN_OBSR_UNITS := "%"]
  
# Results
ALL_HBA1C[EN_OBSR_UNITS=="%", 
          c("EN_OBSR_RESULT_NUM", "EN_OBSR_UNITS") := list(
            round(10.93 * EN_OBSR_RESULT_NUM - 23.5),
            "mmol/mol")]

ALL_HBA1C <- ALL_HBA1C[EN_OBSR_RESULT_NUM>20 & EN_OBSR_RESULT_NUM<5000] # -1017 records
  
ALL_HBA1C <- ALL_HBA1C[OBRQ_REPORT_STATUS=="F" | OBRQ_REPORT_STATUS=="FC"] # -3995 records
  
# Date
ALL_HBA1C[, EN_RESULT_DATETIME := as.character(paste(RESULT_DATE, RESULT_DATE_TIMESTAMP, sep=" "), 
                                               format="%Y-%m-%d %H:%M:%S")]
  
# Remove Duplicates: Same person, sameday, sametime, same result
ALL_HBA1C <- ALL_HBA1C[, by=list(VSIMPLE_INDEX_MASTER, EN_RESULT_DATETIME, EN_OBSR_RESULT_NUM)
                       , index := seq_len(.N)][index==1, -"index", with=F] # -665615

# Reduce down to one record per day
# If there are multiple records on the same day, then keep on the last one of the day
ALL_HBA1C <- ALL_HBA1C[order(EN_RESULT_DATETIME, decreasing = T)
                       , by=list(VSIMPLE_INDEX_MASTER, RESULT_DATE)
                       , index:=seq_len(.N)][index==1, -"index", with=F] # -42491

# NHI DOB Match
NHI <- read.fst("V:/source_data/R/NHI/VSIMPLE_NHI_LOOKUP_AUG2019.fts", as.data.table = T)

ALL_HBA1C <- ALL_HBA1C[, NHI_DOB := NHI$EN_DOB[match(VSIMPLE_INDEX_MASTER, NHI$VSIMPLE_INDEX_2NDARY)]]
ALL_HBA1C[, PAT_DOB := as.Date(PAT_DOB)]

DateIdentityCheck <- function(x, y){
  
  library(lubridate, quietly = T)
  
  mapply(function(x, y){
    
    date.1 <- c(year(x), month(x), day(x))
    date.2 <- c(year(y), month(y), day(y))
    
    +(sum(date.1 %in% date.2)>=2)
    
  }, x=x, y=y)
}

ALL_HBA1C[PAT_DOB != NHI_DOB, 
          passed := DateIdentityCheck(x = PAT_DOB, y = NHI_DOB)]

ALL_HBA1C <- ALL_HBA1C[passed == 1 | is.na(passed)] # -3817

ALL_HBA1C <- ALL_HBA1C[,.(VSIMPLE_INDEX_MASTER, FACILITY_CODE, OBRQ_FILLER_FCTY, OBRQ_ABNORMAL, OBRQ_REPORT_TYPE, 
                          EN_OBSR_RESULT_NUM, EN_OBSR_UNITS, RESULT_DATE)]

# Save
for(year in 2004:2018){
  
  write.fst(ALL_HBA1C[year(RESULT_DATE) == year],
            paste0("source_data/R/TESTSAFE/HBA1C/VSIMP_HBA1C_", year, ".fst"), 75)
  
}

write.fst(ALL_HBA1C, "source_data/R/TESTSAFE/HBA1C/VSIMP_HBA1C_2004_2018.fst", 75)



