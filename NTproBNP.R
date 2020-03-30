
# TestSafe NTproBNP Extraction v1

# Last updated, October 2019

# Uses the 2019 TestSafe extract from 2004-2018
# Uses VSIMPLE_INDEX_MASTER

library(data.table)
library(fst)

test.desc <- toupper(c("B-TYPE NATRIURETIC PEPTIDE", "BNP", "BRAIN NATRIURETIC PEPTIDE", "BRAIN NATURETIC PEPTIDE", 
                       "NT PROBNP", "NT-PROBNP"))

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

write.fst(ALL_DATA, "source_data/R/TESTSAFE/Working/ALL_NTproBNP_2004_2018.fst", 75)


#  Clean
ALL_DATA <- read.fst("source_data/R/TESTSAFE/Working/ALL_NTproBNP_2004_2018.fst",
                     as.data.table = T)

ALL_DATA[, EN_OBSR_UNITS := copy(tolower(OBSR_UNITS))]
ALL_DATA[, EN_OBSR_RESULT_NUM := copy(as.numeric(OBSR_RESULT_NUM))]

# There shouldn't be any records without a unit. Definitely remove if unit is missing.
ALL_DATA <- ALL_DATA[EN_OBSR_UNITS == "pmol/l" | EN_OBSR_UNITS == "pmoll"]

# Apply range Check
# Use TestSafe original limits: 1 - 4139. If outside of range, numeric results are NA :. Remove any NA numeric results
ALL_DATA <- ALL_DATA[!is.na(EN_OBSR_RESULT_NUM)] # -103

# Enesure OBRQ_RESULT_TYPE is NA [ie. all results usuable]
ALL_DATA <- ALL_DATA[is.na(OBRQ_RESULT_TYPE)] # -35

ALL_DATA <- ALL_DATA[OBRQ_REPORT_STATUS %in% c("F", "FC", "FCD", "FD", "FDC")] # -165 records

# Date
ALL_DATA[, EN_RESULT_DATETIME := as.character(paste(RESULT_DATE, RESULT_DATE_TIMESTAMP, sep=" "), 
                                              format="%Y-%m-%d %H:%M:%S")]

# Remove Pure Duplicates: Same person, sameday, sametime, same result, same results facility
ALL_DATA <- ALL_DATA[, by=list(VSIMPLE_INDEX_MASTER, EN_RESULT_DATETIME, EN_OBSR_RESULT_NUM, OBSR_OBSID_SCH)
                     , index := seq_len(.N)][index==1, -"index", with=F] # -174

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

ALL_DATA <- ALL_DATA[passed == 1 | is.na(passed)] # -109

ALL_DATA <- ALL_DATA[,.(VSIMPLE_INDEX_MASTER, VIS_PAT_CLASS, OBSC_DESC, FACILITY_CODE, OBRQ_FILLER_FCTY, OBRQ_ABNORMAL, OBRQ_REPORT_TYPE, OBRQ_REPORT_STATUS,
                        OBSR_OBSID_SCH, EN_OBSR_RESULT_NUM, EN_OBSR_UNITS, OBSR_RANGE, RESULT_DATE, EN_RESULT_DATETIME)]

# Save
for(year in 2004:2018){

  write.fst(ALL_DATA[year(RESULT_DATE) == year],
            paste0("source_data/R/TESTSAFE/BNP/VSIMP_BNP_", year, ".fst"), 75)

}

write.fst(ALL_DATA, "source_data/R/TESTSAFE/BNP/VSIMP_BNP_2004_2018.fst", 75)




