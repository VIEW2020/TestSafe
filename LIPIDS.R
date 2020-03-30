
# TestSafe HDL, LDL, TRI and TCL Extraction v3

library(data.table)
library(fst)

# Descriptions
hdl.desc <- toupper(c("HDL CHOL", "HDL CHOL.", "HDL CHOLESTEROL", "HDLC", "CHOLESTEROL (HDL)"))
ldl.desc <- toupper(c("CHOLESTEROL (LDL) (CALCULATED)", "CHOLESTEROL (LDL)(CALCULATED)", "LDL C MEASURED", "LDL CHOL (C)", "LDL CHOL (CALCULATED)", "LDL CHOL - CALCULATED", "LDL CHOL.(C)", "LDL CHOLESTEROL", "LDL CHOLESTEROL (C)"))
tri.desc <- toupper(c("TRIGLYCERIDE", "TRIGLYCERIDES"))
tcl.desc <- toupper(c("TOTAL CHOLESTEROL", "TOTAL CHOLESTEROL SERUM", "CHOLESTEROL SERUM", "CHOLESTEROL"))

all.desc <- c(hdl.desc, ldl.desc, tri.desc, tcl.desc, cho.desc)

# Params
files.list <- list.files("V:/source_data/R/TESTSAFE/SIMPLE/")

for(file in files.list){
  
  # Import Total Population
  DATA <- read.fst(paste0("source_data/R/TESTSAFE/SIMPLE/", file), as.data.table = T)
  
  # Filter by Description
  DATA <- DATA[DESC_UPPER %in% all.desc]
  
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

write.fst(ALL_DATA, "source_data/R/TESTSAFE/Working/ALL_CHOL_2004_2018.fst", 75)

NHI <- read.fst("V:/source_data/R/NHI/VSIMPLE_NHI_LOOKUP_AUG2019.fts", as.data.table = T)

# Limits
hdl.lim <- 51.8
ldl.lim <- 77.7
tri.lim <- 56.5
tcl.lim <- 103.6

for(test.type in c("hdl", "ldl", "tri", "tcl")){
  
  test.desc <- get(paste0(test.type, ".desc"))
  res.limit <- get(paste0(test.type, ".lim"))
  
  DATA <- ALL_DATA[DESC_UPPER %in% test.desc]
  
  # Enrich Data
  DATA[, EN_OBSR_UNITS := copy(tolower(OBSR_UNITS))]
  DATA[, EN_OBSR_RESULT_NUM := copy(as.numeric(OBSR_RESULT_NUM))]
  
  DATA <- DATA[EN_OBSR_RESULT_NUM >= 1 & EN_OBSR_RESULT_NUM <= res.limit] 
  DATA <- DATA[OBRQ_REPORT_STATUS=="F" | OBRQ_REPORT_STATUS=="FC"] 
  
  # Date
  DATA[, EN_RESULT_DATETIME := as.character(paste(RESULT_DATE, RESULT_DATE_TIMESTAMP, sep=" "), 
                                                format="%Y-%m-%d %H:%M:%S")]
  
  # Remove Duplicates: Same person, sameday, sametime, same result
  DATA <- DATA[, by=list(VSIMPLE_INDEX_MASTER, EN_RESULT_DATETIME, EN_OBSR_RESULT_NUM)
               , index := seq_len(.N)][index==1, -"index", with=F] # 
  
  # Reduce down to one record per day
  # If there are multiple records on the same day, then keep on the last one of the day
  DATA <- DATA[order(EN_RESULT_DATETIME, decreasing = T)
               , by=list(VSIMPLE_INDEX_MASTER, RESULT_DATE)
               , index:=seq_len(.N)][index==1, -"index", with=F] # 
  
  # NHI DOB Match
  DATA <- DATA[, NHI_DOB := NHI$EN_DOB[match(VSIMPLE_INDEX_MASTER, NHI$VSIMPLE_INDEX_2NDARY)]]
  DATA[, PAT_DOB := as.Date(PAT_DOB)]
  
  DateIdentityCheck <- function(x, y){
    
    library(lubridate, quietly = T)
    
    mapply(function(x, y){
      
      date.1 <- c(year(x), month(x), day(x))
      date.2 <- c(year(y), month(y), day(y))
      
      +(sum(date.1 %in% date.2)>=2)
      
    }, x=x, y=y)
  }
  
  DATA[PAT_DOB != NHI_DOB, 
       passed := DateIdentityCheck(x = PAT_DOB, y = NHI_DOB)]
  
  DATA <- DATA[passed == 1 | is.na(passed)] # -5163
  
  DATA <- DATA[,.(VSIMPLE_INDEX_MASTER, VIS_PAT_CLASS, OBSC_DESC, FACILITY_CODE, OBRQ_FILLER_FCTY, OBRQ_ABNORMAL, OBRQ_REPORT_TYPE, OBRQ_REPORT_STATUS,
                  OBSR_OBSID_SCH, EN_OBSR_RESULT_NUM, EN_OBSR_UNITS, OBSR_RANGE, RESULT_DATE, EN_RESULT_DATETIME)]
  
  # Save
  for(year in 2004:2018){
    
    write.fst(DATA[year(RESULT_DATE) == year],
              paste0("source_data/R/TESTSAFE/LIPIDS/VSIMP_", toupper(test.type), "_", year, ".fst"), 75)
    
  }
  
  write.fst(DATA, paste0("source_data/R/TESTSAFE/LIPIDS/VSIMP_", toupper(test.type), "_2004_2018.fst"), 75)
  
}



