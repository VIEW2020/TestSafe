
# Latest Thyroid Simulating Hormone for Total Population

# Last updated September 2018

library(dplyr)
library(data.table)
library(foreign)
library(fst)

# ENHI
ENHI <- read.fst("source_data/R/ENHI BOTH TABLES/ALL_ENIGMA_ALL_INHOUSE_PRIMARY_SECONDARY_JUL2018.fst")    
ENHI <- setDT(ENHI)[,.(VIEW_ENHI_MASTER, EXTERNAL_INDEX_MASTER, STUDENT_INDEX_MASTER, VIEW_TESTSAFE_2NDARY)]
ENHI <- setkey(setDT(ENHI), "VIEW_TESTSAFE_2NDARY")

# Descriptions
# Strings sourced from 2004-2017 master description index - provided by HealthAlliance
# Nb: CREATININE PLASMA excluded. They are very few in record.
desc.strings <- c("THYROID STIMULATING HORMONE", "TSH")


for (year in 2000:2016){
  
    # # i.    Import Total Population
    DATA <- readRDS(paste0("source_data/R/TESTSAFE/TESTSAFE_", year, ".rds"))
    
    # ii.   Filter by Description
    DATA[, OBSC_DESC := toupper(OBSC_DESC)]
    
    DATA <- DATA[OBSC_DESC %in% desc.strings]
    
    # iii.  Remove unuseful selected variables
    #       OBRQ_CLINICAL_INFO [Could have up to 135 characters]
    #       OBRQ_FILLER_APP, Any "_SERVICE" variables, Any "_DOC" variables, 
    #       Any "_Scr" variables, Any "_ADDR" variables [Contain free text and does not contain useful information
    DATA[, c("OBRQ_ATT_DOC_CODE", "OBRQ_ATT_DOC_SCH", "OBRQ_REF_DOC_CODE", "OBRQ_REF_DOC_SCH", "OBRQ_CON_DOC_CODE", 
             "OBRQ_CON_DOC_SCH", "OBRQ_ORD_DOC_CODE", "OBRQ_ORD_DOC_SCH",  "OBRQ_ADM_DOC_CODE", "OBRQ_ADM_DOC_SCH",
             "OBRQ_CLINICAL_INFO", "OBRQ_FILLER_APP", "OBRQ_SERVICE", "OBRQ_SERV_ID", "OBRQ_SERV_ID_SCH",
             "OBRQ_SRC_ADDITIVE", "OBRQ_SRC_SITE", "OBRQ_SRC_SITE_MOD", "OBRQ_SRC_TEXT",
             "FAC_ADDR_CODE", "FAC_ADDR_STREET", "FAC_ADDR_OTHER", "FAC_ADDR_CITY", "FAC_ADDR_STATE",
             "OBRQ_COLLECTOR_ID", "OBRQ_COLLECTOR_LOCN", "OBRQ_COLLECTOR_METHOD",
             "OBRQ_ORGANISATION", "OBRQ_ORGANISATION_NAME") := NULL]
  
    # iv.   Tidy Results
    DATA <- DATA[, c("OBSR_UNITS", "OBSR_RESULT_NUM"):=list(tolower(OBSR_UNITS),
                                                            as.numeric(OBSR_RESULT_NUM))][!is.na(OBSR_RESULT_NUM)]

        # # Some results have no units [very few: <1000 records for each year of data]. 
        # # If unit is missing, then result values <5 are mmol/l, and >5 are umol/l
        # DATA[is.na(OBSR_UNITS), OBSR_UNITS := ifelse(OBSR_RESULT_NUM<5, 
        #                                              "mmol/l", 
        #                                              "umol/l")]
        # 
        # # Unit conversion: mmol/l to umol/l
        # # Only apply conversion if result is <5
        # DATA[OBSR_UNITS=="mmol/l", c("OBSR_RESULT_NUM", "OBSR_UNITS") := list(ifelse(OBSR_RESULT_NUM<=5,
        #                                                                              1000 * OBSR_RESULT_NUM,
        #                                                                              OBSR_RESULT_NUM),
        #                                                                       "umol/l")]
        # 
        # # Apply range Check
        # # Remove out of range results <20umol/l or >=5000 umol/l [very few: <1400 records for each year of data]
        # DATA <- DATA[OBSR_RESULT_NUM>20 & OBSR_RESULT_NUM<5000]
        
        # Remove records with non-final or final correction report status
        # Can be alot ~10% results removed each year
        DATA <- DATA[OBRQ_REPORT_STATUS=="F" | OBRQ_REPORT_STATUS=="FC"]
    
       # v.   Assemble Date & Timestamp
      setnames(DATA, "OBRQ_RECV_DATE_TIMESTAMP", "OBRQ_RECV_TIMESTAMP")
      
      DATA[, OBRQ_RECV_DATE_TIMESTAMP := as.character(paste(OBRQ_RECV_DATE, OBRQ_RECV_TIMESTAMP, sep=" "), format="%Y-%m-%d %H:%M:%S")]
      
      # vi.    ENHI Swap
      DATA <- setkey(DATA, "ENHI")
      DATA <- ENHI[DATA, nomatch=0][,!c("VIEW_TESTSAFE_2NDARY"), with=F]
      
      # vii.  Remove Duplicates: Same person, sameday, sametime, same result
      #       Removes ~100 record per year
      DATA <- DATA[, by=list(VIEW_ENHI_MASTER, OBRQ_RECV_DATE, OBRQ_RECV_DATE_TIMESTAMP, OBSR_RESULT_NUM)
                   , index := seq_len(.N)][index==1, !c("index"), with=F]
    
      # Reduce down to one record per day
      # If there are multiple records on the same day, then keep on the last one of the day
      DATA <- DATA[order(OBRQ_RECV_DATE_TIMESTAMP, decreasing = T)
                   , by=list(VIEW_ENHI_MASTER, OBRQ_RECV_DATE)
                   , index:=seq_len(.N)][index==1, !c("index"), with=F]
    
    # viii. Export
    saveRDS(DATA, paste0("source_data/R/TESTSAFE/TEMP/TSH_TOTALPOP_", year, ".rds"))
    
     rm(DATA); gc()
    
  print(paste0(year, " completed"))
  
}


for(year in 2000:2016){

  DATA <- readRDS(paste0("source_data/R/TESTSAFE/TEMP/TSH_TOTALPOP_", year, ".rds"))
  
    if(year==2000){
     ALL_TSH <- DATA
  } else {
     ALL_TSH <- rbind(ALL_TSH, DATA)
  }
  
   # To VIEW_Data 
   # For SAS (.dta)
   foreign::write.dta(DATA, paste0("source_data/STATA/TESTSAFE/CREATININE/SCR_TOTALPOP_", year, ".dta"))
   
   # For R 
   DATA <- DATA[, !c("EXTERNAL_INDEX_MASTER", "STUDENT_INDEX_MASTER")]
   saveRDS(DATA, paste0("source_data/R/TESTSAFE/TSH/TSH_TOTALPOP_", year, ".rds"))
  
  print(paste0(year, " completed!"))
  
}
