
# Latest Haemoglobin Test Extraction for Total Population

# Last updated March 2018

library(foreign)
library(data.table)
  
setwd("V:/")

ENHI <- readRDS("source_data/R/ENHI BOTH TABLES/VIEW_P__VIEW_TSF_S_AUG2017.rds")
  
  setkey(ENHI, "VIEW_TESTSAFE_2NDARY")
  
  
for(year in 2000:2016){
  
  # i.    Import Total Population
  DATA <- readRDS(paste0("source_data/R/TESTSAFE/TESTSAFE_", year, ".rds"))
    
    setkey(DATA, "ENHI")
    
  # ii.   Filter by Description
  DATA[, OBSC_DESC := toupper(OBSC_DESC)]
  
  DATA <- DATA[OBSC_DESC %in% "FERRITIN"]
  
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

        # Some results have no units [very few: between <10 records for each year of data]. 
        # For ferritin, keep only ug/l results. Ie. removes records with ng/ml. Very few <20 in 2015
        DATA <- DATA[!is.na(OBSR_UNITS) & OBSR_UNITS=="ug/l"]
        
        # Apply range Check
        # Remove results that are >=100000. No lower limit except 0. 
        DATA <- DATA[OBSR_RESULT_NUM>0 & OBSR_RESULT_NUM<100000]
        
        
  # v.   Assemble Date & Timestamp
  setnames(DATA, "OBRQ_RECV_DATE_TIMESTAMP", "OBRQ_RECV_TIMESTAMP")
  
  DATA[, OBRQ_RECV_DATE_TIMESTAMP := as.POSIXct(paste(OBRQ_RECV_DATE, 
                                                        OBRQ_RECV_TIMESTAMP, 
                                                        sep=" "), format="%Y-%m-%d %H:%M:%S")]

  # vi.   ENHI Swapover
  DATA <- ENHI[DATA, nomatch=0][,!c("VIEW_ENHI_2NDARY", "VIEW_TESTSAFE_2NDARY"), with=F]
  
  
  # vii.  Remove Duplicates: Same person, sameday, same result
  DATA <- DATA[, by=list(VIEW_ENHI_MASTER, OBRQ_RECV_DATE, OBSR_RESULT_NUM)
               , index := seq_len(.N)][index==1, !c("index"), with=F]
  
  
  # Reduce down to one record per day
  # If there are multiple records on the same day, then keep on the last one of the day
  DATA <- DATA[order(OBRQ_RECV_DATE_TIMESTAMP, decreasing = T)
               , by=list(VIEW_ENHI_MASTER, OBRQ_RECV_DATE)
               , index:=seq_len(.N)][index==1, !c("index"), with=F]
  
  
  # viii. Export
  saveRDS(DATA, paste0("source_data/R/TESTSAFE/FERRITIN/TS_FERRITIN_TOTALPOP_", year, ".rds"))
  
      # For SAS
      char.vars <- names(DATA)[which(lapply(DATA, class)=="character")][-1]
      
      DATA[, .SDcols=char.vars
           , (char.vars) := lapply(.SD, 
                                   function(x)
                                     gsub("^$", ".", x))]
  
      DATA[, .SDcols=char.vars
           , (char.vars) := lapply(.SD, 
                                   function(x)
                                     ifelse(is.na(x), ".", x))]
      
      setDF(DATA)
      
      write.dta(DATA, paste0("source_data/STATA/TESTSAFE/FERRITIN/TS_FERRITIN_TOTALPOP_", year, ".dta"))
        
    rm(DATA); gc()
    
  print(paste0(year, " completed"))
      
}


# Compile into a single unified dataset
for(year in 2000:2016){

  DATA <- readRDS(paste0("source_data/R/TESTSAFE/FERRITIN/TS_FERRITIN_TOTALPOP_", year, ".rds"))

  if(year==2000){

    ALL_FERRITIN <- DATA

    } else {

    ALL_FERRITIN <- rbind(ALL_FERRITIN, DATA)

    }

  print(paste0(year, " completed!"))

}
  
saveRDS(ALL_FERRITIN, "source_data/R/TESTSAFE/FERRITIN/TS_FERRITIN_TOTALPOP_2000_2016.rds")
