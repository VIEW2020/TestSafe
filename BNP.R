
# Latest Haemoglobin Test Extraction for Total Population

# Last updated March 2018

library(foreign)
library(data.table)
  
setwd("V:/")

ENHI <- readRDS("source_data/R/ENHI BOTH TABLES/Archive/VIEW_P__VIEW_TSF_S_AUG2017.rds")
  
  setkey(ENHI, "VIEW_TESTSAFE_2NDARY")
  
  
for(year in 2004:2016){
  
  # i.    Import Total Population
  DATA <- readRDS(paste0("source_data/R/TESTSAFE/TESTSAFE_", year, ".rds"))
    
    setkey(DATA, "ENHI")
    
  # ii.   Filter by Description
  DATA[, OBSC_DESC := toupper(OBSC_DESC)]
  
  bnp.strings <- c("BNP", "BRAIN NATRIURETIC PEPTIDE", "BRAIN NATURETIC PEPTIDE", "NT-PROBNP", "NT-PROBNP - OTHER", "NT PROBNP")
  
  DATA <- DATA[OBSC_DESC %in% bnp.strings]
  
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
  saveRDS(DATA, paste0("source_data/R/TESTSAFE/BNP/TS_BNP_TOTALPOP_", year, ".rds"))
        
    rm(DATA); gc()
    
  print(paste0(year, " completed"))
      
}


# Compile into a single unified dataset
for(year in 2004:2016){

  DATA <- readRDS(paste0("source_data/R/TESTSAFE/BNP/TS_BNP_TOTALPOP_", year, ".rds"))

  if(year==2004){

    ALL_BNP <- DATA

    } else {

    ALL_BNP <- rbind(ALL_BNP, DATA)

    }

  print(paste0(year, " completed!"))

}

ALL_BNP <- ALL_BNP[, year := year(OBRQ_RECV_DATE)][!year==2017]

library(dplyr)


ALL_BNP %>% 
   group_by(year) %>% 
   summarise(freq = n(), 
             n = uniqueN(VIEW_ENHI_MASTER))
