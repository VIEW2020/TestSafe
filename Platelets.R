
# Latest Platelets Test Extraction for Total Population v3

# Last updated October 2018
# Nb: Version 3 draws from updated data from HealthAlliance for ACR_HB_PL delivered by Noureen October 2018

library(foreign)
library(data.table)
library(fst)

ENHI <- read.fst("source_data/R/ENHI BOTH TABLES/VIEW_P__VIEW_TSF_S_JUL2018.fst")
  
setDT(ENHI)
  setkey(ENHI, "VIEW_TESTSAFE_2NDARY")
  
pl.strings <- c("PLATE COUNT", "PLATE COUNT (CFU/ML)", "PLATELET COUNT", "PLATELETS", "RED CELLS AND PLATELETS", 
                "PLATELET CITRATE")
  
  
for(year in 2004:2017){
  
  # i.    Import Total Population
  DATA <- readRDS(paste0("source_data/R/TESTSAFE/TESTSAFE_ACR_HB_PL_", year, ".rds"))
    
    setkey(DATA, "ENHI")
    
  # ii.   Filter by Description
  DATA[, OBSC_DESC := toupper(OBSC_DESC)]
  
  DATA <- DATA[OBSC_DESC %in% pl.strings]
  
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

        # Some results have no units [very few: <10 records for each year of data].
        DATA <- DATA[!is.na(OBSR_UNITS)]
  
        # Apply range Check
        # Remove results that are >=10,000 x10^9/l. No lower limit except 0. 
        DATA <-  DATA[OBSR_RESULT_NUM>0 & OBSR_RESULT_NUM< 10000]
   
   # Remove records with non-final or final correction report status
   # Can be alot ~10% results removed each year
   DATA <- DATA[OBRQ_REPORT_STATUS=="F" | OBRQ_REPORT_STATUS=="FC"]       
        
  # v.   Assemble Date & Timestamp
  setnames(DATA, "OBRQ_RECV_DATE_TIMESTAMP", "OBRQ_RECV_TIMESTAMP")
  
  DATA[, OBRQ_RECV_DATE_TIMESTAMP := as.POSIXct(paste(OBRQ_RECV_DATE, 
                                                        OBRQ_RECV_TIMESTAMP, 
                                                        sep=" "), format="%Y-%m-%d %H:%M:%S")]

  # vi.   ENHI Swapover
  DATA <- ENHI[DATA, nomatch=0][,!c("VIEW_ENHI_2NDARY", "VIEW_TESTSAFE_2NDARY"), with=F]
  
  
  # vii.  Remove Duplicates: Same person, sameday, same result
  #       Very few: Removes up to <700 records (in 2016)
  DATA <- DATA[, by=list(VIEW_ENHI_MASTER, OBRQ_RECV_DATE, OBSR_RESULT_NUM)
               , index := seq_len(.N)][index==1, !c("index"), with=F]
  
  
  # Reduce down to one record per day
  # If there are multiple records on the same day, then keep on the last one of the day
  #       Removes up to 24230 records (in 2016)
  DATA <- DATA[order(OBRQ_RECV_DATE_TIMESTAMP, decreasing = T)
               , by=list(VIEW_ENHI_MASTER, OBRQ_RECV_DATE)
               , index:=seq_len(.N)][index==1, !c("index"), with=F]
  
  
  # viii. Export
  saveRDS(DATA, paste0("source_data/R/TESTSAFE/PLATELETS/TS_PLATELETS_TOTALPOP_", year, "_v3.rds"))
        
    rm(DATA); gc()
    
  print(paste0(year, " completed"))
      
}

  
# Compile into a single unified dataset
for(year in 2000:2016){

  DATA <- readRDS(paste0("source_data/R/TESTSAFE/PLATELETS/TS_PLATELETS_TOTALPOP_", year, ".rds"))

  if(year==2000){

    ALL_PLATELETS <- DATA

    } else {

    ALL_PLATELETS <- rbind(ALL_PLATELETS, DATA)

    }

  print(paste0(year, " completed!"))

}
  
saveRDS(ALL_PLATELETS, "source_data/R/TESTSAFE/PLATELETS/TS_PLATELETS_TOTALPOP_2000_2016.rds")
