
# TestSafe Raw Data Preparation 2019

# Last Updated July, 2019

# Main functions: 
# 1.  Reading text files into R and converting to fst files
# 2.  Removing useless variables, observations, duplicates
# 3.  Overal pre-clean of TestSafe source data  

# Very Important:
# Must use argument fill = TRUE in fread() as some text fields contain : and | symobols
# Use OBRQ_DATE_ENQ as the normalised date / timestamp as intended by HealthAlliance


library(data.table)
library(fst)

ENHI <- read.fst("source_data/R/ENHI BOTH TABLES/VSIMPLE_PS__TEST_S_JUN2019.fst", as.data.table = TRUE)

setkey(ENHI, VIEW_TESTSAFE_2NDARY)

colnames <- fread("V:/common_lookups/TestSafe/colname.csv")$name

# Variables for removal
vars.to.remove <- c("PAT_RELIGION","FAC_ADDR_CODE","FAC_ADDR_STREET","FAC_ADDR_OTHER",       
                    "FAC_ADDR_CITY","FAC_ADDR_CODE","FAC_ADDR_STREET","FAC_ADDR_OTHER",       
                    "FAC_ADDR_CITY","FAC_ADDR_CODE","FAC_ADDR_STREET","FAC_ADDR_OTHER",       
                    "FAC_ADDR_CITY","OBRQ_ATT_DOC_CODE","OBRQ_ATT_DOC_SCH","OBRQ_REF_DOC_CODE",    
                    "OBRQ_REF_DOC_SCH","OBRQ_CON_DOC_CODE","OBRQ_CON_DOC_SCH","OBRQ_SRC_ADDITIVE",    
                    "OBRQ_SRC_SITE_MOD","OBRQ_SRC_TEXT","OBRQ_ADM_DOC_CODE","OBRQ_ADM_DOC_SCH",     
                    "OBRQ_COLLECTOR_ID","OBRQ_COLLECTOR_LOCN","OBRQ_COLLECTOR_METHOD",
                    "PAT_RACE",	"PAT_DEATH_DT",	"PAT_DEATH_INDICATOR", "PAT_MARITAL_STATUS", 
                    "OBRQ_CLINICAL_INFO", "FAC_ADDR_STATE", "OBRQ_PLACER_ORD", "OBRQ_SRC_SITE", 
                    "OBRQ_ORGANISATION", "OBRQ_ORGANISATION_NAME", "OBRQ_REQ_DATE",
                    "OBRQ_ORD_DOC_CODE","OBRQ_SERV_ID","OBRQ_OBS_DATE","OBRQ_SPEC_RECV_DT","OBSR_OBSSUBID")

# Character vars
char.vars <- c("PAT_DOB","PAT_SEX","FACILITY_CODE","FACILITY_NAME",   
               "VIS_ID","VIS_ID_SCH","VIS_PAT_CLASS","VIS_ADM_DATE","OBRQ_DATE_ENQ","OBRQ_FILLER_FCTY",  
               "OBRQ_FILLER_ORD","OBRQ_ORD_DOC_SCH","OBRQ_SERV_ID_SCH",    
               "OBRQ_REP_DATE","OBRQ_RECV_DATE","OBRQ_PRIORITY","OBRQ_SERVICE","OBRQ_ABNORMAL",     
               "OBRQ_RESULT_TYPE","OBRQ_REPORT_STATUS","OBRQ_REPORT_TYPE","OBRQ_FILLER_APP","OBSR_OBSID",        
               "OBSR_OBSID_SCH","OBSC_DESC","OBSSSUB_DESC","OBSR_RESULT_CHAR","OBSR_RANGE",    
               "OBSR_ABNORMAL","OBSR_STATUS","OBSR_UNITS","OBSR_RES_TYPE","OBSR_LONG_RES_FLAG")

filled.files <- c("DataExtract_2007-09.txt", "DataExtract_2007-10.txt", "DataExtract_2012-01.txt", "DataExtract_2013-02.txt",
                  "DataExtract_2013-07.txt", "DataExtract_2013-11.txt", "DataExtract_2015-09.txt", "DataExtract_2017-11.txt", 
                  "DataExtract_2018-01.txt", "DataExtract_2018-02.txt", "DataExtract_2018-04.txt", "DataExtract_2018-05.txt", 
                  "DataExtract_2018-06.txt", "DataExtract_2018-07.txt", "DataExtract_2018-08.txt", "DataExtract_2018-09.txt", 
                  "DataExtract_2018-11.txt", "DataExtract_2018-12.txt")


# ---- A. Import & variable clean -----

files.list <- list.files("V:/source_data/Text Files/TestSafe/", pattern = ".txt")

for(file in files.list){
  
  DATA <- if(file %in% filled.files){
    
    read.table(paste0("V:/source_data/Text Files/TestSafe/", file), 
               na.strings = "NA", 
               strip.white = TRUE,
               sep = "|",
               quote = "",
               fill = TRUE) 
  } else {
    
    fread(paste0("V:/source_data/Text Files/TestSafe/", file), 
          na.strings = "NA", 
          strip.white = TRUE,
          sep = "|",
          quote = "",
          fill = TRUE)
  }
  
  names(DATA) <- colnames
  
  DATA <- DATA[, -vars.to.remove, with = F]
  
  DATA[, DESC_UPPER := toupper(OBSC_DESC)]
  
  DATA[, (char.vars) := lapply(.SD, 
                               function(x)
                                 gsub("^$", NA, x)), .SDcols = char.vars]
  
  main.dates <- c("RESULT_DATE", "RESULT_DATE_TIMESTAMP")
  
  DATA[, (main.dates) := list(as.Date(as.character(OBRQ_DATE_ENQ),format = "%Y-%m-%d"),
                              strftime(as.POSIXct(OBRQ_DATE_ENQ, format="%Y-%m-%d %H:%M:%S"),format="%H:%M:%S"))]

  setkey(DATA, ENHI)
  DATA <- ENHI[DATA, nomatch = 0][, -c("VSIMPLE_INDEX_2NDARY", "VIEW_TESTSAFE_2NDARY"), with = F]
  
  save.name <- gsub(".txt", ".fst", 
                    gsub("DataExtract", "TestSafe", 
                         gsub("-", "_", file)))
  
  write.fst(DATA, paste0("C:/temp/", save.name), 75)
  
  print(paste0(file, " completed")); if(file=="DataExtract_2017-06.txt"){gc()}
  
}


# ---- B. Observations removal -----
# This step is for very basic and preliminary cleaning only
# NB: Use this process to also capture lookup variables

files.list <- list.files("C:/temp/")

for(file in files.list){
   
   DATA <- read.fst(paste0("C:/temp/", file), as.data.table = T)
   
   desc.to.remove <- grep(paste0("^[[:punct:]]|^[[:digit:]]$|COMMENT|REVIEW|AUTHORISED BY|IMAGES|SEND AWAY|ADMINISTERED|ADMINISTRATION|BOOKING"), 
                          unique(DATA$DESC_UPPER), 
                          value = T)
   
   DATA <- DATA[OBRQ_REPORT_STATUS!="X"]
   DATA <- DATA[!DESC_UPPER %in% desc.to.remove]
   DATA <- unique(DATA)
   
   if(file == files.list[1]){
     FACILITY <- unique(DATA[, c("FACILITY_NAME", "FACILITY_CODE"), with = F])
     DESC_CODES <- unique(DATA[, "OBSC_DESC", with = F])
   } else {
     FACILITY <- unique(rbind(unique(DATA[, c("FACILITY_NAME", "FACILITY_CODE"), with = F]),
                              FACILITY))
     DESC_CODES <- unique(rbind(unique(DATA[, "OBSC_DESC", with = F]),
                                DESC_CODES))
   }
   
   save.name <- paste0("VSIMP_TESTSAFE", gsub("TestSafe", "", file))
   
   write.fst(DATA, paste0("C:/temp/", save.name), 75)
   
   print(paste0(file, " completed")); rm(DATA)
   
}


# ---- C. Lookup Tables -----

DESC_CODES$DESC_UPPER <- toupper(DESC_CODES$OBSC_DESC)
FACILITY[is.na(FACILITY_NAME), FACILITY_NAME := FACILITY_CODE]

DESC_CODES <- DESC_CODES[order(DESC_UPPER)]
FACILITY <- FACILITY[order(FACILITY_NAME)]

write.fst(DESC_CODES, "source_data/R/TESTSAFE/Test_Descriptions_July2019.fst")
write.fst(FACILITY, "source_data/R/TESTSAFE/Test_Facility_Index_July2019.fst")


for(year in 2004:2018){
   
  files.list <- list.files("C:/temp/", pattern=paste0(year))
  
  output <- rbindlist(lapply(files.list,
                             function(x){
                               DATA <- read.fst(paste0("C:/temp/", x), as.data.table = TRUE)
                               DATA[, list(n = .N), by = DESC_UPPER]
                             }))
  
  output <- output[, list(n = sum(n)), by = DESC_UPPER]

  setnames(output, "n", paste0("n", year))
  
  AllOutputs <- if(year==2004){
    output
  } else {
    merge(AllOutputs, output,
          by = "DESC_UPPER",
          all = T)
  }
  print(paste(year, "completed"))
}


# Tidy UP
desc.to.remove <- grep(paste0("^[[:punct:]]$|^[[:digit:]]$|^$|COMMENT|REVIEW|AUTHORISED BY|IMAGES|SEND AWAY|ADMINISTERED|ADMINISTRATION|BOOKING"), 
                       unique(AllOutputs$DESC_UPPER), 
                       value = T)

extra.removal <- grep(paste0("MINUTE$|^OTHER$|NOTE|^TEST |^REPORT|^BOOKING|^REPORT|^USE|USED|^NA$|MESSAGE|CONSULT"), 
                       unique(AllOutputs$DESC_UPPER), 
                       value = T)

to.remove <- c(desc.to.remove, extra.removal)

AllOutputs <- AllOutputs[!DESC_UPPER %in% to.remove]

# Total Frequency
AllOutputs[, nTotal := apply(.SD, 1,
                             function(x)
                               sum(x, na.rm = T)), .SDcols = grep("^n", names(AllOutputs), value = T)]

AllOutputs <- AllOutputs[nTotal != 1]

fst::write.fst(AllOutputs, "common_lookups/TestSafe/Test_Desc_Yearly_nCount_July2019.fst")
xlsx::write.xlsx(AllOutputs, "common_lookups/TestSafe/Test_Desc_Yearly_nCount_July2019.xlsx", row.names = F, showNA = F)
