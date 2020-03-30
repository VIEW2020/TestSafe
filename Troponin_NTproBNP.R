
library(fst)

TEST_DESC <- read.fst("common_lookups/CURRENT_TSDESC_LOOKUP.fst", as.data.table = T)

# Troponins
trop.subst <- toupper(c("tni", "tnt", "trop", "troponin", "hs"))

TROP_DESC <- TEST_DESC[grepl(paste0(trop.subst, collapse = "|"), DESC_UPPER)]

troponins <- c("CARDIAC TROPONIN I", "CARDIAC TROPONIN T", "HS TROPONIN I", "HS TROPONIN T", "HSTROPONIN T",
               "ISTAT TROPONIN I", "TROPININ I", "TROPONIN", "TROPONIN I", "TROPONIN I (POC)", "TROPONIN I ADV",
               "TROPONIN I TO WAITEMATA DISTRI", "TROPONIN T", "TROPONIN T (HIGH SENSITIVITY)", "TROPONIN T (QUANTITATIVE)",
               "TROPONIN T - SERUM", "TROPONIN-I")

TROPONINS <- TEST_DESC[DESC_UPPER %in% troponins]


# NTproBNP
bnp.subst <- toupper(c("n-terminal", "^nt", "bnp", "natriurtic", "peptide"))

BNP_DESC <- TEST_DESC[grepl(paste0(bnp.subst, collapse = "|"), DESC_UPPER)]

bnp <- c("ATRIAL NATURIETIC PEPTIDE", "B-TYPE NATRIURETIC PEPTIDE", "BNP", "BRAIN NATRIURETIC PEPTIDE", "BRAIN NATURETIC PEPTIDE",
         "NT PROBNP", "NT-PROBNP", "NT-PROBNP - OTHER", "PRO BNP")

NTPROBNP <- TEST_DESC[DESC_UPPER %in% bnp]


EXPORT <- rbind(TROPONINS, NTPROBNP)

# Export
xlsx::write.xlsx(EXPORT, "common_lookups/TestSafe/Troponins_ntprobnp_2004_2018_totals.xlsx", row.names = F, showNA = F)
