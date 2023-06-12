# This script transfers data from Databricks
rm(list = ls())

# Setup Databricks connection --------------------------------------------------
con <- DBI::dbConnect(odbc::odbc(),
                      "Databricks",
                      timeout = 60,
                      PWD = rstudioapi::askForPassword("Password please:"))

# Transfer data from DataBricks ------------------------------------------------
chunks <- 10
df <- NULL

for (i in 1:chunks) {
  
  print(paste0("Transferring chunk ",i," of ",chunks,"."))
  
  tmp <- DBI::dbGetQuery(con, paste0("SELECT * FROM dars_nic_391419_j3w9t_collab.ccu051_out_analysis WHERE CHUNK='",i,"'"))
  
  df <- rbind(df,tmp)
  
}

# save as table 
data.table::fwrite(df,paste0("ccu051_cohort",gsub("-","",Sys.Date()),".csv.gz"))

