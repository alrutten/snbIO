

boxon_data = function(con, startd=Sys.Date()-31,endd=Sys.Date(), bx= c(1:277)) 
 {
  
  if (missing(con)) {
     con = dbcon()
     on.exit(dbDisconnect(con))
  }
  
  #date_ is the only reliable datetime in file_status.
  #last_dt
  
  startd = as.POSIXct(startd,tz='UTC')-86400
  endd   = as.POSIXct(endd,tz='UTC')+86400
 
  onoff     = dbq(con,paste0("SELECT box,date_,date_prev startd,dt_last,upload_status 
                               from file_status g 
                               where g.box in (",paste(bx,collapse = ','),") 
                                 and upload_status >0 
                               ORDER BY box, date_"))	
  
  onoff$date_  = as.POSIXct(onoff$date_,tz = 'UTC')
  onoff$startd = as.POSIXct(onoff$startd,tz = 'UTC')
  onoff$dt_last =as.POSIXct(strptime(onoff$dt_last, format = '%Y-%m-%d %H:%M:%S',tz='UTC'))
  
  #find the first and the last file, with margins
  
    onoff$select = ifelse((onoff$startd<=startd&onoff$date_>startd)|onoff$date_>endd,1,0)
  onoff           = ddply(onoff,.(box),transform, select = cumsum(select))
  onoff           = onoff[which(onoff$select==1),]	
  
  #cutoff
  onoff$startd[which(onoff$startd<startd)] = startd
  onoff$date_[which(onoff$date_>endd)] = endd
  onoff$dt_last[which(onoff$dt_last>endd)] = endd
  
  #calculate (round to .1 day to get rid of smallish discrepancies in pull/actual datetime)
  onoff$totalDays = round(as.numeric(difftime(onoff$date_,onoff$startd),'days'),1)
  onoff$onDays    = round(as.numeric(difftime(onoff$dt_last,onoff$startd),'days'),1)
  
  
  # fix random years (mostly in 2009)
 # onoff$yr = as.numeric(substring(onoff$date_,1,4))
#  onoff$dt_last = backtoPOSIXct(ifelse(!(as.numeric(substring(onoff$dt_last,1,4))>=onoff$yr-1&as.numeric(substring(onoff$dt_last,1,4))<=onoff$yr),
 #                                      as.POSIXct(strptime(paste0(ifelse(substring(onoff$date_,6,7)=='01'&substring(onoff$dt_last,6,7)!='01',onoff$yr-1,onoff$yr),substring(onoff$dt_last,5,19)),format = '%Y-%m-%d %H:%M:%S')),
  #                                     onoff$dt_last))
  
 onoff$select =  NULL
  
  
  return(onoff)
}