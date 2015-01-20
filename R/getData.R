#' identify intervals where the box is most likely 'on'
#' 
#' @description within a given period of time, find all intervals where the 
#'   specified box(es) were, according to themselves, 'on'. As in, fully 
#'   functional.
#'   
#' @param con an open connection (to SNBatWESTERHOLZ2, but also works for 
#'   SFatWESTERHOLZ)
#' @param startd starting date of the period of interest
#' @param endd end date of the period of interest
#' @param bx box (or vector of boxnumbers) for which to find 'on' periods
#'   
#' @usage boxon_data(con, startd=Sys.Date()-31,endd=Sys.Date(), bx= c(1:277))
#' @return a dataframe with, for each downloaded file, the boxnumber, download 
#'   date, start date (truncated to the given startd), the last datetime 
#'   (truncated to the given endd), the upload status, the total number of days 
#'   covered by this chunk of time, and the total number of 'on' days in this 
#'   chunk of time. Currently, this function is used to plot the background 
#'   rectangles for the actograms.
#' @section NB: if onDays is larger than totalDays, the file has an RTC problem.
#'   
#' @section POINT OF IMPROVEMENT: This function currently only uses data 
#'   available in the file_status table, because that is much faster than 
#'   trawling RAW_YYYY. However, this approach means that for instance 
#'   intermittent malfunctioning (where the box switches to garble halfway 
#'   through the file, but switches back to normal behaviour later on) is 
#'   missed, and that the function errs on the side of type 2 error. I think 
#'   this function should be as conservative as possible so that you can trust 
#'   that IF a box displays as 'ON' it fucking well IS on. So i think the
#'   function should either query RAW_YYYY, or flag RTC-problem files (because
#'   boxes that are backwards-jumping will still produce valid datetimes within
#'   the start and end limits of the file)

boxon_data = function(con, startd=Sys.Date()-31,endd=Sys.Date(), bx= c(1:277)) 
 {
  
  if (missing(con)) {
     con = dbcon(user='snb',password='cs',database='SNBatWESTERHOLZ2')
     on.exit(dbDisconnect(con))
  }
  
  #date_ is the only reliable datetime in file_status, so select files based on date_
  
  # increase interval by one day both sides
  startd = as.POSIXct(startd,tz='UTC')-86400
  endd   = as.POSIXct(endd,tz='UTC')+86400
 
  onoff     = dbq(con,paste0("SELECT box,date_,date_prev startd,dt_last + INTERVAL (FUNCTIONS.isDST(dt_last)-1) HOUR dt_last,upload_status 
                               from file_status g 
                               where g.box in (",paste(bx,collapse = ','),") 
                                 and upload_status >0 and (",shQuote(startd)," between date_prev and date_ 
                                                           or ",shQuote(endd)," between date_prev and date_
                                                            or date_ between ",shQuote(startd)," and ",shQuote(endd),")

                               ORDER BY box, date_"))	
  
  onoff$date_  = as.POSIXct(onoff$date_,tz = 'UTC')
  onoff$startd = as.POSIXct(onoff$startd,tz = 'UTC')
  onoff$dt_last =as.POSIXct(strptime(onoff$dt_last, format = '%Y-%m-%d %H:%M:%S',tz='UTC'))
  
  #cutoff
  onoff$startd[which(onoff$startd<startd)] = startd
  onoff$date_[which(onoff$date_>endd)] = endd
  onoff$dt_last[which(onoff$dt_last>endd)] = endd
  onoff = subset(onoff,onoff$dt_last>startd)
  
  #calculate (round to .1 day to get rid of smallish discrepancies in pull/actual datetime)
  onoff$totalDays = round(as.numeric(difftime(onoff$date_,onoff$startd),'days'),1)
  onoff$onDays    = round(as.numeric(difftime(onoff$dt_last,onoff$startd),'days'),1)
  
  # boxes that have higher onDays than totalDays have datetime errors.
  
  # fix random years (mostly in 2009)
 # onoff$yr = as.numeric(substring(onoff$date_,1,4))
#  onoff$dt_last = backtoPOSIXct(ifelse(!(as.numeric(substring(onoff$dt_last,1,4))>=onoff$yr-1&as.numeric(substring(onoff$dt_last,1,4))<=onoff$yr),
 #                                      as.POSIXct(strptime(paste0(ifelse(substring(onoff$date_,6,7)=='01'&substring(onoff$dt_last,6,7)!='01',onoff$yr-1,onoff$yr),substring(onoff$dt_last,5,19)),format = '%Y-%m-%d %H:%M:%S')),
  #                                     onoff$dt_last))

  
  return(onoff)
}