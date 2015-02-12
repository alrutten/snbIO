#' conditional inference tree used to assign direction
#' 
#' decision tree, based on the 2013 calibrations, used to predict direction for 
#' previously extracted and summarised events.
#' 
#' @details \itemize{ \item the tree was trained using party::ctree on the first
#'   ten visits for each video in the 2013 calibrations (351 visits total). In 
#'   the tree data, LB = -1 means only the outer barrier is firing; 0 means both
#'   barriers are firing, and 1 means only the inner barrier fires. \item as the
#'   tree is based on chick-feeding data, predictions on direction outside the 
#'   chick-feeding period are inaccurate. Because of the nature of the feeding 
#'   visits, most transits outside the breeding season are classified as 'out'.}
#'   
#' @seealso 
#' \code{\link[party]{ctree}},\href{http://scidb.orn.mpg.de/scidbwiki/westerholz/doku.php?id=smart_nest_box_project:calibrations:2013}{snb
#' calibrations 2013}
#' 
#' 
#' 
#' @usage data(ct)
#' @section tree: plot(ct)
#' @name ct
#'   
NULL

#'
#'assign readings to separate transit events
#'
#'\code{assignEvents} interprets whether a row of data belongs to the same or a 
#'separate transit
#'
#'@usage assignEvents(d,maxt=2.1)
#'  
#'@seealso  \code{\link{extractEvents}} for extracting and summarising events, 
#'  \code{\link{loadEvents}} for the wrapper function that identifies, extracts,
#'  assigns direction, and loads events into BETA_EventsYYYY.
#'  
#'@param d A dataframe containing data from a single box from 
#'  SNBatWESTERHOLZ.RAWYYYY, as-is.
#'@param maxt Time-cutoff in seconds, if the timedifference between a line and
#'  the next is over maxt the line gets assigned to a new event.
#'  
#'@details \describe{ \item{general}{this function is called from loadEvents(), 
#'  which processes the data in RAW_YYYY and saves events (visits/transits) to 
#'  BETA_EventsYYYY. Running assignEvents() solo is only interesting if you're 
#'  tweaking event assignment.} \item{event assignment} {The following criteria
#'  are used to identify the start of a new event: \itemize{ \item a change in
#'  boutlength of more than 20 \item a different transponder is read \item the
#'  current reading is more than maxt seconds after the previous one } Other
#'  options include the firing of both lightbarriers, which has a low rate of
#'  false positives- however, if one of the barriers malfunctions, you'd still
#'  want to try and extract proper transits, for which the change in boutlength
#'  is the most accurate. }}
#'  
#'@return the original dataframe with added boutID (event ID) and
#'  transponder-with-na's-filled
#'  
#'@section PLEASE NOTE, FIX NEEDED: the way events are assigned now by 
#'  assignEvents() is problematic when two birds visit the box simultaneously; 
#'  each time a different transponder is read starts a new event, so when both 
#'  birds visit their visits will typically be split into multiple visits. The 
#'  problem with splitting the dataframe on transponder before assigning is that
#'  you then miss the first LB reading for most visits, which is essential for 
#'  direction assignment. I wanted to solve this by not using a change in 
#'  transponder reading to assign a new event, and changing extractEvents() and 
#'  the ct tree accordingly, but i haven't tested this on the calibration data 
#'  yet.
assignEvents = function(d,maxt = 2.1) {
  #d must have datetime_, r_pk, transp, box, bout_length, LB
  # returns d plus boutID
  # RUN PER BOX
  if (length(unique(d$box))>1) stop('run this per box (use lapply/ddply')
  d = d[order(d$r_pk),]
  # columns to keep on return()
  keepvars = c(names(d),'boutID','filltransp')
  
  # add previous and next times, and previous boutlength
  d$datetime_ = as.POSIXct(d$datetime_,tz='UTC')
  d$timePrev=c(d$datetime_[1],d$datetime_[-nrow(d)])
  d$timenxt=c(d$datetime_[-1],d$datetime_[nrow(d)])
  d$blPrev=c(d$bout_length[1],d$bout_length[-nrow(d)])
  
  # find places where readings are more than maxt seconds apart.
  # this is here because this information is used to replace na transpondervalues
  timeout    = as.POSIXct(d$timePrev,tz='UTC')<(as.POSIXct(d$datetime_,tz='UTC')-maxt)
  
  # make filltransp variable that has a transpondervalue for every line (assigning activity to the last known transponder) 
  if(is.na(d$transp[1])) d$transp[1]='foo'
  #the first NA transponders of the next event will be wrongly attributed to the previous bird-make sure transp does not spill over into next visit
  d$filltransp=na.locf(d$transp,na.rm=FALSE)
  d$transp[timeout] = 'ta' 
  d$filltransp[d$filltransp=='ta'] = NA
  d$filltransp=na.locf(d$filltransp,na.rm=FALSE,fromLast = TRUE)
  d$transp[d$transp%in%c('foo','ta')] = NA
  
  # add the previous transponder
  d$transpPrev=c('foo',d$filltransp[-nrow(d)])
  
    # bout start/stop criteria: different transponder, drop in boutlength,
  # increase in boutlength, reading over maxt seconds after previous bldrop and
  # blinc are separate because they describe the start (bldrop) and stop (blinc)
  # of an event; it's useful to have them separate when you're tweaking the
  # function.
  
  othertransp = d$filltransp!=d$transpPrev&(d$filltransp!='foo'&d$transpPrev!='foo')
  bldrop     = d$bout_length<d$blPrev-20
  blinc      = d$bout_length>d$blPrev+20
  
  #add boutID
  d$boutID = 0
  d$boutID[timeout|bldrop|blinc|othertransp] = 1
  d$boutID[1] = 1
  d$boutID = cumsum(d$boutID)
  
  # reset transp to NA when there is not actually a transponder read
  filltransp = ddply(d,.(boutID),summarise, filltransp = unique(transp[!is.na(transp)]))
  d$filltransp = NULL
  d = merge(d,filltransp,all.x=TRUE)
  
  # return only the interesting variables  
  return(d[order(d$r_pk),keepvars])
}

#'extract and summarise events
#'
#'@seealso  \code{\link{assignEvents}} for assigning an event ID to the raw 
#'  data, \code{\link{loadEvents}} for the wrapper function that identifies, 
#'  extracts, predicts direction, and loads events into BETA_EventsYYYY.
#'  
#'@param d a dataframe as returned by assignEvents()
#'  
#'@details various parameters that in the calibration data differ between 'ins' 
#'  and 'outs' are extracted for each event: \itemize{\item the duration of the 
#'  event (in s) of each visit (room for improvement: could be in ms). Shorter 
#'  for 'in'. \item the number of lines with no transponderreading before the 
#'  first transponderreading. higher for 'ins' for birds that hang. \item the 
#'  first and last LB reading of the event. \item the predicted direction based 
#'  on the first and last LB reading. \item whether there is an actual transit 
#'  (both LB's firing) occurring during the event. \item the number of rows in 
#'  the event (disregarding bout_length, so not the actual number of lines in 
#'  the datafile). Less for 'ins'. \item the average bout_length during the 
#'  event. higher for 'ins'. Higher variance in bout_length for unclear visits 
#'  (not currently considered or calculated; room for improvement). \item the 
#'  sum of all boutlengths in the event. This is the actual number of lines in 
#'  the raw datafile. \item starting time, number of lines, sum of all 
#'  boutlenghts, and direction based on LB readings for the previous event per 
#'  transponder. \item the ratio between the number of lines in the current
#'  event and the number of lines in the previous event for that transponder.}
#'  Only events that contain a transit or a transponderreading (or both) are
#'  returned.
#'  
#'@return a dataframe with, for each valid event, a number of parameters that 
#'  can be used to assign direction (which happens in loadEvents())
extractEvents = function(d) {
  # re-assign LB readings so that -1 is outside, 0 is both firing, 1 is inside
  newLB = c('10','12','02')
  d$LB = match(d$LB,newLB)
  d$LB = d$LB-2
  #d$LB[is.na(d$LB)] = 0
  #re-sort dataframe to original order (just in case)
  d=d[order(d$r_pk),]
  # summarise per bout
  events=ddply(d[d$boutID>0,],.(box,id,boutID),summarise, 
               transp=filltransp[1], #by necessity, each event is associated with only one transponder
               startt=min(datetime_), #starting time
               endt=max(datetime_),   # end time
               duration = as.numeric(difftime(max(datetime_),min(datetime_), tz='UTC', units = 'secs')), #event duration (longer for 'in')
               nline_beforetransp =which(!is.na(transp))[1], #number of lines in event before first transponderreading (higher for 'in')
               first_LB=LB[which(!is.na(LB))][1], #first LB reading
               last_LB = tail(c(NA,LB[which(!is.na(LB))]),1), #last LB reading
               anyLB12 = any(LB==0,na.rm=TRUE), #is there any actual transit (both LBs firing)
               n_lines=length(LB), #number of rows in event (higher for 'in')
               mean_bl = mean(bout_length), #average boutlength in event (higher for 'in')
               sum_bl=as.numeric(sum(bout_length)), #sum of boutlengths (=actual number of lines in rawdatafile, higher for 'in')
               ratio_l_bl = n_lines/sum_bl) #brain fart
  
  # discard events where there's no transit and no transponder
  events=events[!(!events$anyLB12&is.na(events$transp)),] 
  
  #if there is any data left:
  if (nrow(events)>0) {
    #find LBdir; IF this can be assigned it's the most reliable in the calibration data (98% of event directions identified correctly)
  #events$LBdir = events$last_LB - events$first_LB
    events$LBdir=0
	  events$LBdir[which(events$first_LB==-1)] = 1
	  events$LBdir[which(events$first_LB==1)] = 2
	  events$LBdir[which(events$last_LB==1 & events$first_LB!=1)] = 1
	  events$LBdir[which(events$last_LB==0 & events$first_LB!=-1)] = 2
  
  #do previouses per transponder (ratio between several of the extracted
  #parameters between current and previous event for that bird differ between
  #ins and outs)
  
    events = events[order(events$transp,events$startt),]
    prevevents = ddply(events,.(transp),  function(x) rbind(x[1,], x[-nrow(x),]))		
    names(prevevents) = paste0('prev_',names(events)) 
    events = cbind(events,prevevents[,c('prev_n_lines','prev_sum_bl','prev_startt','prev_transp','prev_LBdir')])
    events$ratio_l_prevl = events$n_lines/events$prev_n_lines
    }
   return(events[order(events$startt),])
}  

#'assign, summarise, and upload events
#'
#'
#'@description \code{loadEvents} is the wrapper around 
#'  \code{\link{assignEvents}}, which assigns a boutID to the raw data, and 
#'  \code{\link{extractEvents}}, which extracts and summarises eligible events. 
#'  \code{loadEvents} assigns event ids to previously unprocessed raw data, 
#'  extracts and summarises events that contain an actual transit (both LB's 
#'  fireing) or a transponderreading or both, assigns direction to these events 
#'  using the `ct` inference tree object in the package to assign direction to 
#'  each event, and loads to BETA_EventsYYYY.
#'  
#'@details pipeline: \enumerate{\item looks for fileids that are in 
#'  SNBatWESTERHOLZ2.file_status but not in BETA_EventsYYYY.(room for 
#'  improvement: this way, files with no valid events are processed every time 
#'  because the fileid will never show up in any events table). \item for each 
#'  fileid, queries RAW_YYYY for all data from this file. \item matches 
#'  transponder to bird/fieldteam ID. \item saves one record for each distinct 
#'  transponder with invalid datetimes. \item assigns events using 
#'  \code{\link{assignEvents}}. \item summarises and extracts valid events using
#'  \code{\link{extractEvents}}. \item predicts event direction using 
#'  \code{\link{ct}}. \item saves events to BETA_EventsYYYY.} If you want to 
#'  re-assign events for a certain file, delete all occurences of that fileid 
#'  from BETA_EventsYYYY.
#'  
#'@seealso  \code{\link{extractEvents}} for extracting and summarising events, 
#'  \code{\link{assignEvents}} for assigning an event ID to the raw data., 
#'  \code{\link{ct}} for a description of the inference tree used to predict 
#'  direction.
#'  

#' @param year_ year for which to find unloaded events and assign, extract, and
#'   load these.
#'@return TRUE if the data get processed and loaded correctly.
loadEvents = function(year_=substring(Sys.Date(),1,4)) {

  con = dbcon(database = 'SNBatWESTERHOLZ2',user='snb',password = 'cs')
  on.exit(closeCon(con))
  
  IDs = birdIDs()
  data(ct)
  
  vars = paste0("c('",paste(strsplit(gsub(' ','',paste(ct@data@formula$input)),'\\+')[[2]],collapse='\',\''),"')")
  keepvars =  dbq(con, "SHOW COLUMNS FROM ALL_EVENTS FROM SNBatWESTERHOLZ2")$Field
  #fids = dbq(con,'select id from file_status where year_ = 2013 and box between 100 and 201 order by id desc')$id
  
  #see if there is new data to be processed
  foo = try({fids = dbq(con, paste0("select f.id from file_status f 
                                    LEFT JOIN (select distinct id from BETA_Events",year_,") b on f.id = b.id 
                                    where b.id IS NULL and year_ = ",year_," order by id desc"))$id},silent=TRUE)
  
  if ((!class(foo)=='try-error')&length(fids)>0) {
    for (i in 1:length(fids)) {
      d = dbq(con, paste0("select * from RAW_",year_," where id = ",fids[i]))
      if (nrow(d)>0) {
        # if there are NA datetimes, keep one record for each transponder
        nulls = d[is.na(d$datetime_),]
        if (nrow(nulls) > 0) {
          #i was going to plug the count into te duration field (so that it would still be clear from the counts who is feeding the most) but then decided this would be bad.
          nulls = ddply(nulls,.(box,datetime_,transp,id),summarise, count = length(transp)) 
          nulls = merge(nulls,IDs, by.x='transp',by.y='transponder',all.x=TRUE,incomparables = NA) 
          nulls[,setdiff(keepvars, names(nulls))] = NA
         writeload(nulls[,keepvars],con=con,db='SNBatWESTERHOLZ2',tb = paste0('BETA_Events',year_))
        }
        #now, assign & extract events for all valid datetimes
        d = d[!is.na(d$datetime_),]
        if (nrow(d)>0) {
          dd = assignEvents(d)
          dd = extractEvents(dd)
        if (nrow(dd)>0) {
          #FIXME 2014-07-16: changed LBdir output from extractEvents to 1(in), 2(out), 0(none); didn't update tree so have to change it back here
          dd$LBdir[dd$LBdir==2] = -1
          dd$direction_raw = round(predict(ct,newdata = dd[,eval(parse(text=vars))]),2)
          dd$direction = round(dd$direction_raw,0)
          dd$direction[abs(dd$direction_raw-dd$direction)>0.1] = 0
                 
          dd = merge(dd,IDs, by.x='transp',by.y='transponder',all.x=TRUE,incomparables = NA) 
        
          writeload(dd[,keepvars],con=con,db='SNBatWESTERHOLZ2',tb = paste0('BETA_Events',year_),ignore=TRUE)
        }
      }
    }
  }
  }
}

#' identify, extract, and upload smartfeeder visits
#' 
#' \code{loadSFEvents} assigns, extracts, and loads events from smartfeederdata 
#' from SFatWESTERHOLZ.RAW_YYYY. Visit data gets loaded into 
#' SFatWESTERHOLZ.EventsYYYY.
#' 
#' @param cutoff if the time in seconds elapsed between two readings is larger 
#'   than the cutoff value, data gets assigned to a new visit.
#' @details pipeline similar to \code{\link{loadEvents}}, but event assignment 
#'   and summarisation is simplified and no direction prediction takes place. 
#'   Event assignment is based on time elapsed since previous reading only.
#'   
#' @return TRUE if data gets loaded successfully.
loadSFEvents = function(cutoff=2) {
  
  con = dbcon(database = 'SFatWESTERHOLZ',user='snb',password = 'cs')
  on.exit(closeCon(con))
  IDs = birdIDs()
  
  keepvars = dbq(con,"SHOW COLUMNS FROM ALL_EVENTS from SFatWESTERHOLZ")$Field
  
  foo = try({fids = dbq(con, "select year_, f.id from file_status f 
                                    LEFT JOIN (select distinct id from ALL_EVENTS) b on f.id = b.id 
                                    where b.id IS NULL order by id desc")},silent=TRUE)
 
  
  if ((!class(foo)=='try-error')&nrow(fids)>0) {
    for (i in 1:nrow(fids)) {
      d = dbq(con, paste0("select * from RAW_",fids$year_[i]," where id = ",fids$id[i]," 
                           and transp IS NOT NULL order by datetime_ asc"))
      if (nrow(d)>0) {
          nulls = unique(subset(d,is.na(datetime_)))
          d = subset(d,!is.na(datetime_))
          if (nrow(d)>0) {
            d$prev = c(d$datetime_[1],d$datetime_[-nrow(d)])
            d$event = 0
            d$event = cumsum(as.numeric(as.numeric(difftime(as.POSIXct(d$datetime_),as.POSIXct(d$prev)))>cutoff ))#new visit if no reading for 2 seconds
            events = ddply(d,.(event,transp,id,feeder),summarise,startt = min(as.POSIXct(datetime_),na.rm=TRUE),
                                                endt = max(as.POSIXct(datetime_),na.rm=TRUE))
            if (nrow(nulls)>0) events = rbind(events, data.frame(event=NA,feeder=nulls$feeder,transp = nulls$transp,startt = NA,endt=NA,id=nulls$id))
      
            events = merge(events,IDs,all.x=TRUE,by.x='transp',by.y='transponder')
            navars = setdiff(keepvars,names(events))
            events[,navars]=NA
            writeload(events[,keepvars],con=con,db='SFatWESTERHOLZ',tb = 'ALL_EVENTS',ignore=TRUE)
          }
      }
    }
  }
}

#' retrieve all bird ringnumbers/cb code/transponder/genetic sex combinations
#' from the database
#' 
#' query  BTatWESTERHOLZ.ADULTS, BTatWESTERHOLZ.CHICKS,
#' FIELD_BTatWESTERHOLZ.ADULTS, and SNBatWESTERHOLZ2.test_transponders (a view
#' that trawls all authors tables) to find all existing
#' ringnumber/transponder/colourbandcombo/genetic sex combinations.
#' 
#' @return a dataframe with variables birdID,transponder,cb,sex. each distinct
#'   combination has one line, so if a bird gets retraspondered it will appear
#'   more than once in this dataframe. the fieldteam gets birdID 'fieldteam'.
birdIDs = function() {
  con = dbcon(database = 'SNBatWESTERHOLZ2',user='snb',password = 'cs')
  on.exit(closeCon(con))
  
  return(dbq(con,snipFetch(con=con,ID=59)))
  
}