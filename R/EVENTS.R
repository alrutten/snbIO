
assignEvents = function(d,maxt = 2.1) {
  #d must have datetime_, r_pk, transp, box, bout_length, LB
  # returns d plus boutID
  # RUN PER BOX
  if (length(unique(d$box))>1) stop('run this per box (use lapply/ddply')
  d = d[order(d$r_pk),]
  keepvars = c(names(d),'boutID','filltransp')
  d$datetime_ = as.POSIXct(d$datetime_,tz='UTC')
  d$timePrev=c(d$datetime_[1],d$datetime_[-nrow(d)])
  d$timenxt=c(d$datetime_[-1],d$datetime_[nrow(d)])
  d$blPrev=c(d$bout_length[1],d$bout_length[-nrow(d)])
  timeout    = as.POSIXct(d$timePrev,tz='UTC')<(as.POSIXct(d$datetime_,tz='UTC')-maxt)
  
  
  
  if(is.na(d$transp[1])) d$transp[1]='foo'
  d$transp[timeout] = 'ta' #make sure transp does not spill over into next visit
  d$filltransp=na.locf(d$transp,na.rm=FALSE)
  d$filltransp[d$filltransp=='ta'] = NA
  d$filltransp=na.locf(d$filltransp,na.rm=FALSE,fromLast = TRUE)
  d$transp[d$transp%in%c('foo','ta')] = NA
  
  
  d$transpPrev=c('foo',d$filltransp[-nrow(d)])
  
  
  # bout start/stop criteria: different transponder, drop in boutlength, increase in boutlength, reading over maxt seconds after previous
  
  othertransp = d$filltransp!=d$transpPrev&(d$filltransp!='foo'&d$transpPrev!='foo')
  bldrop     = d$bout_length<d$blPrev-20
  blinc      = d$bout_length>d$blPrev+20
  
  d$boutID = 0
  d$boutID[timeout|bldrop|blinc|othertransp] = 1
  d$boutID[1] = 1
  d$boutID = cumsum(d$boutID)
  
  # reset transp to NA when there is not actually a transponder read
  filltransp = ddply(d,.(boutID),summarise, filltransp = unique(transp[!is.na(transp)]))
  d$filltransp = NULL
  d = merge(d,filltransp,all.x=TRUE)
  
  
  
  return(d[order(d$r_pk),keepvars])
}


extractEvents = function(d, min_t = 2.1) {
  
  newLB = c('10','12','02')
  d$LB = match(d$LB,newLB)
  d$LB = d$LB-2
  #d$LB[is.na(d$LB)] = 0
  d=d[order(d$r_pk),]
  events=ddply(d[d$boutID>0,],.(box,id,boutID),summarise, transp=filltransp[1],
               startt=min(datetime_),
               endt=max(datetime_),
               duration = as.numeric(difftime(max(datetime_),min(datetime_), tz='UTC', units = 'secs')),
               nline_beforetransp =which(!is.na(transp))[1],
               first_LB=LB[which(!is.na(LB))][1],
               last_LB = tail(c(NA,LB[which(!is.na(LB))]),1),
               anyLB12 = any(LB==0,na.rm=TRUE),
               n_lines=length(LB),
               mean_bl = mean(bout_length),
               sum_bl=as.numeric(sum(bout_length)),
               ratio_l_bl = n_lines/sum_bl)
  # discard events where there's no transit and no transponder
  events=events[!(!events$anyLB12&is.na(events$transp)),] 
  
  events$LBdir = events$last_LB - events$first_LB
  events$LBdir[is.na(events$LBdir)] = 0
  #do previouses per transponder
  events = events[order(events$transp,events$startt),]
  prevevents = ddply(events,.(transp),  function(x) rbind(x[1,], x[-nrow(x),]))		
  names(prevevents) = paste0('prev_',names(events)) 
  events = cbind(events,prevevents[,c('prev_n_lines','prev_sum_bl','prev_startt','prev_transp','prev_LBdir')])
  events$ratio_l_prevl = events$n_lines/events$prev_n_lines
  return(events[order(events$startt),])
}  

birdIDs = function() {
  con = dbcon(database = 'SNBatWESTERHOLZ2',user='snb',password = 'cs')
  on.exit(dbDisconnect(con))
  
  d = sql(con,"SELECT x.*, s.sex from 
          (SELECT distinct ID birdID, transponder, FUNCTIONS.combo(UL,LL,UR,LR) cb from BTatWESTERHOLZ.ADULTS where transponder IS NOT NULL
          UNION SELECT distinct c.ID birdID, c.transponder, FUNCTIONS.combo(a.UL,a.LL,a.UR,a.LR) cb from BTatWESTERHOLZ.CHICKS c 
          LEFT JOIN BTatWESTERHOLZ.ADULTS a on c.ID = a.ID where c.transponder IS NOT NULL
          UNION SELECT distinct ID birdID, transponder, FUNCTIONS.combo(UL,LL,UR,LR) cb from FIELD_BTatWESTERHOLZ.ADULTS where transponder IS NOT NULL
          UNION SELECT 'fieldteam' birdID, transponder, 'fieldteam' cb from SNBatWESTERHOLZ2.test_transponders 
          )x
          left join BTatWESTERHOLZ.SEX s on x.birdID = s.ID
          
          order by transponder
          "
  )
  
  return(d)
}
loadEvents = function(year_=substring(Sys.Date(),1,4)) {

  con = dbcon(database = 'SNBatWESTERHOLZ2',user='snb',password = 'cs')
  on.exit(dbDisconnect(con))
  IDs = birdIDs()
  load("/home/snb/snb_io/data/ct.Rdata")
  vars = paste0("c('",paste(strsplit(gsub(' ','',paste(ct@data@formula$input)),'\\+')[[2]],collapse='\',\''),"')")
  keepvars = names(sql(con,'select * from BETA_Events2014 limit 1'))
  #fids = sql(con,'select id from file_status where year_ = 2013 and box between 100 and 201 order by id desc')$id
  foo = try({fids = sql(con, paste0("select f.id from file_status f 
                                    LEFT JOIN (select distinct id from BETA_Events",year_,") b on f.id = b.id 
                                    where b.id IS NULL and year_ = ",year_," order by id desc"))$id},silent=TRUE)
  if ((!class(foo)=='try-error')&length(fids)>0) {
    for (i in 1:length(fids)) {
      d = sql(con, paste0("select * from RAW_",year_," where id = ",fids[i]))
      if (nrow(d)>0) {
        nulls = d[is.na(d$datetime_),]
        if (nrow(nulls) > 0) {
          nulls = ddply(nulls,.(box,datetime_,transp,id),summarise, count = -1)
          nulls = merge(nulls,IDs, by.x='transp',by.y='transponder',all.x=TRUE,incomparables = NA) 
          nulls[,setdiff(keepvars, names(nulls))] = NA
          dbWriteTable(con,paste0('BETA_Events',year_),nulls[,keepvars],row.names=FALSE,append=TRUE)
        }
        d = d[!is.na(d$datetime_),]
        if (nrow(d)>0) {
          dd = assignEvents(d)
          dd = extractEvents(dd)
        if (nrow(dd)>0) {
          dd$direction_raw = round(predict(ct,newdata = dd[,eval(parse(text=vars))]),2)
          dd$direction = round(dd$direction_raw,0)
          dd$direction[abs(dd$direction_raw-dd$direction)>0.1] = 0
                 
          dd = merge(dd,IDs, by.x='transp',by.y='transponder',all.x=TRUE,incomparables = NA) 
        
          dbWriteTable(con,paste0('BETA_Events',year_),dd[,keepvars],row.names=FALSE,append=TRUE)
        }
      }
    }
  }
  }
}

