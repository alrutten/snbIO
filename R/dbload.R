
load.techxls = function(path="/ds/raw_data_kemp/FIELD/Westerholz/SNB/TECH/Details_csv/details.xlsx" ){
  
  con      = dbConnect(dbDriver("MySQL"), user = "snb", password = "cs", host = "scidb.orn.mpg.de", dbname = 'TECHatWESTERHOLZ')  
  on.exit(mysqlCloseConnection(con))
  
  details   = readWorksheetFromFile(path,header=TRUE,sheet=1)
  details   = details[!is.na(details$box),]
  
  
  dbdetails = dbReadTable(con,"technical_details")
  dbdetails$thing = paste(dbdetails$box,dbdetails$date_time_field)
  details$thing =   paste(details$box,details$date_time_field)
  new = setdiff(details$thing,dbdetails$thing)
  
  details = details[which(details$thing %in% new),]
  
  
  
  if (nrow(details)>0) {
    details$Batt=gsub(",","\\.",details$Batt)
    details$Batt_gew=gsub(",","\\.",details$Batt_gew)
    details$SD_ID=NA
    details$SD_K_Reperatur=NA
    details$thing = NULL
    foo = dbWriteTable(con, "technical_details", details,append=TRUE ,row.names=FALSE)
    if (foo) return(paste(nrow(details),"records loaded into TECHatWESTERHOLZ.technical_details")) else
      return(paste('error loading',nrow(details),'into TECHatWESTERHOLZ.technical_details'))
    
  } else return('no new records found') 
  
  
}


readRaw = function(snbdir = snbDir, path){
  
  fname = gsub('//','/',paste(snbdir,path,sep='/'))
  
  if (!file.exists(fname)) {
    fname = list.files(dirname(fname),full.names=TRUE,ignore.case=TRUE)[1]
    
    if (!file.exists(fname)) return(FALSE)
  }
  
  d = read.table(file=fname,colClasses="character",row.names=NULL,col.names="V1",comment.char="",blank.lines.skip=TRUE)
  
  if (nrow(d)>0){  
    if (is.clean(d$V1)) {
      status=1
      bouts = data.frame(full=rle(d$V1)[[2]], bout_length = rle(d$V1)[[1]],stringsAsFactors=FALSE)	}	 else {
        
        status=2
        bouts=readGrexp(d, path=fname)
        
      }
    bouts$status=status
    bouts$rawchar=sum(nchar(d$V1))		
  } else bouts=data.frame(full=format(file.info(path)$mtime,"%d%m%y%H%M%S"), bout_length=0, status=17, rawchar=0,stringsAsFactor = FALSE)
  return(bouts)
}



loadDbase = function(year=as.numeric(format(Sys.Date(),"%Y"))){
  
  counter = 0
  failure = 0
  
  con      = dbConnect(dbDriver("MySQL"), user = "snb", password = "cs", host = "scidb.orn.mpg.de", dbname = 'SNBatWESTERHOLZ2')  
  on.exit(mysqlCloseConnection(con))
  
  flist = sql(con,paste0("SELECT * FROM file_status f Where upload_status=0 and year_=",year," limit 1"))
  blacklist = '-1'  
  # cat('loading.')
  
  
  while(nrow(flist)>0) {
    
    indb=sql(con,paste0("SELECT id from RAW_",year, " where id = ",flist$id," limit 1"))   # prevent double uploading
    
    if (nrow(indb)>0) stop(paste(indb$id,"is already uploaded"))
    
    
    
    foo = try(loadSingle(con=con, id=flist$id, check = FALSE))
    
    if (class(foo) == 'try-error') {
      blacklist = c(blacklist,flist$id)
      failure = failure+1
    }
    counter = counter+1
    flist    = sql(con,paste0("SELECT * FROM file_status f 
                              Where upload_status=0 and year_=",year,
                              " and id not in (",paste(blacklist,collapse=','),") limit 1")) 
    
  }
  
  return(data.frame(tried = counter, failed = failure)) 
  
}	

loadSingle=function(con,id,check = TRUE,...) 
{ 
  dc = FALSE
  if (missing(con)) {
    con      = dbConnect(dbDriver("MySQL"), user = "snb", password = "cs", host = "scidb.orn.mpg.de", dbname = 'SNBatWESTERHOLZ2')  
    on.exit(mysqlCloseConnection(con))  
  }
  focal  = sql(con,paste("select * from SNBatWESTERHOLZ2.file_status where id=",id,sep=""))
  prev   = sql(con,paste("SELECT max(date_) md from SNBatWESTERHOLZ2.file_status where box =",focal$box," and path not LIKE('%CF%') and date_ <",shQuote(focal$date_)))$md
  year   = focal[1,"year_"]
  if (is.na(prev)) prev = as.POSIXct(focal$date_)-get('SD_interval', envir = .snb) *86400
  
  if (check) {
    indb=sql(con,paste("select * from SNBatWESTERHOLZ2.RAW_",year," where id=",id," limit 1",sep=""))
    if (nrow(indb)>0) dropSingle(con=con,id=id)
  }
  # read & extract data
  
  dframe=extractVars(readRaw(path=focal[1,"path"]))
  
  
  rawchar=dframe$rawchar[1] #THIS HAS TO GO FROM read as well
  dframe$rawchar=NULL
  focal$upload_status = dframe$status[1]
  # CHECKS
{
  
  # valid/non-NA datetimes
  days    = format(seq.Date(from=as.Date(prev)-1,to=as.Date(focal$date_)+1,by='day'),format='%m-%d')
  dtvalid = round(100*(sum(!is.na(dframe$datetime_)&dframe$bout_length!=-1&format(dframe$datetime_,format = '%m-%d')%in%days))/nrow(dframe),2)
  na_dt   = dtvalid<100
  
  # 2012 year issue (boxes think it's 2008)
  if (year==2012) dframe$datetime_ = as.POSIXct(gsub('2008','2012',dframe$datetime_))
  # wrong year
  yrvalid = round(100*sum(as.numeric(format(dframe$datetime_,"%Y"))%in% c((year-1):(year+1)))/nrow(dframe),2)
  wy      = yrvalid<100
  
  # number of timejumps
  n_timejumps = sum(as.numeric(c(dframe$datetime_[-1], dframe$datetime_[length(as.vector(dframe$datetime_))])) <
                      as.numeric(dframe$datetime_), na.rm = TRUE)
  
  focal$year_valid	   = yrvalid
  focal$datetime_valid  = dtvalid
  focal$n_timejumps     = n_timejumps  
  
  if (!na_dt) {  # ONLY try to correct year if there is nothing fishy with any datetime_	 
    if (wy) #wrong year
    {
      dframe$datetime_ = as.POSIXct(strptime(ifelse(as.numeric(format(dframe$datetime_,"%m"))<= 
                                                      as.numeric(substring(focal[1,"date_"],6,7)),
                                                    paste(substring(focal[1,"date_"],1,4),
                                                          format(dframe$datetime_,"%m%d%H%M%S"),sep=""),
                                                    paste(as.numeric(substring(focal[1,"date_"],1,4))-1,
                                                          format(dframe$datetime_,"%m%d%H%M%S"),sep="")),
                                             format='%Y%m%d%H%M%S'),tz="GMT")
    }
  }
}


if (nrow(dframe)>0)
{
  dframe=dframe[,-c(3,4,6,10)]
  dframe$id=focal[1,"id"]
  dframe$box=focal[1,"box"]
  
  lastBat = tail(dframe[which(!is.na(dframe$bv)),],1)
  if (nrow(lastBat)==0) lastBat = data.frame(datetime_ = -1,bv = 'BV-1.0V')
  
  
  dframe$r_pk=NA
  
  
  
  if (nrow(dframe)>1)	{
    foo =  writeload(d=dframe[,c('box','datetime_','PIR','LB','id','transp','bv','bout_length','r_pk')], con=con, db='SNBatWESTERHOLZ2', tb = paste0("RAW_",focal$year_[1]))
    if (is.null(foo)) sql(con,paste("Update SNBatWESTERHOLZ2.file_status SET upload_status=",focal[1,"upload_status"], 
                                    ", datetime_valid=",focal[1,"datetime_valid"],
                                    ", year_valid=",focal[1,"year_valid"],
                                    ", n_timejumps=",focal[1,"n_timejumps"],
                                    ", dt_loaded='",Sys.time(),
                                    "', lastBatdt='",lastBat[1,"datetime_"],
                                    "', lastRT=-1
                                    , lastBV=", substring(lastBat[1,"bv"],3,6),
                                    ", dt_last='", max(dframe$datetime_,na.rm = TRUE), 
                                    "' WHERE id =",focal[1,"id"],sep=""))  else  sql(con, paste("Update SNBatWESTERHOLZ2.file_status SET upload_status = -1 WHERE id=", focal[1,"id"]))	
    
  }	
  
} else 
{
  sql(con, paste("Update SNBatWESTERHOLZ2.file_status SET upload_status = -1 WHERE id=", focal[1,"id"]))	
  sql(con, paste('Update SNBatWESTERHOLZ2.file_status SET remarks ="upload unsuccessful critical errors" WHERE id=',focal[1,"id"]))
}

gc()
}

readGrexp = function(d=NULL,path=NA, min.match=0.1){
  if (!exists('d')) d = read.table(file=path,colClasses="character",row.names=NULL,col.names="V1",comment.char="",blank.lines.skip=TRUE)
  
  yrbroken=FALSE
  d$match = grepl(paste("^",regexpString("anyDate"),sep=""),d$V1,perl=TRUE)			#does the record start with a valid date?
  if (nrow(d)==sum(!d$match)){ 							# NOTHING MAKES SENSE 
    bouts=data.frame(full=format(file.info(path)$mtime-86400,"%d%m%y%H%M%S"),bout_length=-1) } else { 
      
      d$fault=ifelse((nchar(d$V1)>32|(nchar(d$V1)>23 & grepl("V",d$V1)==TRUE)),1,0) # isn't the record too long?
      if (any(is.na(as.numeric(substring(d$V1,0,12))))) yrbroken=TRUE  # are there letters where there should be none?
      str = ifelse(!yrbroken,paste("(\\d\\d)(([01]\\d)|(2[0-4]))([0-5]\\d)([0-5]\\d)"),paste("(.{2})(([01]\\d)|(2[0-4]))([0-5]\\d)([0-5]\\d)"))
      
      
      d$nd=ifelse(d$match,substring(d$V1,1,4),ifelse(grepl(paste(regexpString("ddmm")),substring(d$V1,1,4),perl=TRUE),substring(d$V1,1,4),0))	#find valid daymonths to seed regexps with
      nds = unique(d$nd[d$nd!=0])
      
      
      
      if (d$nd[1]==0) {    # if first record is not valid
        d$nd[1] =yesterday(nds[1])
        nds=c(d$nd[1],nds)
      }
      if (exists('nds')){
        
        d$nxt=(d$match==FALSE)*2+(d$fault==1)		# split runs of wrongs into runs of specific wrongs
        
        
        #find start and end points of runs of most likely clean, and most likely faulty records
        
        d$st=ifelse(d$nxt!=c(d$nxt[1],d$nxt[-nrow(d)]),1,0) #find the start and end of runs of wrong/right records
        d$end=ifelse(d$nxt!=c(d$nxt[-1],d$nxt[nrow(d)]),1,0)
        d$st[which(d$st==1)]=cumsum(d$st[which(d$st==1)])+1
        d$st[1]=1
        d$end[which(d$end==1)]=cumsum(d$end[which(d$end==1)])
        d$end[nrow(d)]=max(d$st)			#fix last record
        
        # find previous and next valid daymonths for runs of fault
        
        lastndpos = 1
        bouts="0"
        for (j in 1:max(d$st)) {					#cycle through all wrong/right runs
          cut = d[which(d$st==j):which(d$end==j),]  #extract current bit of data
          if (all(cut$fault==0))  {					# all records are valid, or at least of valid length
            bouts = append(bouts,cut$V1)
          } else {	
            
            dm=ifelse(cut$nd[1]!=0,cut$nd[1],ifelse(lastndpos==1,nds[lastndpos],nds[lastndpos-1]))
            mnd=max(which(cut$nd!=0),na.rm=T)
            ldm = ifelse(mnd>-Inf,ifelse(which(nds==cut$nd[mnd])==length(nds),yesterday(yesterday(dm)),nds[which(nds==cut$nd[mnd])+1]),yesterday(yesterday(dm)))
            
            
            
            cut = gsub("F0001","foo",paste(c(bouts[length(bouts)],d[(which(d$st==j)):which(d$end==j),"V1"]),collapse="",sep=""))	#add previous record to cut, match will be excluded
            cut=gsub("\\n","",cut)		# because of weirdness in some files
            tc=nchar(cut)	
            
            while (yesterday(dm)!=ldm&nchar(cut)>12) {
              rexp=paste("(",dm,")",str,sep="")	#add last valid date to regexp
              
              g = gregexpr(rexp,cut)
              getPos =lapply(g, function(x) data.frame(Start = x, Length = attributes(x)$match.length ))
              o = do.call(rbind, getPos)
              o$diff= c(o$Start[-1],tc)-o$Start
              if ((tc>500)&(any(o$diff>60))&any(o$diff<60))  { o=o[1:max(which(o$diff<60)),]
                                                               tc=o$Start[nrow(o)]+12}
              if (sum(o[2])/tc >= min.match) {		# write matches to bouts
                f=substring(cut, o$Start,ifelse(c(o$Start[-1]-1,nchar(cut))-o$Start<31,c(o$Start[-1]-1,nchar(cut)),o$Start+30))
                f = gsub("foo","F0001",f, fixed=TRUE) 
                f = gsub("[a-z]","0",f)	
                if (f[1]==bouts[length(bouts)]) f=f[-1] #exclude first match (was added to control matching)
                if (length(f)>0 & substring(f[1],0,nchar(bouts[length(bouts)]))==bouts[length(bouts)]) bouts=bouts[-length(bouts)] #something was probably missing from the previous record
                bouts = append(bouts,f)
                cut = substring(cut,o$Start[nrow(o)]+12,nchar(cut))
                tc=nchar(cut)	
              }
              dm=tomorrow(dm)
              lastndpos=ifelse(any(nds==dm),which(nds==dm),lastndpos)
            }
          }
        }	
        rl=rle(bouts[which(nchar(bouts)>12)])	
        bouts=data.frame(full=rl[2],bout_length=rl[1])[-1,]		} else bouts=data.frame(full=format(file.info(path)$mtime-86400,"%d%m%y%H%M%S"),bout_length=1)
    }
  return(bouts)
}


dropSingle= function(con, id=id)
{ if (missing(con)) {con = snbcon(credentials = get('cred',envir=.snb))
                     dc = TRUE
}
year=sql(con,paste("select year_ from file_status where id=",id,sep=""))
sql(con,paste("delete from RAW_",year," where id=",id,sep=""))
sql(con,paste("delete from RAW_",(year-1)," where id=",id,sep=""))
sql(con,paste("update file_status set upload_status=0 where id=", id, sep=""))
if (dc) on.exit(mysqlCloseConnection(con))
}  


extractVars <- function(d)  {
  
  dfr=data.frame(id = NA, box=NA, full = d[,1],status=d[,3],
                 datetime_=as.POSIXct(strptime(substring(d[,1],1,12),format = "%d%m%y%H%M%S",tz = 'UTC')), 
                 rest=substring(d[,1],13,nchar(d[,1])), bout_length=d[,2], rawchar=d[,4], 
                 transp=ifelse(nchar(d[,1])==32,substring(d[,1],17,32),NA), 
                 bv=ifelse(nchar(d[,1])==23,substring(d[,1],17,23),NA),
                 pirlb=substring(d[,1],13,16),
                 PIR=substring(d[,1],13,14),
                 LB=substring(d[,1],15,16),
                 stringsAsFactors = FALSE)
  
  if(nrow(dfr)>1 & dfr$status[1]>1) {	
    dfr$datetime_=ifelse(is.na(dfr$datetime_),as.POSIXct(strptime(paste(substring(dfr$full,1,4),"65",substring(dfr$full,7,12),sep=""),format="%d%m%y%H%M%S")),dfr$datetime_) #for if year is not numeric
    dfr$datetime_=backtoPOSIXct(dfr$datetime_)
    tst = c("transp","bv","pirlb")
    for (k in 1:length(tst)) {
      td=regexpr(regexpString(tst[k]), dfr$rest) 
      dfr[,paste(tst[k])]=substring(dfr$rest, td, td + attr(td, "match.length") - 1)
      if ( k!=2) dfr$rest= paste(substring(dfr$rest,1,td-1),substring(dfr$rest,td+attr(td,"match.length"),nchar(dfr$rest)),sep="") else
        dfr$rest=ifelse(td>-1,paste(substring(dfr$rest,1,td-1)),dfr$rest)
    }
    if 	(median(nchar(dfr$pirlb[dfr$pirlb!='']),na.rm=TRUE)<4) { 
      tpir=regexpr("[12]+",dfr$pirlb)
      tlb=regexpr("[34]+",dfr$pirlb)
      dfr$PIR = ifelse(as.numeric(row.names(dfr))<=max(which(nchar(dfr$pirlb)<4),na.rm=TRUE),
                       substring(dfr$pirlb,tpir,tpir+attr(tpir,"match.length")-1),
                       substring(dfr$pirlb,1,2)) 
      dfr$LB = ifelse(as.numeric(row.names(dfr))< max(which(nchar(dfr$pirlb)<4),na.rm=TRUE),
                      substring(dfr$pirlb,tlb,tlb+attr(tlb,"match.length")-1),
                      substring(dfr$pirlb,3,4))} else {
                        dfr$PIR = substring(dfr$pirlb,1,2)
                        dfr$LB = substring(dfr$pirlb,3,4)
                      }
    
    dfr$PIR=ifelse(is.na(as.numeric(dfr$PIR)),NA,dfr$PIR)
    dfr$LB=ifelse(is.na(as.numeric(dfr$LB)),NA,dfr$LB)
    dfr$transp=ifelse(dfr$transp=="",NA,dfr$transp)
    dfr$bv=ifelse(dfr$bv=="",NA,dfr$bv)
  }
  return(dfr)
  
  
  
  
}

