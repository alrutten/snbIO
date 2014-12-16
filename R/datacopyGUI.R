
  
  checkSD = function(con, SDsn) {
     
    if (missing(con)) {
      con = dbcon(database = 'SNBatWESTERHOLZ2',user='snb',password = 'cs')
      on.exit(closeCon(con))
    }
    check=dbq(con,paste("SELECT sum(SD_K_status > 0) n_bad, 
                        sum(SD_K_status>0)/count(SD_K_status) ratio, count(SD_K_Status) total 
                        from (SELECT SD_K_status 
                               from TECHatWESTERHOLZ.technical_details t 
                               where SD_K_Status IS NOT NULL
                               and SD_ID=",shQuote(SDsn),
                              " UNION ALL SELECT SD_K_status from SFatWESTERHOLZ.file_status
                               where SD_K_Status IS NOT NULL
                               and SD_ID=",shQuote(SDsn),") t",sep="") 
    )
    msg = paste("Card ID ",SDsn,"\ncard used",check$total,"times previously")
    return(ifelse(check$ratio>0.27 & check$total>4,paste(msg,"\nDESTROY THIS CARD"),msg))	
    
  }  
  
  getSD = function(logger = 'snb',dtime,bx,filename="BOX001.TXT" ) { #logger comes from getSDGui() and can only be 'snb' or 'sf'
     if (!logger%in%c('snb','sf')) return('woops')
    if(logger=='snb') {
      con = dbcon(database = 'SNBatWESTERHOLZ2',user='snb',password = 'cs')
      rawDir = snbDir
      prev = dbq(con,paste("SELECT max(date_time_field) md 
                             from TECHatWESTERHOLZ.technical_details 
                              where box = ",bx," 
                              and SD_K_Status IS NOT NULL and date_time_field<",shQuote(gsub("\\.","-",dtime))))$md
      
    } else {
      con = dbcon(database = 'SFatWESTERHOLZ',user='snb',password = 'cs')
      rawDir = sfDir
      prev = dbq(con,paste("SELECT max(date_) md 
                             from SFatWESTERHOLZ.file_status
                              where feeder = ",bx," 
                              and SD_K_Status IS NOT NULL and date_<",shQuote(gsub("\\.","-",dtime))))$md
      
    }
    on.exit(closeCon(con))
    outname = paste(substr(dtime,1,4),substr(dtime,1,10),bx,"box001.txt",sep="/")
    outfile = paste(rawDir,outname,sep='/')
    dtime=gsub("\\.","-",dtime)
    
    drv=findRemovable()
       
    if (!(file.exists(outfile))) { 
      dir.create(dirname(outfile),showWarnings=FALSE,recursive=TRUE)
# COPY FILE AND GET METADATA            
      if (length(drv)>0)  {   # SD card gets detected
        filename=paste(drv[1],"\\",filename,sep="")
        Sdsn=getSDserial(drv)
        out=checkSD(con=con,SDsn=Sdsn)
        if (file.exists(filename)){        # file present
          insize=file.info(filename)$size   # this information is no longer accessible after trying file.copy because of i/o errors    
          file.copy(filename,outfile,overwrite=FALSE) #sadly, file.copy() returns TRUE for partial copies
          
          #catch network connectivity problem during copying (file.copy returns TRUE but outfile does not exist
          if (is.na(file.info(outfile)$size)) return('network problem: \nremove card\nput back into reader\nclick "start upload" again') else 
            metadata = data.frame(path = outname,
                                  year_ = substr(dtime,1,4),
                                  date_ = paste(dtime),
                                  date_time_field = paste(dtime),
                                  date_prev = paste(prev),
                                  filesize = file.info(outfile)$size/1000,
                                  box = bx,
                                  feeder=bx,
                                  upload_status = 0,
                                  boxversion = 0,
                                  SD_K_Status = ifelse(insize == file.info(outfile)$size,0,5),
                                  SD_K_Aktion = ifelse(insize == file.info(outfile)$size,0,3),
                                  SD_ID = Sdsn,
                                  out =ifelse(insize == file.info(outfile)$size,
                                              paste("file copied without problems\n",out,'\n'),
                                              paste("card damaged, file partly recovered\n",out,"\n")),
                                  stringsAsFactors=FALSE)
        } else #no file on card
          metadata = data.frame(year_ = substr(dtime,1,4),
                                date_ = paste(dtime),
                                date_time_field = paste(dtime),
                                date_prev = paste(prev),
                                box = bx,
                                feeder=bx,
                                SD_K_Status =3,
                                SD_ID = Sdsn,
                                out =paste("No file on card\n",out,"\n"),
                                stringsAsFactors=FALSE)
          
      } else #card not detected
        metadata = data.frame(year_ = substr(dtime,1,4),
                              date_ = paste(dtime),
                              date_time_field = paste(dtime),
                              date_prev = paste(prev),
                              box = bx,
                              feeder = bx,
                              SD_K_Status =1,
                              out =paste("CAUTION: card not detected"),
                              stringsAsFactors=FALSE)

# WRITE METADATA TO APPROPRIATE TABLES
            if (logger=='snb') {
              snbvars = dbq(con, "SHOW COLUMNS FROM file_status FROM SNBatWESTERHOLZ2")$Field
              snbna = setdiff(snbvars,names(metadata))
              metadata[,snbna] = NA
              if (!is.na(metadata$path))  writeload(metadata[,snbvars],con=con,db='SNBatWESTERHOLZ2',tb = 'file_status')
              
              dbq(con,"USE TECHatWESTERHOLZ")
             
              techvars =  dbq(con, "SHOW COLUMNS FROM technical_details FROM TECHatWESTERHOLZ")$Field
              techna = setdiff(techvars,names(metadata))
              metadata[,techna] = NA
              writeload(metadata[,techvars],con=con,db='TECHatWESTERHOLZ',tb = 'technical_details')
             } else { #logger = 'sf'
              sfvars = dbq(con, "SHOW COLUMNS FROM file_status FROM SFatWESTERHOLZ")$Field
              sfna = setdiff(sfvars,names(metadata))
              metadata[,sfna] = NA
              writeload(metadata[,sfvars],con=con,db='SFatWESTERHOLZ',tb = 'file_status')
              }
    } else #file already exists!
      return(paste('ERROR: destination file ',outfile,' already exists. File not copied!',sep=""))
   
    cat(metadata$out[1])
    return(metadata$out[1])
  }
  
  
  
  getHandheld = function(rawDir =snbDir,dtime, filename="box001.txt") {
    
    con = dbcon(database = 'SNBatWESTERHOLZ2',user='snb',password = 'cs')
    
    on.exit(closeCon(con))
        
    drv=findRemovable()
    path=paste(drv[1],"\\",dtime,sep="")
    
    flist=list.files(path, recursive = TRUE, full.names = TRUE) 
    boxlist=basename(dirname(flist))
    errlist=''
    if (length(flist)==0) return("nothing to upload") else
    {
      for (i in (1:length(flist))) {
        outdir=paste(rawDir,substr(dtime,1,4),paste(substr(dtime,1,10),"CF",sep=""),tolower(boxlist[i]),sep="/")
        mtime=file.info(flist[i])$mtime
        prev = dbq(con,paste("SELECT max(date_time_field) md from TECHatWESTERHOLZ.technical_details where box = ",tolower(boxlist[i])," and SD_K_Status IS NOT NULL and date_time_field<",shQuote(mtime)))$md
        dir.create(outdir,recursive=TRUE)
        if (!file.exists(paste(outdir,filename,sep="/"))) {
          file.copy(flist[i],outdir,overwrite=FALSE)
          dbq(con,paste("INSERT INTO TECHatWESTERHOLZ.technical_details (date_time_field,box,SD_K_Status, SD_K_Aktion) VALUES('",mtime,"','",boxlist[i],"','0','0')",sep=""))
          dbq(con,paste0("INSERT INTO SNBatWESTERHOLZ2.file_status (path,year_,date_,date_prev,filesize,box,upload_status,boxversion) VALUES('",paste(gsub(snbDir,'',outdir),filename,sep='/'),"',",substr(dtime,1,4),
                         ",'",mtime,"',",shQuote(prev),",",round(file.info(flist[i])$size/1000),",",boxlist[i],",0,0)"))
          
        } else errlist=paste(errlist,boxlist[i],sep=',')
      }
      return(paste(i,' files processed',ifelse(errlist!='',paste('; NB: destination files for box(es) ',errlist,' already existed. THOSE FILES NOT COPIED',sep=""),''),sep=""))
    }
  }	
  
  
  
  
  getSDGui=function(dte=format(Sys.time()-86400,"%Y.%m.%d %H:%M:%S"),logger='snb') {
    if (!logger%in%c('snb','sf')) return('woops')
    sdw=tktoplevel()
    tktitle(sdw) = paste(logger,"SD card copying")
    boxsel=tclVar("666")
    datesel=tclVar(dte)
    
   
    boxentry  = tkentry(sdw, textvariable=boxsel)
    boxlabel  = tklabel(sdw,text="box/feeder:")
    dateentry = tkentry(sdw,textvariable=datesel)
    datelabel = tklabel(sdw,text="datetime ")
    
    OnOk = function(bx=tclvalue(boxsel),dt_=tclvalue(datesel)){
      # if (missing(con)) con=snbcon()
      cardThere = findRemovable()
      if (length(cardThere)>1) tkmessageBox(message = "remove all USB storage devices that are not the SD-card you want to copy. T")
      if(length(cardThere)==0) foo = tkmessageBox(message="no card detected. Proceed?\n (yes= after inserting card (or if it's already there)\n no= cancel upload",type='yesno') else 
        foo = tclVar('yes')
      if (tclvalue(foo)=='yes'){
        txt = getSD(logger=logger,dtime=dt_,bx=bx)
        tkmessageBox(message=paste(txt))
      } else tkmessageBox(message="upload canceled")
    }
    
    OK.but <-tkbutton(sdw,text="START UPLOAD",command=function() OnOk())
    Cancel.But =tkbutton(sdw,text="CANCEL/DONE",command=function() tkdestroy(sdw))
    
    tkgrid(boxlabel,boxentry,datelabel,dateentry)
    tkgrid(tklabel(sdw,text="    "))
    tkgrid(OK.but,Cancel.But) 
    tkgrid.configure(OK.but,column=1)
    tkgrid.configure(Cancel.But,column=2)
  }
  
  
  getHandheldGui=function(dte=format(Sys.Date()-1,"%Y.%m.%d")) {
    hhw=tktoplevel()
    
    datesel=tclVar(dte)
    dateentry = tkentry(hhw,textvariable=datesel)
    datelabel=tklabel(hhw,text="date ")
    
    OnOk=function(){
      cardThere=findRemovable()
      if(length(cardThere)==0) foo=tkmessageBox(message="no card detected. Proceed?\n (no= cancel upload)",type='yesno') else foo=tclVar('yes')
      if (tclvalue(foo)=='yes'){
        txt=getHandheld(dtime=tclvalue(datesel))
        tkmessageBox(message=paste(txt))
      } else tkmessageBox(message="upload canceled")
    }
    OK.but <-tkbutton(hhw,text="START UPLOAD",command=function() OnOk())
    Cancel.But =tkbutton(hhw,text="CANCEL/DONE",command=function() tkdestroy(hhw))
    tkgrid(datelabel,dateentry)
    tkgrid(tklabel(hhw,text="    "))
    tkgrid(OK.but,Cancel.But) 
    tkgrid.configure(OK.but,column=1)
    tkgrid.configure(Cancel.But,column=2)
  }
  
  snbSDGui = function() {
    getSDGui()
  }
  
  sfSDGui = function() {
    getSDGui(logger='sf')
  }
  
  findRemovable = function() { #uses wmic to list all removable drives (fsutil needs admin power)
    
    drvLetter =paste(unique(gsub("[^A-Z]","",(system('wmic logicaldisk get caption',intern=TRUE)))),":",sep="")
    drvUsed=as.numeric(gsub("\\D","",system('wmic logicaldisk get freespace',intern=TRUE))[-1])
    drvType=as.numeric(gsub("\\D","",system('wmic logicaldisk get drivetype',intern=TRUE))[-1])
    
    drv=as.data.frame(cbind(drvLetter,drvUsed,drvType),stringsAsFactors=FALSE)
    
    return(drv$drvLetter[which(!is.na(drv$drvUsed)&drv$drvType==2)])
    
    
  }
  matchGregexpr = function(pattern, charv, ...) 
  {
    require(plyr)
    if (storage.mode(charv) != "character") 
      stop("please feed me a charactervector")
    allmatch = gregexpr(pattern, charv, ...)
    convertMatches = c()
    for (i in 1:length(allmatch)) {
      thisLine <- allmatch[[i]]
      if (thisLine[1] != -1) {
        convertMatches <- rbind(convertMatches, data.frame(element = i, 
                                                           start = thisLine, end = thisLine + attr(thisLine, 
                                                                                                   "match.length") - 1))
      }
    }
    if (is.null(convertMatches)) 
      return(NULL)
    convertMatches = adply(convertMatches, 1, function(row) {
      row$match = substr(charv[row$element], row$start, row$end)
      return(as.data.frame(row))
    })
    convertMatches$match <- as.character(convertMatches$match)
    return(convertMatches)
  }
  getSDserial = function(SDdir) { 
    foo=shell(paste("dir ",SDdir,sep=""),intern=TRUE)
    foo=gsub(".*is\ ","",foo[2])
    foo = matchGregexpr('.{4}-.{4}',foo)$match
    return(foo)
  }
  SDmaintenance = function() {
    
    sdw=tktoplevel()
    rb1 <- tkradiobutton(sdw)
    rb2 <- tkradiobutton(sdw)
    rbValue <- tclVar("retired")
    tkconfigure(rb1,variable=rbValue,value="retired")
    tkconfigure(rb2,variable=rbValue,value="glued")
    
    onOk = function() {
      con = dbcon(database='TECHatWESTERHOLZ',user='snb',password='cs')
      drv = findRemovable()
      if (length(drv)>0) {
        SDid = getSDserial(drv)
        rbVal = as.character(tclvalue(rbValue))
        dbq(con,paste('INSERT INTO TECHatWESTERHOLZ.SD_maintenance (SDid,action) VALUES (',shQuote(SDid),',',shQuote(rbVal),')'))
        tkmessageBox(message = paste(SDid,rbVal))
        closeCon(con)
      } else tkmessageBox(message='card not detected')
    }
    OK.but <-tkbutton(sdw,text="GO",command=function() onOk())
    Cancel.But =tkbutton(sdw,text="CANCEL/DONE",command=function() tkdestroy(sdw))
    tkgrid(tklabel(sdw,text=''),tklabel(sdw, text = "this SD card has been:"))
    tkgrid(rb1,tklabel(sdw,text='retired'),sticky='w')
    tkgrid(rb2,tklabel(sdw,text='glued'),sticky='w')
    tkgrid(OK.but,Cancel.But)
    
    
  }
