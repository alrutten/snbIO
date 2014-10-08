
  
  checkSD = function(con, SDsn) {
    dc = FALSE
    if (missing(con)) {
      con = dbcon(database = 'SNBatWESTERHOLZ2',user='snb',password = 'cs')
      on.exit(closeCon(con))
    }
    check=dbq(con,paste("SELECT sum(SD_K_status > 0) n_bad, 
                        sum(SD_K_status>0)/count(SD_K_status) ratio, count(SD_K_Status) total 
                        from TECHatWESTERHOLZ.technical_details
                        where SD_K_Status IS NOT NULL
                        and SD_ID=",shQuote(SDsn),sep="") 
    )
    msg = paste("Card ID ",SDsn)
    return(ifelse(check$ratio>0.27 & check$total>4,paste(msg,"\nDESTROY THIS CARD"),paste(msg,"\ncard used",check$total,"times previously")))	
    
  }  
  
  getSD = function(snbDir = '//ds/raw_data_kemp/FIELD/Westerholz/SNB/RAWDATA',dtime,bx,filename="BOX001.TXT" ) {
    
    con = dbcon(database = 'SNBatWESTERHOLZ2',user='snb',password = 'cs')
    
    outname = paste(substr(dtime,1,4),substr(dtime,1,10),bx,"box001.txt",sep="/")
    outfile = paste(snbDir,outname,sep='/')
    dtime=gsub("\\.","-",dtime)
    drv=findRemovable()
    prev = dbq(con,paste("SELECT max(date_time_field) md from TECHatWESTERHOLZ.technical_details where box = ",bx," and SD_K_Status IS NOT NULL and date_time_field<",shQuote(dtime)))$md
    
    
    if (!(file.exists(outfile))) { 
      dir.create(dirname(outfile),showWarnings=FALSE,recursive=TRUE)
      
      
      
      if (length(drv)>0)  {   # SD card gets detected
        filename=paste(drv[1],"\\",filename,sep="")
        Sdsn=getSDserial(drv)
        out=checkSD(con=con,SDsn=Sdsn)
        if (file.exists(filename)){        # file present
          insize=file.info(filename)$size   # this information is no longer accessible after trying file.copy because of i/o errors    
          file.copy(filename,outfile,overwrite=FALSE) #sadly, file.copy() returns TRUE for partial copies
          
          dbq(con,paste("INSERT INTO SNBatWESTERHOLZ2.file_status (path,year_,date_,date_prev,filesize,box,upload_status,boxversion) VALUES('",outname,"',",substr(dtime,1,4),
                        ",'",dtime,"',",shQuote(prev),",",file.info(outfile)$size/1000,",",bx,",0,0)",sep=""))
          
          if (insize == file.info(outfile)$size) {
            dbq(con,paste("INSERT INTO TECHatWESTERHOLZ.technical_details (date_time_field,box,SD_K_Status,SD_K_Aktion,SD_ID) VALUES('",dtime,"','",bx,"','0','0','",Sdsn,"')",sep=""))
            out=paste("file copied without problems\n",out,'\n')
          } else{	
            dbq(con,paste("INSERT INTO TECHatWESTERHOLZ.technical_details (date_time_field,box,SD_K_Status,SD_K_Aktion,SD_ID) VALUES('",dtime,"','",bx,"','5','3','",Sdsn,"')",sep=""))
            out=paste("card damaged, file partly recovered\n",out,"\n")
          } } else{
            dbq(con,paste("INSERT INTO TECHatWESTERHOLZ.technical_details (date_time_field,box,SD_K_Status,SD_ID) VALUES('",dtime,"','",bx,"','3','",Sdsn,"')",sep=""))
            out=paste("No file on card\n",out,"\n")
          } } else {	dbq(con,paste("INSERT INTO TECHatWESTERHOLZ.technical_details (date_time_field,box,SD_K_Status,SD_ID) VALUES('",dtime,"','",bx,"','1','NULL')",sep=""))
                     out=paste("CAUTION: card not detected")
          }	
    } else out=paste('ERROR: destination file ',outfile,' already exists. File not copied!',sep="")
    on.exit(closeCon(con))
    cat(out)
    return(out)
  }
  
  
  
  getHandheld = function(snbDir = '//ds/raw_data_kemp/FIELD/Westerholz/SNB/RAWDATA',dtime, filename="box001.txt") {
    
    con = dbcon(database = 'SNBatWESTERHOLZ2',user='snb',password = 'cs')
    
    drv=findRemovable()
    path=paste(drv[1],"\\",dtime,sep="")
    
    flist=list.files(path, recursive = TRUE, full.names = TRUE) 
    boxlist=basename(dirname(flist))
    errlist=''
    if (length(flist)==0) paste("nothing to upload") else
    {
      for (i in (1:length(flist))) {
        outdir=paste(snbDir,substr(dtime,1,4),paste(substr(dtime,1,10),"CF",sep=""),tolower(boxlist[i]),sep="/")
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
      paste(i,' files processed',ifelse(errlist!='',paste('; NB: destination files for box(es) ',errlist,' already existed. THOSE FILES NOT COPIED',sep=""),''),sep="")
    }
    on.exit(closeCon(con))
  }	
  
  
  
  
  getSDGui=function(dte=format(Sys.time()-86400,"%Y.%m.%d %H:%M:%S")) {
    
    sdw=tktoplevel()
    
    boxsel=tclVar("666")
    datesel=tclVar(dte)
    
    
    
    
    boxentry  = tkentry(sdw, textvariable=boxsel)
    boxlabel  = tklabel(sdw,text="box (all 3 digits):")
    dateentry = tkentry(sdw,textvariable=datesel)
    datelabel = tklabel(sdw,text="datetime ")
    
    OnOk = function(bx=tclvalue(boxsel),dt_=tclvalue(datesel)){
      # if (missing(con)) con=snbcon()
      cardThere = findRemovable()
      if (length(cardThere)>1) tkmessageBox(message = "remove all USB storage devices that are not the SD-card you want to copy. Now.")
      if(length(cardThere)==0) foo = tkmessageBox(message="no card detected. Proceed?\n (yes= after inserting card (or if it's already there)\n no= cancel upload",type='yesno') else 
        foo = tclVar('yes')
      if (tclvalue(foo)=='yes'){
        txt = getSD(dtime=dt_,bx=bx)
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
  
  findRemovable = function() { #uses wmic to list all removable drives (fsutil needs admin power)
    
    drvLetter =paste(unique(gsub("[^A-Z]","",(system('wmic logicaldisk get caption',intern=TRUE)))),":",sep="")
    drvUsed=as.numeric(gsub("\\D","",system('wmic logicaldisk get freespace',intern=TRUE))[-1])
    drvType=as.numeric(gsub("\\D","",system('wmic logicaldisk get drivetype',intern=TRUE))[-1])
    
    drv=as.data.frame(cbind(drvLetter,drvUsed,drvType),stringsAsFactors=FALSE)
    
    return(drv$drvLetter[which(!is.na(drv$drvUsed)&drv$drvType==2)])
    
    
  }
  
  getSDserial = function(SDdir) { 
    foo=shell(paste("dir ",SDdir,sep=""),intern=TRUE)
    foo=gsub(".*is\ ","",foo[2])
    return(foo)
  }
  retireSD = function() {
    
    sdw=tktoplevel()
    onOk = function() {
      con = dbcon(database='TECHatWESTERHOLZ',user='snb',password='cs')
      drv = findRemovable()
      if (length(drv)>0) {
        SDid = getSDserial(drv)
        dbq(con,paste('INSERT IGNORE INTO TECHatWESTERHOLZ.SD_end (SDid) VALUES (',shQuote(SDid),')'))
        tkmessageBox(message = paste(SDid,'retired'))
        closeCon(con)
      } else tkmessageBox(message='card not detected')
    }
    OK.but <-tkbutton(sdw,text="retire SDcard",command=function() onOk())
    Cancel.But =tkbutton(sdw,text="CANCEL/DONE",command=function() tkdestroy(sdw))
    tkgrid(OK.but,Cancel.But)
    
    
  }
