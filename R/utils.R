

writeload = function(d, fname = gsub('\\\\','/',tempfile()), con, db, tb, ignore = FALSE, charset = 'latin1') {
  #ignore is for INSERT IGNORE
  write.table(d,fname,sep=',', na='\\N',quote=FALSE,row.names=FALSE, col.names=FALSE,fileEncoding = charset)
  if (ignore) foo = dbq(con,paste0("LOAD DATA LOCAL INFILE ", shQuote(fname), " IGNORE INTO TABLE ",paste(db,tb,sep='.'), 
                       " CHARACTER SET ",charset," FIELDS TERMINATED BY ',';")) else
              foo = dbq(con,paste0("LOAD DATA LOCAL INFILE ", shQuote(fname), " INTO TABLE ",paste(db,tb,sep='.'), 
                                              " CHARACTER SET ",charset," FIELDS TERMINATED BY ',';"))           
  file.remove(fname)
  return(foo)
}

is.clean = function(var) { #omit last row because that may be truncated although there's no other problems
  posl=c(16,23,32)
  if (length(var)>1) all(all(unique(nchar(var[-length(var)]) %in% posl,na.rm=TRUE)),all(!is.na(as.numeric(substring(var,0,12))))) else
    all(all(nchar(var)%in% posl,na.rm=TRUE),all(!is.na(as.numeric(substring(var,0,12)))))
}

regexpString = function(item, year=9999, version=0) { 
  
  #  ideal string patterns; dd mm yy hh mm ss pir lb tr
  
  transp     = "\\w{7,11}F0001"  
  ddmm ="((0[1-9])|([12]\\d)|(30(?!02))|(31(?!((0[2469])|11))))((0[1-9])|(1[012]))"
  #day 0-29    day 30 notfeb day 31 not 30daymonths        month
  prevyear =substring(year-1,3,4)
  anyDate =paste(ddmm,"(\\d\\d)(([01]\\d)|(2[0-4]))([0-5]\\d)([0-5]\\d)",sep="") #taking max days per month into account
  #year          hh            mm        ss
  specDate = paste("(((0[1-9])|([12]\\d)|(3[01]))((0[1-9])|(1[012]))(",substring(year,3,4),"|",prevyear,")(([01]\\d)|(2[0-4]))([0-5]\\d)([0-5]\\d)([1234]{0,4}|[012]{4}))", sep="")
  
  pirlb = "(^[012]{4}|(^1234|^123|^124|^134|^234|^12|^13|^14|^23|^24|^34|^1|^2|^3|^4))" # if version<2 pirlb should be ascending, can't be done in regexp hence had to list all combinations
  pir = ifelse(version<2,"[12]{1,2}","[01][02]")  
  lb = ifelse(version<2,"[34]{1,2}","[01][02]")	
  
  bv 			= "(BV|RT).{5}"	
  
  Record = paste(specDate,".*?(?=(",specDate,"|$))", sep="")  # valid date, random junk, until next valid date
  
  
  switch(item, transp =transp, ddmm=ddmm, anyDate=anyDate, specDate=specDate, pirlb=pirlb, pir=pir, lb=lb, bv=bv, Record=Record)
}





find.tj = function(var){
  which(as.numeric(var)> as.numeric(c(var[-1], var[length(as.vector(var))])))
}

tomorrow = function(ddmm){
  format(strptime(paste(ddmm,"08",sep=""),format="%d%m%y",tz="UTC")+24*3600,"%d%m")  }

#strptime assumes year=current year when no year is supplied, hence the addition of '08' to make 2902 recognisable as date.

yesterday = function(ddmm) { 
  format(strptime(paste(ddmm,"08",sep=""),format="%d%m%y",tz="UTC")-24*3600,"%d%m") }				
