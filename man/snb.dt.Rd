
\name{snb.dt}
\alias{backtoPOSIXct}
\alias{wday}
\alias{tomorrow}
\alias{yesterday}
\title{various datetime manipulation functions}
\description{ various datetime manipulation functions}

\usage{backtoPOSIXct(timevar)

wday(datetime)

tomorrow(ddmm)

yesterday(ddmm)}

\arguments{
	\item{timevar}{ex-POSIXct numerical variable}
	\item{datetime}{datetime variable}
	\item{ddmm}{4-character variable containing day (dd) and month (mm)}
		}
		
\details{
backtoPOSIXct re-instates POSIXct class to a timevar that was originally POSIXct (and is now a numerical value representing the number of seconds since 01-jan-1970 due to some random action in r that made it lose its attributes)	

wday returns the day of the week, starting on monday, to be compatible with %W (%w starts on sunday, but %W starts on monday)

tomorrow returns the day after ddmm (including 29th of february); make sure to put ddmm in quotes

yesterday returns the day before ddmm (including 29th of february); make sure to put ddmm in quotes
}	
\value{
\item{backtoPOSIXct}{a POSIXct variable}
\item{wday}{a numerical value from 0 to 6 representing day of the week (0 being monday)}
\item{tomorrow, yesterday}{a character string ddmm, with dd=day and mm=month}
}
\note{}
	

\examples{

df = data.frame(v1=c(rep(1,4),2,3),time=as.POSIXct(strptime(c(rep("101010101010",5),"101010101011"),format = "\%d\%m\%y\%H\%M\%S")))
foo=aggregate(df,list(df$v1),max)
backtoPOSIXct(foo$time)

yesterday("0103")
tomorrow("0103")

wday(Sys.time())

}


\author{AR 13.04.2011}