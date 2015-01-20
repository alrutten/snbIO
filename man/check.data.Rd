
\name{check.data}
\alias{is.clean}
\alias{is.friday}
\alias{is.ascending}
\title{various data checks (and finding out whether it is friday, because it is very important to know if it's friday or not)}
\description{various data checks (and finding out whether it is friday, because it is very important to know if it's friday or not)}
\usage{is.ascending(var)

is.clean(var)


is.friday(dt=Sys.Date())

}
\arguments{
	\item{var}{variable (numeric, for is.ascending)}
	\item{dt}{datetime}
	\item{year}{year}
	\item{con}{connection to SQL database}
	}
\details{
is.clean checks if the data in 'var' starts with 12 numeric characters, and whether the length of 'var' is within valid record limits (i.e., in c(12:16,19:23,28:32)	

is.ascending checks that var is in ascending order.

}

\value{
is.clean and is.ascending return a boolean (TRUE for clean/ascending, FALSE for not clean/not ascending).


is.friday may tell you whether it is friday or not.}

\note{} 
\examples{

foo <- data.frame(a = c(1:4,2,5:10))
is.ascending(foo$a)

bar = c(1:20, rep(20,4))
is.ascending(bar)

is.friday()

}


\author{AR 12.04.2011}