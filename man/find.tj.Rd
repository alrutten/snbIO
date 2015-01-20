
\name{find.tj}
\alias{find.tj}
\title{find timejump}
\description{pinpoints rows where a numerical variable is not in ascending order}
\usage{find.tj(var)}
\arguments{
	\item{var}{a vector or numerical variable (e.g. datetime)}
	}
\value{ returns a vector with the position(s) of number(s) in var that is/are followed by a lower number}
\note{can be used to identify problem rows when is.ascending() returns FALSE.} 
	
\seealso{\code{\link{is.ascending}}}
\examples{

x = c(1:5,3:5,4:7)
find.tj(x)
}


\author{AR 04.05.2011}