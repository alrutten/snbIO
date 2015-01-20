
\name{loadDbase}
\alias{loadDbase}
\title{batchprocess to upload unprocessed datafiles into the SNBatWESTERHOLZ2 database}
\description{loads, checks, and uploads BOX0001.txt files with status=0 in SNBatWESTERHOLZ2.file_status (and changes the status according to data quality)}
\usage{loadDbase(con=snbcon(),year=current year)}
\arguments{
	\item{con}{snb database connection}
	\item{year}{ year for which to run the batchprocess, default is the current year}
	}
\value{}
\note{} 

\seealso{  See Also  \code{\link{extractVars} \link{readRaw} \link{readGrexp} \link{loadSingle} \link{readSingle}} }

\examples{

	loadDbase()

}


\author{AR 05.02.2011}