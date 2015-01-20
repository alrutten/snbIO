
\name{read}
\alias{readRaw}
\alias{readGrexp}
\alias{readSingle}
\alias{dropSingle}
\alias{loadSingle}
\title{reads raw BOX001.txt datafiles}
\description{attempts to read a raw datafile with read.table; if the resulting dataframe is not conform the boundaries of is.clean the data get extracted using regular expression matching in readGrexp. Output is a dataframe with full record, bout length, and status.}
\usage{
readRaw(path)
readGrexp(d=NA, path=NA,min.match=0.1)
readSingle(con=con, path=NA, id=NA)
loadSingle(con = snbcon(), id, ...)
dropSingle(con = snbcon(), id, ...)
		}
\arguments{
	\item{path}{BOX001.txt file location}
	\item{d}{a data.frame with a variable V1 that contains the raw data}
	\item{min.match}{threshold value for the ratio of number of characters in matched records vs. total number of characters within a chunk of faulty records. This prevents the regular expression from latching on to 'wrong' dates (which leads to discarding of valid data). If set too high, however, valid matches in runs of junk get discarded as well. The RAW_YEAR databases get loaded with a min.match of 0.1; if you want to load individual files you may get more valid matches by playing around with the value of min.match a bit, depending on the anatomy of the datafile (mainly the length of junk runs).}
	\item{con}{connection to an SQL database}
	\item{id}{file id value in SNBatWESTERHOLZ2.file_status table}
	}
\details{
readRaw() reads the BOX001.txt datafile using read.table, and then tests the data using is.clean(). If any of the records fail this test, the data is subsequently passed to readGrexp(), which parses the data using regular expression matching. readGrexp() can be used directly as well.
	
readGrexp() divides the d$V1 data into chunks of most likely valid records (which start with a valid date and whose length does not exceed 32 characters), and most likely faulty records (too long, too short, don't start with a valid date). The valid bits are read as is, and the last and first dates on neighbouring valid chunks are used to formulate the regular expression with which data is extracted from chunks of faulty records. When the ratio of matched data vs. total data within a faulty chunk does not exceed min.match, the matches are discarded and the consecutive date is tested. This is to prevent 'matching' very unlikely dates (and, consequently, not matching the actual dates- this would lead to data loss), which is also what happens if the regular expression is set to match any valid ddmmyy combination.

either d or a filepath should be provided.


readSingle() uses either the path, or the connection and file id provided to read a BOX0001.txt file using read.table().
}



\value{ 
\item{readRaw}{a dataframe with variable 'full' containing full valid records, 'bout_length' containing the run lenght (n lines in file) for each specific record, and 'status' for upload status (1=using read.table, 2= using readGrexp)}
\item{readGrexp}{ same as read.raw, minus the 'status' variable}
}

\note{} 

\seealso{\code{\link{extractVars}, \link{loadDbase}}}
	

\examples{


cl = dbq(con=dbcon(),paste("SELECT * FROM file_status f Where status=1 and year_=2011"))
foo=readRaw(path=cl[nrow(cl),"path"])

loadSingle(con=dbcon(),id=20106)


}


\author{AR 13.10.2014}