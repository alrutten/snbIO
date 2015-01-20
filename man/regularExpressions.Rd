
\name{regexpString}
\alias{regexpString}
\alias{regexpFetch}
\alias{regexpSplit}

\title{Regular expression strings}
\description{A collection of regular expression strings}
\usage{regexpString(item, year = 9999, version = 0)}

\arguments{
	  \item{item}{ Item (transponder, date, pirlb or batvolt) name (quotted) }
	  \item{dat}{ a vector  }
	  \item{firstMatch}{ a logical value (set to FALSE) when TRUE only the 1st match is returned}
	}

\value{
	\code{regexpString} {returns a regular expression string}
	\code{regexpFetch} {calls \code{regexpString} and applies the regular expression, see example}
	\code{regexpSplit} {splits a string into a vector given a regular expression}
	
 }	

\examples{
### regexpString
regexpString("transp")
regexpString("Date")
regexpString("pirlb")
regexpString("batvolt")
regexpString("transp")

### regexpFetch
String = c("1902101305270010E4E741F9C66F0001", "1902101305270012E4E741F9C66F0001", 
"1902101305280012E4E741F9C66F0001", "1902101305290012E4E741F9C66F0001")

regexpFetch(regexpString("transp"), String)
regexpFetch(regexpString("Date"), String)

regexpSplit(regexpString("Date"), paste(String, collapse = "") )





}


\author{MV}

















