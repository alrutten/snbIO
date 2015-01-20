
\name{getSD}
\alias{getSD}

\title{upload raw data from SDcards}
\description{uploads raw data from SDcards into a (previously created) RAWDATA/year/date folder}
\usage{
getSD(logger='snb',dtime,bx,filename="BOX001.TXT" )
		}
\arguments{
\item{logger}{logger type, currently 'snb' (loads to SNBatWESTERHOLZ2) and 'sf' (loads to SFatWESTERHOLZ) }
\item{dtime}{datetime at which the card was taken from the nestbox (should be written on the cardcover)}
\item{bx}{box that the SDcard belonged to}
\item{filename}{name of the datafile on the SDcard}
}
	
\details{
getSD generates the outputdirectory /bx, and copies as much of the datafile as possible. It generates a line in TECHatWESTERHOLZ.technical_details specifying dtime (see above), and the SD_K_Status (depending on how problematic the copying was).

}



\value{
NULL, if you're lucky.
}

\note{} 

\seealso{}
	

\examples{

the datadirectory is hardcoded, so don\'t run any examples.


}


\author{AR 11.11.14}