
\name{snb}
\alias{snb}
\docType{package}
\title{Smart nest box data: management, post-processing and analysis}
\description{ \pkg{snb} overview and concepts}

\details{

\strong{database upload/download functions}
	\tabular{ll}{
  \link{getSDGui} \tab userinterface for copying SD-cards \cr
	\link{loadDbase} \tab batch function to process unprocessed files \cr
	\link{readRaw} \tab attempts to read rawdata using read.table; if that fails, reverts to read.grexp \cr
	\link{readGrexp} \tab reads rawdata using regular expression matching to find valid data \cr
	\link{extractVars} \tab parses valid variables from raw data \cr
	
	}

\strong{Post-processing raw data: function list}
	\tabular{ll}{
	\link{find.tj} \tab identifies numerical values that are followed by a lower value \cr
	\link{is.clean} \tab \cr
	
	}
	
\strong{Post-processing raw data: pipe line description}
	\tabular{ll}{
	}

	
\strong{Post-processing extract visits functions}
	\tabular{ll}{
  \link{assignEvents} \tab 
	\cr
	}


}




\author{Mihai Valcu, Anne Rutten, Lotte Schlicht}


