\name{snowFT-package}
\alias{snowFT-package}
\alias{snowFT}
\docType{package}
\title{
Fault Tolerant Simple Network of Workstations
}
\description{
Extension of the snow package supporting fault tolerant
and reproducible applications, as well as supporting easy-to-use parallel programming - only one function is needed. 
It is written for the PVM communication layer.
}
\details{

\tabular{ll}{
Package: \tab snowFT\cr
Version: \tab 1.1-1\cr
License: \tab GPL\cr
Depends: \tab R (>= 2.3), snow (>= 0.3-3), rpvm (>= 1.0.2)\cr
}

The main function of this package is \code{\link{performParallel}}.

The package supports a snow cluster that is:
\enumerate{
\item Fault tolerant: The master checks repeatedly for failure in its waiting time and initiates a failure recovery if needed.
\item Load balanced AND produces reproducible results: one stream of random numbers associated with one replicate (instead of one stream per node as handled by snow).
\item Computationally transparent: Currently processed replicates and failed replicates stored into files. Allows defining a function that is called after each given number of replicates.
\item Dynamically resizeable: The cluster size is stored in a file which is read by the master repeatedly. In case of a modification the cluster is updated.
\item No administration overhead: All administration is managed by the master in its waiting time. (Note that there is a time-overhead for creating and destroying the cluster, as well as the RNG initialization. Thus, simple operations, such as the example below, will not gain from running in parallel.)
\item Allows running processes sequentially with the same random numbers as it would in parallel. Thus, results can be compared between the two modes.
\item Easy to use: All features, including creating the cluster, RNG initialization and clean-up, are available through one single function - \code{\link{performParallel}}.
}
}
\author{
Hana Sevcikova, A. J. Rossini

Maintainer: Hana Sevcikova <hanas@u.washington.edu>
}
\references{
http://www.stat.washington.edu/hana/parallel/snowFT-doc.pdf
}
\keyword{programming}
\keyword{ package }
\seealso{
\code{\link[snow]{clusterCall}}, \code{\link{performParallel}}
}
\examples{
  \dontrun{
# generates 500 times 1000 normally distributed random numbers on 5 nodes.
res <- performParallel(5, rep(1000, 500), fun=rnorm)
print(mean(unlist(res)))
}

}
