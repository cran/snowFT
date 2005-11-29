\name{snowFT-startstop}
\title{Starting snowFT Clusters}
\alias{makeClusterFT}
\usage{
makeClusterFT(spec, type = getClusterOption("type"), ...)
}
\arguments{
  \item{spec}{Cluster specification.}
  \item{type}{Character string that specifies cluster type. Only \code{type="PVM"} supported.}
  \item{...}{cluster option specifications}
}
\description{
  Functions to start a snowFT cluster and to set default cluster
  options.
}
\details{
  \code{makeClusterFT} starts a cluster of the specified or default type, loads the snowFT library on each node
  and returns a reference to the cluster.  Only the  cluster type
  \code{"PVM"} is supported in this version.  The \code{spec} argument should be an integer
  specifying the number of slave nodes to create. See \code{setDefaultClusterOptions()} of the snow package for setting cluster options.

  The cluster should be stopped by \code{stopCluster} of the snow package.
}
\seealso{\code{snow-startstop} functions of the snow package.} 
\examples{
  \dontrun{
cl <- makeClusterFT(5)
res <- clusterApplyFT(cl, 1:10, get("+"), 3)
stopCluster(res[[2]])
print(res[[1]])
  }
}
\keyword{programming}
