\name{snowFT-repair}
\title{Repairing a Cluster}
\alias{addtoCluster}
\alias{removefromCluster}
\alias{repairCluster}
\usage{
addtoCluster(cl, spec, ..., options = defaultClusterOptions)
removefromCluster(cl, nodes)
repairCluster(cl, nodes, ..., options = defaultClusterOptions)
}
\arguments{
  \item{spec}{Cluster specification. Using PVM, it is the number of
    additional nodes to create.}
  \item{cl}{Cluster object.}
  \item{nodes}{Indices of nodes.}
  \item{options}{Cluster options object.}
  \item{...}{Cluster option specifications.}
}
\description{
  Functions to add, remove and restart nodes of a snowFT cluster.
}
\details{
  \code{addtoCluster} adds new nodes to cluster \code{cl} and loads
snowFT on them. For \code{"PVM"}
  cluster the \code{spec} argument should be an integer
  specifying the number of nodes to create.

  \code{removefromCluster} removes nodes given by indices \code{nodes}
  from cluster \code{cl}.

  \code{repairCluster} replaces nodes (given by indices \code{nodes}) by
  new created nodes and loads snowFT on the new nodes.
  
  Cluster options used in  \code{addtoCluster} and \code{repairCluster}
  should be the same as used for creating the cluster \code{cl} (see
  \code{\link{makeClusterFT}}). 
}
\value{All three functions return the updated cluster object.}
\author{Hana Sevcikova}
\examples{
  \dontrun{
cl <- makeClusterFT(5)
clusterApply(cl, 1:5, get("+"), 3)
cl <- addtoCluster(cl,3)
fail<-findFailedNodes(cl)
cl<-repairCluster(cl,fail[,1])
clusterApply(cl, 1:8, get("+"), 3)
stopCluster(cl)
}
}
\keyword{programming}
