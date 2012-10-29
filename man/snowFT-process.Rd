\name{snowFT-process}
\title{Process control}
\alias{processStatus}
\alias{findFailedNodes}
\usage{
processStatus(node)
findFailedNodes(cl)
}
\description{Functions for process control in a cluster.}
\arguments{
  \item{cl}{Cluster object.}
  \item{node}{Node object.}
}
\details{
  \code{processStatus} checks the existence of a node.
  
  \code{findFailedNodes} identifies all failed nodes in the cluster.
  
  These functions are available only for the PVM layer.
}

\value{
  \code{processStatus} returns \code{FALSE} if the node does not exist,
  otherwise \code{TRUE}.
  
  \code{findFailedNodes} returns a matrix with one row per failed node
  and three columns: 1. node index within the cluster, 2. number of the last replication
  computed on that node, 3. node id. 
}

\author{Hana Sevcikova}
\keyword{programming}
