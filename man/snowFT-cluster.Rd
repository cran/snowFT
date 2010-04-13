\name{snowFT-cluster}
\title{Cluster-Level Functions with Fault Tolerant Features}
\alias{performParallel}
\alias{clusterApplyFT}
\alias{clusterCallpart}
\alias{clusterEvalQpart}
\usage{
clusterApplyFT(cl, x, fun, initfun = NULL, exitfun = NULL, 
               printfun = NULL, printargs = NULL, 
               printrepl = max(length(x)/10,1), gentype = "None", 
               seed = rep(123456,6), prngkind = "default", para = 0, 
               mngtfiles = c(".clustersize",".proc",".proc_fail"), 
               ft_verbose = FALSE, ...)

performParallel(count, x, fun, initfun = NULL, exitfun = NULL, 
                printfun = NULL, printargs = NULL, 
                printrepl = max(length(x)/10,1), 
                cltype = getClusterOption("type"), 
                gentype = "RNGstream", seed = sample(1:9999999,6), 
                prngkind = "default", para = 0, 
                mngtfiles = c(".clustersize",".proc",".proc_fail"),
                ft_verbose = FALSE, 
                outfile = getClusterOption("outfile"), ...)

clusterCallpart(cl, nodes, fun, ...)

clusterEvalQpart(cl, nodes, expr)
}
\arguments{
  \item{cl}{Cluster object.}
  \item{count}{Number of cluster nodes. If \code{count=0}, the process is run sequentially.}
  \item{fun}{Function or character string naming a function.}
  \item{x}{Array whose length determines how many times \code{fun} is to
  be called. \code{x[i]} is passed to \code{fun} (as its first argument)
  at $i$th call.}
\item{initfun}{Function or character string naming a
function with no
  arguments that is to
  be called on each node prior to the computation.}
\item{exitfun}{Function or character string naming a function with no
  arguments that is to
  be called on each node after the computation is completed.}
\item{printfun, printargs, printrepl}{\code{printfun} is a function or
  character string naming a function that is to be called on the master
  node after each
  \code{printrepl} completed replicates, and thus it can be used for accessing
  intermediate results. Arguments passed to
  \code{printfun} are: a list (of length \code{|x|}) of results (including
the non-finished
  ones), the number of finished results,
  and \code{printargs}.}
\item{cltype}{Character string that specifies cluster type (see
  \code{\link{makeClusterFT}}).}
\item{gentype}{Character string that specifies type of the used
  RNG. Possible values: "RNGstream" (L'Ecuyer's RNG),
  "SPRNG", or "None", see
\code{\link{clusterSetupRNG.FT}}. If
  \code{gentype="None"}, no RNG action is taken.}
\item{seed, prngkind, para}{Seed, kind and parameters for the RNG (see
  \code{\link{clusterSetupRNG.FT}}).}
\item{mngtfiles}{A character vector of length 3 containing names of
  management files: \code{mngtfiles[1]} for managing the
  cluster size, \code{mngtfiles[2]} for storing the replicates
  being currently computed, \code{mngtfiles[3]} for storing the failed
  replicates. If any of these files equals an empty string, the
  corresponding management actions are not performed. If the files
  already exist, their content
  is overwritten.}
  \item{ft_verbose}{If TRUE, debugging messages are sent to standard output.}
  \item{outfile}{Name of file which the nodes write into.} 
  \item{expr}{Expression to evaluate.}
  \item{nodes}{Indices of cluster nodes.}
  \item{...}{Additional arguments to pass to function \code{fun}.}
}
\description{
  Functions extending the collection of cluster-level functions of the
  snow package providing fault tolerance, reproducibility and additional
  management features. The heart of the package is the function
  \code{performParallel}. 
}
\details{
  \code{clusterApplyFT} is a fault tolerant version of
  \code{clusterApplyLB} of the snow package with additional features, such as results
  reproducibility, computation transparency and dynamic cluster
resizing. The master process searches for failed nodes in its
  waiting time. If failures are detected, the cluster is
  repaired. All failed computations are restarted (in three additional
  runs) after the replication
  loop is finished, and hence the user should not notice any
  interruptions.

 The file \code{mngtfiles[1]} is initially written by the master
  prior to the computation and it contains a single integer value corresponding
  to the number of cluster nodes. Then the value can be arbitrarily changed by
  the user (but should remain in the same format). The master reads the
  file  in its waiting time. If the value in this file is larger than
  the current
  cluster size, new nodes are created and the computation is expanded on
  them. If on the other hand the value is smaller, nodes are
  successively discarded after they finish their current
  computation.
  The arguments \code{initfun, exitfun} in
\code{clusterApplyFT} are only used, if there are
  changes in the cluster, i.e. if new nodes are added or if nodes are
  removed from cluster.

  The RNG uses
the scheme 'one stream per replicate', in contrary to 'one stream per
node' used by \code{clusterApplyLB}. Therefore with each replicate, the
RNG is reset to the corresponding stream (identified by the replicate
number). Thus, the final results are reproducible.

  \code{performParallel} is a wrapper function for
  \code{clusterApplyFT} and we recommend using this function rather than
  using \code{clusterApplyFT} directly. It creates a cluster of
\code{count} nodes,
  on all nodes it
  calls \code{initfun} and initializes the RNG. Then it calls
  \code{clusterApplyFT}. After the computation is finished, it calls
  \code{exitfun} on all nodes and stops the cluster. If \code{count=0}, function \code{fun} is invoked sequentially with the same settings (including random numbers) as it would in parallel. This mode can be used for debugging purposes.

  \code{clusterCallpart} calls a function \code{fun} with identical arguments 
  \code{...} on nodes
  specified by indices \code{nodes} in the cluster \code{cl} and returns a list
  of the results.
  
  \code{clusterEvalQpart} evaluates a literal expression on nodes
  specified by indices \code{nodes}. 
}

\value{\code{clusterApplyFT} returns a list of two elements. The first
  one is a list (of length \code{|x|}) of results, the second one is the
(possibly updated)
  cluster object.

  \code{performParallel} returns a list of results.
}
\examples{
  \dontrun{
# generates n normally distributed random numbers in r replicates
# on p nodes and prints their mean after each r/10 replicate.

printfun <- function(res, n, args=NULL) {
  res <- unlist(res)
  res <- res[!is.null(res)]
  print(paste("mean after:", n,"replicates:", mean(res),
           "(from",length(res),"RNs)"))
  }

r<-1000; n<-100; p<-5
res <- performParallel(p, rep(n,r), fun=rnorm,
  gentype="RNGstream", seed=rep(1,6), printfun=printfun)

# Setting p<-0 will run rnorm sequentially and should give 
# exactly the same results
}
}

\keyword{programming}
\author{Hana Sevcikova}
