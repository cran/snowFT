#
# MPI Implementation
#

recvOneDataFT.MPIcluster <- function(cl, type='b', time=0) {
  # non-blocking receive (type='n') and timeout receive (type='t')
  # not implemented.
  stop("Function recvOneDataFT is not implemented for MPI")
}

addtoCluster.MPIcluster <- function(cl, spec, ...,
                                    options = defaultClusterOptions) {
  stop("Function addtoCluster is not implemented for MPI")
}

removefromCluster.MPIcluster <- function(cl, nodes) {
  stop("Function removefromCluster is not implemented for MPI")
}

repairCluster.PVMcluster <- function(cl, nodes, ...,
                                     options = defaultClusterOptions) {
  stop("Function repairCluster is not implemented for MPI")
}


processStatus.MPInode <- function(node) {
  stop("Function addtoCluster is not implemented for MPI")
}

getNodeID.MPInode <- function(node) {
  return(node$rank)
}
