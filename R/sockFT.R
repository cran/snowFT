#
# Socket Implementation
#

processStatus.SOCKnode <- function(node) {
  stop("Function addtoCluster is not implemented for Socket")
}

getNodeID.SOCKnode <- function(node) {
  return(node$con)
}
