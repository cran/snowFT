#
# PVM Implementation
#

recvOneDataFT.PVMcluster <- function(cl,type='b',time=0) {

    rtag <- findRecvOneTag(cl, -1)

    if (type == 'n') { # non-blocking receive
      recv <- .PVM.nrecv(-1, rtag)
      if (recv <= 0) return (NULL)
    } else {
      if (type == 't')  {# timeout receive
        recv <- .PVM.trecv(-1, rtag, sec=time)
        if (recv <= 0) return (NULL)
      } else  # blocking receive
      recv <- .PVM.recv(-1, rtag)
    }
    binfo <- .PVM.bufinfo(recv)
    for (i in seq(along = cl)) {
      if (cl[[i]]$tid == binfo$tid) {
        n <- i
        break
      }
    }
    data <- .PVM.unserialize()
    list(node = n, value = data)
  }


addtoCluster.PVMcluster <- function(cl, spec, ...,
                                    options = defaultClusterOptions) {
  options <- addClusterOptions(options, list(...))
  n <- length(cl)
  newcl <- vector("list",n+spec)
  for (i in seq(along=cl))
    newcl[[i]] <- cl[[i]]
  for (i in (n+1):(n+spec)) {
    newcl[[i]] <- newPVMnode(options = options)
    clusterEvalQpart(newcl,i,library(snowFT))
  }	
  class(newcl) <- c("PVMcluster")
  newcl
}

removefromCluster.PVMcluster  <- function(cl, nodes) {
  newcl <- vector("list",length(cl)-length(nodes))
  j<-0
  for (i in seq(along=cl)) {
    if (length(nodes[nodes == i])>0) 
      stopNode(cl[[i]])
    else {
      j<-j+1
      newcl[[j]] <- cl[[i]]
    }
  }
  class(newcl) <- c("PVMcluster")
  newcl
}

repairCluster.PVMcluster <- function(cl, nodes, ...,
                                     options = defaultClusterOptions) {
  newcl <- vector("list",length(cl))
  options <- addClusterOptions(options, list(...))
  for (i in seq(along=cl)) {
    if (length(nodes[nodes == i])>0) {
      stopNode(cl[[i]])
      newcl[[i]] <- newPVMnode(options = options)
      clusterEvalQpart(newcl,i,library(snowFT))
    } else 
    newcl[[i]] <- cl[[i]]
  }
  class(newcl) <- c("PVMcluster")
	
  newcl  
}

processStatus.PVMnode <- function(node) {
  status <- .PVM.pstats(node$tid)
  if (status != "OK")
    return(FALSE)
  return(TRUE)
}

getNodeID.PVMnode <- function(node) {
  return(node$tid)
}
