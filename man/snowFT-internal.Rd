\name{snowFT-internal}
\title{Internal snowFT Objects}
\alias{initStream}
\alias{freeStream}
\alias{resetRNG}
\alias{addClusterOptions}
\alias{recvOneDataFT}
\alias{recvOneDataFT.MPIcluster}
\alias{recvOneDataFT.PVMcluster}
\alias{recvOneResultFT}
\alias{GetNodefromReplic}
\alias{addtoCluster.MPIcluster}
\alias{addtoCluster.PVMcluster}
\alias{checkSprngNode} 
\alias{clusterCheckSPRNG} 
\alias{combinecl} 
\alias{getNodeID}
\alias{getNodeID.MPInode}
\alias{getNodeID.PVMnode}
\alias{getNodeID.SOCKnode}
\alias{initRNGstreamNodeRepli} 
\alias{processStatus.MPInode} 
\alias{processStatus.PVMnode}
\alias{processStatus.SOCKnode}
\alias{removecl}
\alias{removefromCluster.MPIcluster}
\alias{removefromCluster.PVMcluster}
\alias{repairCluster.PVMcluster} 
\alias{updatecl}
\alias{writetomngtfile}
\usage{
getNodeID.SOCKnode(node)
processStatus.SOCKnode(node)

addtoCluster.PVMcluster(cl, spec, ..., options = defaultClusterOptions)
getNodeID.PVMnode(node)
processStatus.PVMnode(node)
removefromCluster.PVMcluster(cl, nodes)
repairCluster.PVMcluster(cl, nodes, ..., options = defaultClusterOptions)

addtoCluster.MPIcluster(cl, spec, ..., options = defaultClusterOptions)
getNodeID.MPInode(node)
processStatus.MPInode(node)
removefromCluster.MPIcluster(cl, nodes)

initStream(type="RNGstream", name, ...)
freeStream(type="RNGstream", oldrng)
resetRNG(cl, nodes, repl, gentype="RNGstream",seed=rep(123456,6))
checkSprngNode()
clusterCheckSPRNG(cl, prngkind = "default", ...)
initRNGstreamNodeRepli(seed, n)

recvOneDataFT(cl, type, time)
recvOneResultFT(cl, type='b',time=0)
recvOneDataFT.MPIcluster(cl, type='b',time=0)
recvOneDataFT.PVMcluster(cl, type='b',time=0)

GetNodefromReplic(cl,replic)
combinecl(oldcl, add)
removecl(oldcl, reps)
updatecl(cl, compcl)
writetomngtfile(cl, file)
}
\description{
  Internal functions for the snowFT package.
}
\details{
  These are not to be called by the user.
}
\keyword{internal}
