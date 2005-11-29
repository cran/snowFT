#
# Process control
#

processStatus <- function(node) UseMethod("processStatus")


#
# Higher-Level Node Functions
#

recvOneDataFT <- function(cl,type,time) UseMethod("recvOneDataFT")
    

#
#  Cluster Modification
#

addtoCluster <- function(cl, spec, ..., options = defaultClusterOptions)
  UseMethod("addtoCluster") 
removefromCluster <- function(cl, nodes) UseMethod("removefromCluster")
repairCluster <- function(cl, nodes, ..., options = defaultClusterOptions)
  UseMethod("repairCluster")

#
# Cluster Functions
#

makeClusterFT <- function(spec, type = getClusterOption("type"), ...) {
    if (is.null(type))
        stop("need to specify a cluster type")
    cl <- switch(type,
        SOCK = stop("Function implemented only for PVM"),
        PVM = makePVMcluster(spec, ...),
        MPI = stop("Function implemented only for PVM"),
        stop("unknown cluster type"))
    clusterEvalQ(cl, library(snowFT))
    return(cl)
}


clusterCallpart  <- function(cl, nodes, fun, ...) {
    for (i in seq(along = nodes))
        sendCall(cl[[nodes[i]]], fun, list(...))
    lapply(cl[nodes], recvResult)
}

clusterEvalQpart <- function(cl, nodes, expr)
    clusterCallpart(cl, nodes, eval, substitute(expr), env=.GlobalEnv)


recvOneResultFT <- function(cl,type='b',time=0) {
    v <- recvOneDataFT(cl,type,time)
    if (length(v) <= 0) return (NULL)
    return(list(value = v$value$value, node=v$node))
}

clusterApplyFT <- function(cl, x, fun, initfun = NULL, exitfun=NULL,
                             printfun=NULL, printargs=NULL,
                             printrepl=max(length(x)/10,1),
                             gentype="None", seed=rep(123456,6),
                             prngkind="default", para=0,
                             mngtfiles=c(".clustersize",".proc",".proc_fail"),
                             ft_verbose=FALSE, ...) {

# This function is a combination of clusterApplyLB and FPSS
# (Framework for parallel statistical simulations), written by
# Hana Sevcikova.
# Features:
#  - fault tolerance - checks for failed nodes, in case of failure the
#                      cluster is repaired and the particular replication
#                      restarted.
#  - reproducible results - each replication is assigned to one
#                           particular RNG stream. Thus, proceeding
#                           in any order will give the same results.
#  - dynamic adaptation of degree of parallelism - the function reads the
#                 desired number of nodes from a file (that can be changed
#                 any time) and increases or decreases the number of nodes.
#  - keeping track about the computat`ion status - the replication numbers
#                 that are currently processed and that failed are
#                 written into files.
#  - efficient administration - all the management work is done only
#                 when there is no message arrived and thus nothing
#                 else to do.

  if (is.na(pmatch(attr(cl,"class"), "PVMcluster"))) {
    cat("\nThe function clusterApplyFT can be used only with PVM interface!\n")
    return(list(NULL,cl))
  }

  if (length(cl)<=0) 
     stop("No cluster created!")
 
  if (!is.na(pmatch(prngkind, "default")))
    prngkind <- "LFG"
  prngnames <- c("LFG", "LCG", "LCG64", "CMRG", "MLFG", "PMLCG")
  kind <- pmatch(prngkind, prngnames)
  gennames <- c("RNGstream", "SPRNG", "None")
  gen <- pmatch(gentype,gennames)
  if (is.na(gen))
    stop(paste("'", gentype,
               "' is not a valid choice. Choose 'RNGstream', 'SPRNG' or 'None'.",
               sep = ""))
  gentype <- gennames[gen]

  lmng <- length(mngtfiles)
  if (lmng < 3)
    mngtfiles[(lmng+1):3] <- ""
  manage <- nchar(mngtfiles) > 0
  if (ft_verbose) {
     cat("\nFunction clusterApplyFT:\n")
     cat("   gentype:",gentype,"\n")
     cat("   seed:   ",seed,"\n")
     cat("   Management files:\n")
     if(manage[1])
	cat("     cluster size:", mngtfiles[1],"\n")
     if(manage[2])
	cat("     running processes:", mngtfiles[2],"\n")
     if(manage[3])
	cat("     failed nodes:", mngtfiles[3],"\n")
  }
  n <- length(x)
  p <- length(cl)
  fun <- fun # need to force the argument
  printfun <- printfun
  val <- NULL

  if (n > 0 && p > 0) {
    wrap <- function(x, i, n, gentype, seed, prngkind, ...){
      if (gentype != "None")
        oldrng <- initStream (gentype, as.character(i), nstream=n,streamno=i-1,
                              seed=seed,kind=prngkind, para=para)
      value <- try(fun(x, ...))
      if (gentype != "None")
        freeStream(gentype, oldrng)
      return(list(value = value, index = i))
    }
    submit <- function(node, job, n, gentype, seed, prngkind) {
      args <- c(list(x[[job]]), list(job), list(n), list(gentype),
                list(seed),list(prngkind),list(...))
      sendCall(cl[[node]], wrap, args)
    }

    val <- vector("list", length(x))
    if (manage[1])
      write(p, file=mngtfiles[1])
    replvec <- 1:n
    for (run in 1:3) { # second and third run is for restarting failed
                       # replications
      if (run > 1) {
	if (ft_verbose) 
		cat(run,"th run for replications:",frep,"\n")
        replvec <- frep
        n<-length(replvec)
      }
      for (i in 1 : min(n, p)) {         
        repl <- replvec[i]
        submit(i, repl,length(x),gen,seed,kind)
        cl[[i]]$replic <- repl
      }
      clall<-cl
      fin <- 0
      frep <- c() # list of replications of failed nodes
      if (manage[3])
        write(frep,mngtfiles[3])
      startit<-min(n, p)
      freenodes<-c()
      it<-startit
      while(fin < (n-length(frep))) {
        it <- it+1
        if (it <= n) 
          repl<-replvec[it]
        while ((length(freenodes) <= 0) |
               ((it > n) & fin < (n-length(frep)))) { # all nodes busy
                                        # or wait for remaining results
          d <- recvOneResultFT(clall,'n') # look if there is any result
          if (length(d) <= 0) { # no results arrived yet
            while (TRUE) {
                   # some administration in the waiting time
                   # ***************************************
              mfn <- findFailedNodes(cl) # look for failed nodes
              if (nrow(mfn) > 0) { # failed nodes found
                fn<-mfn[,1]
                frep <- c(frep, mfn[(mfn[,2]>0),2]) # only nodes where
                                        # computation is running
		if (ft_verbose) {
			cat("   Failed slaves detected: ", fn,"\n")
			cat("   Repair cluster ...\n")
		}
                cl <- repairCluster(cl,fn)
		
                if (!is.null(initfun)){
		  if (ft_verbose) 
			cat("   calling initfun ...\n")
                  clusterCallpart(cl,fn,initfun)
		}
                if (gentype != "None"){
		  if (ft_verbose) 
			cat("   initializing RNG ...\n")
                  resetRNG(cl,fn,length(x),gentype,seed)
		}
                             #keep all nodes in case
                             #messages from failed nodes arrive later
                clall<-combinecl(clall,cl[fn])               
                freenodes<-c(freenodes,fn)
                if (it <= n) break # exit the loop only if there are
                                   # comput. to be started 
              }
              if (manage[1])
                  # read p from a file 
                newp <- scan(file=mngtfiles[1],what=integer(),nlines=1)
              else newp <- p
              if (manage[2])
                  # write the currently processed replications into a file 
                writetomngtfile(cl,mngtfiles[2])
              if (manage[3])
                  # write failed replications into a file
                write(frep,mngtfiles[3])
              if (newp > p) { # increase the grad of parallelism
                cl<-addtoCluster(cl, newp-p)
                if (!is.null(initfun))
                  clusterCallpart(cl,(p+1):newp,initfun)
                if (gentype != "None")
                  resetRNG(cl,(p+1):newp,length(x),gentype,seed)
                clall<-combinecl(clall,cl[(p+1):newp])
                freenodes<-c(freenodes,(p+1):newp)
                p <- newp
                break
              }
              p <- newp
                  # end of administration ****************************
              
              d <- recvOneResultFT(clall,'t',time=5) # wait for a result for
                                                   # 5 sec
              if (length(d) > 0) break # some results arrived, if not
                                       # do administration again
            }
            if ((length(freenodes) > 0) & (it <= n)) break
          }
          val[d$value$index] <- list(d$value$value)
          node <- GetNodefromReplic(cl,d$value$index)
          if (node > 0) {
            if (length(cl) > p) { # decrease the degree of parallelism
              if (!is.null(exitfun))
                clusterCallpart(cl,node,exitfun)
              clall<-removecl(clall,c(cl[[node]]$replic))
              cl <- removefromCluster(cl,node)
            } else {
              freenodes <- c(freenodes,node)
              cl[[node]]$replic<-0
            }
          } else {#result from a failed node
            frep <- frep[-which(frep == d$value$index)]
            clall <- removecl(clall,c(d$value$index))
          }
          fin <- fin + 1
          if (!is.null(printfun) & ((fin %% printrepl) == 0))
            try(printfun(val,fin,printargs))
        }
        if (it <= n) {
          submit(freenodes[1], repl,length(x),gen,seed,kind)
          cl[[freenodes[1]]]$replic <- repl
          clall <- updatecl(clall,cl[[freenodes[1]]])
          freenodes <- freenodes[-1]
        }
      }
      if (length(frep) <= 0) break # everything went well, no need to go
                                        # to the next run
    }
    if (length(frep) > 0)
      cat("\nWarning: Some replications failed!\n") # even in the third run
  }
  return(list(val,cl))
}

performParallel <- function(count, x, fun, initfun = NULL, exitfun =NULL,
                            printfun=NULL,printargs=NULL,
                            printrepl=max(length(x)/10,1),
                            cltype = getClusterOption("type"),
                            gentype="RNGstream", seed=rep(123456,6),
                            prngkind="default", para=0, 
			    mngtfiles=c(".clustersize",".proc",".proc_fail"),
                            ft_verbose=FALSE, ...) {

  RNGnames <- c("RNGstream", "SPRNG", "None")
  rng <- pmatch (gentype, RNGnames)
  if (is.na(rng))
    stop(paste("'", gentype,
               "' is not a valid choice. Choose 'RNGstream', 'SPRNG' or 'None'.",
               sep = ""))

  gentype <- RNGnames[rng]

  if (ft_verbose) {
     cat("\nFunction performParallel:\n")
     cat("   creating cluster ...\n")
  }
  cl <- makeClusterFT(min(count,length(x)), cltype)

  if (!is.null(initfun)) {
    if (ft_verbose) 
	cat("   calling initfun ...\n")
    clusterCall(cl, initfun)
  }

  if (RNGnames[rng] != "None") {
    if (ft_verbose) 
	cat("   initializing RNG ...\n")
    clusterSetupRNG.FT(cl, type=gentype, streamper="replicate", seed=seed,
                    n=length(x), prngkind=prngkind)
  } else {
    if (ft_verbose) 
	cat("   no RNG initialized\n")
  }
  if (ft_verbose) 
     cat("   calling clusterApplyFT ...\n")
 
  res <- clusterApplyFT (cl, x, fun, initfun=initfun, exitfun=exitfun,
                           printfun=printfun, printargs=printargs,
                           printrepl=printrepl, gentype=gentype,
                           seed=seed, prngkind=prngkind,
                           para=para, mngtfiles=mngtfiles, 
			   ft_verbose=ft_verbose, ...)
  if (ft_verbose) 
     cat("   clusterApplyFT finished.\n")
  val<-res[[1]]
  cl <- res[[2]]
  if (!is.null(exitfun)) {
     if (ft_verbose) 
	cat("   calling exitfun ...\n")
     clusterCall(cl, exitfun)
  }
  stopCluster(cl)
  if (ft_verbose) 
     cat("   cluster stopped.\n")
  return(val)
}

clusterSetupRNG.FT <- function (cl, type="RNGstream", streamper="replicate", ...) {
  RNGnames <- c("RNGstream", "SPRNG")
  rng <- pmatch (type, RNGnames)
  if (is.na(rng))
    stop(paste("'", type,
               "' is not a valid choice. Choose 'RNGstream' or 'SPRNG'.",
               sep = ""))
  modus <- c("node", "replicate")
  stream <- pmatch(streamper,modus)
  if (is.na(stream))
    stop(paste("'", streamper,
               "' is not a valid choice. Choose '",modus[1], "' or '",
               modus[2],"'.", sep = ""))
  type <- RNGnames[rng]
  streamper <- modus[stream]
  if (rng == 1) {
    switch (streamper,
            cluster = clusterSetupRNGstream(cl, ...),
            replicate = clusterSetupRNGstreamRepli(cl, ...)
            )
  } else {
    switch (streamper,
            cluster = clusterSetupSPRNG(cl, ...),
            replicate = clusterCheckSPRNG (cl, ...)
            )
  }
  c(type,streamper)
}


#
# Cluster SPRNG Support 
#

clusterCheckSPRNG <- function (cl, prngkind = "default", ...) 
{
  # for purposes of reproducible results. Like clusterSetupSPRNG, but makes
  # only a check,the initialization happens with each replication call
  # in clusterApplyFT

  if (! require(rsprng))
    stop("the `rsprng' package is needed for SPRNG support.")
  if (!is.character(prngkind) || length(prngkind) > 1)
    stop("'rngkind' must be a character string of length 1.")
  if (!is.na(pmatch(prngkind, "default")))
    prngkind <- "LFG"
  prngnames <- c("LFG", "LCG", "LCG64", "CMRG", "MLFG", "PMLCG")
  kind <- pmatch(prngkind, prngnames) - 1
  if (is.na(kind))
    stop(paste("'", prngkind, "' is not a valid choice", sep = ""))
  nc <- length(cl)
  clusterCall(cl, checkSprngNode)
}

checkSprngNode <- function () 
{
  if (! require(rsprng))
    stop("the `rsprng' package is needed for SPRNG support.")
  RNGkind("user")
}

#
# rlecuyer support
#

clusterSetupRNGstreamRepli <- function (cl, seed=rep(12345,6), n, ...){
  # creates on all nodes one stream per replication.  
    if (! require(rlecuyer))
        stop("the `rlecuyer' package is needed for RNGstream support.")
    .lec.init()
    .lec.SetPackageSeed(seed)
    nc <- length(cl)
#    names <- as.character(1:n)
#    .lec.CreateStream(names)
    for (i in 1:nc) {
        invisible(sendCall(cl[[i]],initRNGstreamNodeRepli,
                           c(list(seed),list(n))))
      }
    invisible(lapply(cl[1:nc], recvResult))
  }

initRNGstreamNodeRepli <- function (seed, n) {
    if (! require(rlecuyer))
        stop("the `rlecuyer' package is needed for RNGstream support.") 
    .lec.init()
    .lec.SetPackageSeed(seed)
    names <- as.character(1:n)
    .lec.CreateStream(names)
    return(1)
  }

initStream <- function (type="RNGstream", name, ...) {
  oldrng <- switch(type,
                   RNGstream = .lec.CurrentStream(name),
                   SPRNG = initSprngNode(...)
                   )
  return(oldrng)
}

freeStream <- function (type="RNGstream", oldrng) {
  switch(type,
         RNGstream = .lec.CurrentStreamEnd(oldrng),
         SPRNG = free.sprng(oldrng)
         )
}

resetRNG <- function(cl, nodes, repl, gentype="RNGstream",seed=rep(123456,6))
	 # resets the RNG on selected nodes of cluster cl 
  {
    if(gentype == "RNGstream") {
      for (i in 1:length(nodes)) 
        invisible(sendCall(cl[[nodes[i]]],initRNGstreamNodeRepli,
                           c(list(seed),list(repl))))
      invisible(lapply(cl[nodes], recvResult))
    } else {
      clusterCallpart(cl, nodes, checkSprngNode)
    }
  }

getNodeID <- function (node) UseMethod("getNodeID")

findFailedNodes <- function (cl) {
  failed <- matrix(0,nrow=0,ncol=3)
  for (i in seq(along=cl)) {
    if (!processStatus(cl[[i]]))
      failed<-rbind(failed,c(i,cl[[i]]$replic,getNodeID(cl[[i]])))
  }
  return(failed)
}

combinecl <- function(oldcl, add) {
  attr<- attr(oldcl,"class")
  n <- length(oldcl)
  count <- length(add)
  if (count <= 0 ) return (oldcl)
  cl <- vector("list",n+count)
  for (i in seq(along=oldcl))
    cl[[i]] <- oldcl[[i]]
  j<-0
  for (i in (n+1):(n+count)){
    j<-j+1
    cl[[i]] <- add[[j]]
  }
  class(cl) <- c(attr)
  cl
}

removecl <- function(oldcl, reps) {
  attr<- attr(oldcl,"class")
  n <- length(oldcl)
  count<-length(reps)
  cl <- vector("list",n-count)
  j<-0
  for (i in seq(along=oldcl)) {
    if (length(reps[reps == oldcl[[i]]$replic]) <= 0) {
	j <- j+1 
    	cl[[j]] <- oldcl[[i]]
	}
}
  class(cl) <- c(attr)
  cl
}

updatecl <- function(cl, compcl) {
  for (i in seq(along=cl)) {
    if (getNodeID(cl[[i]]) == getNodeID(compcl)) {
      cl[[i]]$replic<-compcl$replic
      break
    }
  }
  cl
}

GetNodefromReplic <- function(cl,replic) {
  for (i in seq(along=cl))
    if (cl[[i]]$replic == replic) return(i)
  return(0)
}

writetomngtfile <- function(cl, file) {
  n <- length(cl)
  repl<-rep(0,n)
  for (i in seq(along=cl))
    repl[i]<-cl[[i]]$replic
  write(repl,file)
}


#
#  Library Initialization
#

.First.lib <- function(libname, pkgname) {
	   require(snow)
}
