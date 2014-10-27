# Required libraries
library(VGAM)
require(MASS)

options(warn=-1)

args <- commandArgs(trailingOnly=TRUE)

# Firstly check these inputs are valid
pathDataApps <- as.character(args[1])
pathDataDownARev <- as.character(args[2])
isDownloadList <- as.character(args[3])
pathOutput <- as.character(args[4])


# 1 - read in the csv file with real data!!!
if(!file.exists(pathDataApps))
{ stop("The file does not exist")}
realDataApps <- read.csv(pathDataApps, skip=0, as.is=T)


# 2 - read in the csv file with real data!!!
if(!file.exists(pathDataDownARev))
{ stop("The file does not exist")}
realDataDownARev <- read.csv(pathDataDownARev, skip=0, as.is=T)


# Need to define this function here
pareto.MLE <- function(X)
{
  n <- length(X)
  
  m <- min(X)
  
  a <- n/sum(log(X)-log(m))
  
  return( c(m,a) ) 
  
}


# Merge these two files into one data structure by APPID
mergedFiles <- NULL
mergedFiles <- merge(realDataApps, realDataDownARev, by="X.item.id")

if(nrow(mergedFiles)==0)
{stop("There are no common rows between sales and rank data")}

#Need a check in here for negatives and missing data
#mergedFiles$X.item.id = as.numeric(! mergedFiles$X.item.id == "")


# Set data fields to NULL
downloads <- NULL
revenue <- NULL
rank <- NULL


# itemid, position, 
ranks <- mergedFiles$position
downloads <- mergedFiles$downloads
revenues <- mergedFiles$revenue

# Need an exit here if there is no data
if(is.na(ranks) || is.null(ranks))
{stop("Ranks list is either na or null")}



# A very simple non-linear regression that is inaccurate
# but worth a first implementation
# If it is free or paid then the below regression should be carried out
# else it will be on revenue
if (isDownloadList == "true"){ simple.model <- lm(log(downloads) ~ log(ranks))} else{ 
  simple.model <- lm(log(revenues) ~ log(ranks))}


b <- exp(simple.model$coefficients[c("(Intercept)")])
a <- -simple.model$coefficients[c("log(ranks)")]


# if isDownloadList is true then do the calibration
# on downloads, else do it on revenue
# if (isDownloadList == "true")
#   { cal <- pareto.MLE(downloads)}
# else
# {  cal <- pareto.MLE(revenues)}

script.results <- c(a, b, isDownloadList)

cat(script.results)

write.csv( script.results, pathOutput, row.names = c("a","b", "isDownloadList"))