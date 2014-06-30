#
# Created by Patrick O'Driscoll on 12 Jun 2014.
# Copyright © 2014 Reflection.io. All rights reserved.
#

# Read in the two paths and read from the csvs that have been saved
# Read the output file path
# Read in the user variables as input (remove MySQL material)
args <- commandArgs(trailingOnly=TRUE)

pathFree <- as.character(args[1])
pathPaid <- as.character(args[2])
pathOutput <- as.character(args[3])
cut.point.par  <- as.integer(args[4])
Napps <- as.integer(args[5])
Dt.in <- as.integer(args[6])

free.raw <- read.csv(pathFree,skip=0,as.is=T)
paid.raw <- read.csv(pathPaid,skip=0,as.is=T)

#free.raw$usesiap = as.numeric(! free.raw$usesiap == "")
#paid.raw$usesiap = as.numeric(! paid.raw$usesiap == "")

# Allow for truncated regression (standard package)
require(robustbase)

## First, select apps with both a top rank and and grossing position
basic.indx <- !is.na(paid.raw$top.position) & !is.na(paid.raw$grossing.position)  
basic.df <- paid.raw[basic.indx,]


## This is the cut point (where the truncation occurs in the list)
cut.point <- cut.point.par

basic.model <- lmrob(log(grossing.position) ~ log(top.position)+log(price), data=basic.df)

## Compute basic coefficients as in Mellon paper
ag <- -1/basic.model$coefficients[c("log(price)")]

ap <- -1* basic.model$coefficients[c("log(top.position)")]/basic.model$coefficients[c("log(price)")]

b.ratio <- exp(-1*basic.model$coefficients[c("(Intercept)")]/basic.model$coefficients[c("log(price)")])

# This guess is vitally important - an aggregated number has to be accurate
Dt.sim  <- Dt.in

bp.sim <- Dt.sim/(sum((1:cut.point)^-ap))

bg.sim <- b.ratio*bp.sim


## Sample some apps
## NOTE - that this is a proxy for what would happen with real data
Napps.with.info <- Napps


sim.indx <- sample(1:nrow(basic.df), Napps.with.info, replace=F)

apps.with.info.df <- basic.df[sim.indx,]


# From Equation (3) in the Paper!
downloads.info <- bg.sim*((apps.with.info.df$grossing.position)^(-ag)) / apps.with.info.df$price

## This is the end of the proxy section - the developer data will be above rejigged though

## Now we have some app-data, we can infer Dt
my.labelled.df <- cbind(apps.with.info.df, downloads.info)


#This is a robust version of the previous regression
labelled.apps.model <- lm(log(downloads.info) ~ log(grossing.position), data = my.labelled.df)


## compute aggregated Downloads Dt                                                               
Dt <- trunc(sum(exp(predict(labelled.apps.model, data.frame(grossing.position=41:cut.point))))) + sum(downloads.info)

## Compute remaining parameters
#  From paper just before equation (9) - simple algebra
bp <- Dt/(sum((1:cut.point)^(-ap)))
bg <- b.ratio *bp


free.indx  <- !is.na(free.raw$top.position) & !is.na(free.raw$grossing.position) & !is.na(free.raw$usesiap)

my.iap.df <- rbind(basic.df, free.raw[free.indx,])

my.iap.df$iap.ind <- as.numeric(my.iap.df$usesiap == 1 )

my.iap.df$iap.ind[is.na(my.iap.df$iap.ind)] <-  0

my.start <- list(b0=3.9614320, b1=0.3694081, b2= -0.2588174, th=25.8677287)

iap.model <- nls(log(grossing.position) ~ b0 + b1*log(top.position) + b2*log(price + th*as.numeric(iap.ind)), data=my.iap.df, start=my.start)

iap.estimates <- summary(iap.model)$parameters[,1]

names(iap.estimates) <- row.names(summary(iap.model)$parameters)


iap.ag <- -1/iap.estimates["b2"]

iap.ap <- -1*iap.estimates["b1"]/iap.estimates["b2"]

th <- iap.estimates["th"]


## crude r^2

iap.r2 <- cor(log(my.iap.df$grossing.position),fitted.values(iap.model))^2


## Now, free apps

free.ind <- !is.na(free.raw$top.position) & !is.na(free.raw$grossing.position)

my.free.df <- free.raw[free.ind,]

free.model <- lmrob(log(grossing.position)~log(top.position),data=my.free.df) 

# Correct as per paper
af <- free.model$coefficients[c("log(top.position)")]*ag


## note, using theta (th) computed from the previous stage, following discussion with William
# Correct as per paper
bf <- exp(ag*free.model$coefficients[c("(Intercept)")])*bg/th

script.results <-c(cut.point,Napps,ag,ap,b.ratio,Dt.in, bp, bg,iap.ap, iap.ag, af,th, bf)
names(script.results) <- c("cut.point","Napps","ag","ap","b.ratio","Dt.in",  "bp", "bg","iap.ap", "iap.ag", "af", "th","bf")
write.csv(script.results, pathOutput)
print(names(script.results))
print(c(cut.point,Napps,ag,ap,b.ratio, Dt.in,bp, bg,  iap.ap, iap.ag, af,th, bf))







