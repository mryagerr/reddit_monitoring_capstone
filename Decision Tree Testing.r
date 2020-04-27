library(ggplot2)
library(rpart)
library(rpart.plot)
library(GGally)
library(dplyr)
library(rattle)
library(rpart.plot)
library(RColorBrewer)
options(digits=2)
setwd("")
dfapplied <- read.csv("gspc_by_hour_test_data.csv", header=TRUE)


targetvalue = "Next_Delta."

for (xl in colnames(dfapplied)){
  if (!(sapply(dfapplied[xl], class)[1][1] == "factor")) {
    dfapplied[xl] <- lapply(dfapplied[xl], as.double)
  } else {
    dfapplied <- dfapplied[, !(colnames(dfapplied) %in% c(colnames(dfapplied[xl])))]
  }}



anova <- rpart(as.formula(paste(targetvalue, ".", sep=" ~ ")), data = dfapplied , method = 'anova')
dfanova <- rpart.rules(anova,style = "tall",cover= 1,nn = 1)

TitleC = paste("Decision Tree for Reddit Data" )
pdf(file= paste(TitleC,".pdf"))
fancyRpartPlot(anova, palettes = c("Reds"), sub="Testing",main = TitleC)
dev.off()


