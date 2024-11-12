
### 1 Données pour le problème et démarrage

library(rmr2) # Charge RHadoop
rmr.options(backend = "local")
load("Exam_RHadoop_2022.Rdata")
summary(dfexam)
head(dfexam)
sfbg <- to.dfs(dfexam) 
nbblocsdf <- function(bg) {
  mapft = function(k,v) {keyval(dim(v)[1], dim(v)[2])}
  out <- mapreduce(input = bg,
                   map = mapft,
                   verbose = FALSE)
  res <- from.dfs(out)
  cat("Hadoop splits in", length(res$val), "blocs\n")
  cat("# of rows:", res$key, "\n")
  cat("# of cols:", res$val)
}



### 2 Questions

#1
nbblocsdf(sfbg) #4 blocs de dim 11354*4 11354*4 11354*4 938*4 

#2

mapft = function(k,v){keyval(key=v[,4],val=1)}
redft=function(k,v){keyval(key=k,val=sum(v))}
mr=mapreduce(sfbg,
             map = mapft,
             reduce = redft)
res=from.dfs(mr)
n=sum(res$val)

data.frame(table(dfexam[,4]))

#3

mapft = function(k,v){
  keyval(key=1,
         val=data.frame(
           sx=sum(v[,1]),sy=sum(v[,2]),
           sx2=sum(v[,1]^2),sy2=sum(v[,2]^2),
           minx=min(v[,1]),miny=min(v[,2]),
           maxx=max(v[,1]),maxy=max(v[,2]),
           n=length(v[,1]))
  )
  }
redft=function(k,v){keyval(key=k,val=sum(v))}
mr=mapreduce(sfbg,
             map = mapft)
res=from.dfs(mr)
res

#4
summary(dfexam)
head(dfexam[,c(1,3)])

#EN GROS TE COMPLIQUE PAS LA VIE VA AU PLUS SIMPLE
mapft = function(k,v){keyval(key=v[,3],
                             val=v[,1])}
redft=function(k,v){
  keyval(key=k,
         val=c(mean(v),sd(v)))}

mr=mapreduce(sfbg,
             map = mapft,
             reduce = redft
             )
res=from.dfs(mr)
res
head(res)
tapply(dfexam[,1], dfexam[,3],mean)

#5

mapft = function(k,v){keyval(key=1,
                             val=data.frame(
                                   s = sum(v[,2]),
                                   s2 = sum(v[,2]^2),
                                   s3 = sum(v[,2]^3),
                                   n = length(v[,2]))
)}

#Mais en fait on étais pas obligé de faire ça dans un reduce
redft=function(k,v){
  keyval(key=k,
         val=sum(v$s3)/sum(v$n) - sum(v$s2)*2/sum(v$n)*sum(v$s)/sum(n) #Flemme
         )}

mr=mapreduce(sfbg,
             map = mapft
)
res=from.dfs(mr)
res


#6
hist(dfexam[,2],plot = F)$density
cumsum(hist(dfexam[,2],plot = F,nclass=500)$density)





