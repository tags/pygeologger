library(GeoLight)

# Parse command line arguments
options <- commandArgs(trailingOnly = TRUE)
infile <- options[0]
xy <- options[1]

lig <- read.csv(infile, header=T)
trans <- twilightCalc(lig$datetime,lig$light, ask=F)
calib <- subset(trans, as.numeric(trans$tSecond) < as.numeric(strptime("2011-06-25 11:24:30", "%Y-%m-%d %H:%M:%S")))

x=-98.7
y=34.77
elev <- getElevation(calib$tFirst, calib$tSecond, calib$type, known.coord=c(x,y) )

coord <- coord(trans$tFirst,trans$tSecond,trans$type,degElevation=elev)
head(coord)

