# Copyright (C) 2016 Pascal Jürgens and Andreas Jungherr
# See License.txt

# Tutorial: Time Series Analysis - Singe Series
# All messages & #gopdebate

# load library
library(ggplot2)
library(scales)

##########################################################################################
##########################################################################################

# set working directory
setwd("...") # Put here the name of your working directory, you saved your Twitter data in

# load and prepare data - all messages
message_counts_df<-data.frame(read.csv("total_counts_day.csv"))

dates<-as.POSIXct(message_counts_df[,1])
all_messages<-message_counts_df[,2]

plot_all_messages_df<-data.frame(dates,all_messages)

# plot data

setwd("...") # Put here the name of your working directory, you want to save your plots in

plot_all_messages<-ggplot(plot_all_messages_df,aes(x=dates,all_messages))+
	geom_line(stat="identity") +
	geom_point(size=2)+
	theme_bw()+
	xlab("")+
	ylim(0, 116000)+
	theme(axis.text.x=element_text(angle=45,hjust=1))+
	ylab("Message Volume, Daily")

ggsave(file="Message Volume Daily.pdf", width = 170, height = 90, unit="mm", dpi=300)
dev.off()

##########################################################################################
##########################################################################################

setwd("...") # Put here the name of your working directory, you saved your Twitter data in

gopdebate_counts_df<-data.frame(read.csv("hashtag_counts_gop_day.csv"))
gopdebate_dates<-as.POSIXct(gopdebate_counts_df[,1])
gopdebate_mentions<-gopdebate_counts_df[,2]

plot_gopdebate_df<-data.frame(gopdebate_dates,gopdebate_mentions)

# plot data

setwd("...") # Put here the name of your working directory, you want to save your plots in

plot_gopdebate<-ggplot(plot_gopdebate_df,aes(x=gopdebate_dates,gopdebate_mentions))+
	geom_line(stat="identity") +
	geom_point(size=2)+
	theme_bw()+
	xlab("")+
	ylim(0, 20000)+
	theme(axis.text.x=element_text(angle=45,hjust=1))+
	ylab("#GOPDebate, Daily")

ggsave(file="#gopdebate Volume Daily.pdf", width = 170, height = 90, unit="mm", dpi=300)
dev.off()

##########################################################################################
##########################################################################################

# set working directory
setwd("...") # Put here the name of your working directory, you saved your Twitter data in

# load and prepare data - all messages
message_counts_hourly_df<-data.frame(read.csv("total_counts_hour.csv"))

dates_hourly<-as.POSIXct(strptime(message_counts_hourly_df[,1],"%Y-%m-%d %k"))
all_messages_hourly<-message_counts_hourly_df[,2]

plot_all_messages_hourly_df<-data.frame(dates_hourly,all_messages_hourly)

# plot data

setwd("...") # Put here the name of your working directory, you want to save your plots in

plot_all_messages_hourly<-ggplot(plot_all_messages_hourly_df,aes(x=dates_hourly,all_messages_hourly))+
	geom_line(stat="identity") +
	geom_point(size=2)+
	theme_bw()+
	xlab("")+
	ylim(c(0,5000))+
	theme(axis.text.x=element_text(angle=45,hjust=1))+
	ylab("Message Volume, Hourly")

ggsave(file="Message Volume Hourly.pdf", width = 170, height = 90, unit="mm", dpi=300)
dev.off()

##########################################################################################
##########################################################################################

# set working directory
setwd("...") # Put here the name of your working directory, you saved your Twitter data in

gopdebate_counts_h_df<-data.frame(read.csv("hashtag_counts_gop_hour.csv"))
gopdebate_dates_h<-as.POSIXct(strptime(gopdebate_counts_h_df[,1],"%Y-%m-%d %k"))
gopdebate_mentions_h<-gopdebate_counts_h_df[,2]

plot_gopdebate_hourly_df<-data.frame(gopdebate_dates_h,gopdebate_mentions_h)

# plot data

setwd("...") # Put here the name of your working directory, you want to save your plots in

plot_gopdebate_hourly<-ggplot(plot_gopdebate_hourly_df,aes(x=gopdebate_dates_h,gopdebate_mentions_h))+
	geom_line(stat="identity") +
	geom_point(size=2)+
	theme_bw()+
	xlab("")+
	ylim(c(0,3000))+
	theme(axis.text.x=element_text(angle=45,hjust=1))+
	ylab("#GOPDebate, Hourly")

ggsave(file="#gopdebate Volume Hourly.pdf", width = 170, height = 90, unit="mm", dpi=300)
dev.off()

##########################################################################################
##########################################################################################

# set working directory
setwd("...") # Put here the name of your working directory, you saved your Twitter data in

featureless_counts_h_df<-data.frame(read.csv("featureless_counts_hour.csv"))
featureless_dates_h<-as.POSIXct(strptime(featureless_counts_h_df[,1],"%Y-%m-%d %k"))
featureless_mentions_h<-featureless_counts_h_df[,2]

plot_featureless_hourly_df<-data.frame(featureless_dates_h,featureless_mentions_h)

# plot data

setwd("...") # Put here the name of your working directory, you want to save your plots in

plot_featureless_hourly<-ggplot(plot_featureless_hourly_df,aes(x=featureless_dates_h,featureless_mentions_h))+
	geom_line(stat="identity") +
	geom_point(size=2)+
	theme_bw()+
	xlab("")+
	ylim(c(0,6500))+
	theme(axis.text.x=element_text(angle=45,hjust=1))+
	ylab("Featureless, Hourly")

ggsave(file="Featureless Volume Hourly.pdf", width = 170, height = 90, unit="mm", dpi=300)
dev.off()

##########################################################################################
##########################################################################################