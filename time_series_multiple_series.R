# Copyright (C) 2016 Pascal Jürgens and Andreas Jungherr
# See License.txt

# Tutorial: Time Series Analysis - Mentions of Candidates in Twitter

# load library
library(ggplot2)
library(reshape2)
library(scales)

# set working directory
setwd("...") # Put here the name of your working directory, you saved your Twitter data in

# load and prepare data - all messages
message_counts_hourly_df<-data.frame(read.csv("mention_counts_candidates_hour.csv"))

JebBush<-message_counts_hourly_df$jebbush
BenCarson<-message_counts_hourly_df$realbencarson
ChrisChristie<-message_counts_hourly_df$chrischristie
TedCruz<-message_counts_hourly_df$tedcruz
CarlyFiorina<-message_counts_hourly_df$carlyfiorina
MikeHuckabee<-message_counts_hourly_df$govmikehuckabee
JohnKasich<-message_counts_hourly_df$johnkasich
RandPaul<-message_counts_hourly_df$randpaul
MarcoRubio<-message_counts_hourly_df$marcorubio
DonaldTrump<-message_counts_hourly_df$realdonaldtrump

dates_hourly<-message_counts_hourly_df[,1]
dates_hourly<-as.POSIXct(strptime(dates_hourly,"%Y-%m-%d %k"))

plot_all_candidate_mentions_hourly_df<-data.frame(JebBush,
	BenCarson,
	ChrisChristie,
	TedCruz,
	CarlyFiorina,
	MikeHuckabee,
	JohnKasich,
	RandPaul,
	MarcoRubio,
	DonaldTrump)

# plot data

setwd("...") # Put here the name of your working directory, you want to save your plots in

plot_all_candidate_mentions_hourly_melted<-melt(plot_all_candidate_mentions_hourly_df)
plot_all_candidate_mentions_hourly_melted_df<-data.frame(
	date=as.POSIXct(strptime(dates_hourly,"%Y-%m-%d %H")),
	names=factor(plot_all_candidate_mentions_hourly_melted$variable,
		levels=c("JebBush",
		"BenCarson",
		"ChrisChristie",
		"TedCruz",
		"CarlyFiorina",
		"MikeHuckabee",
		"JohnKasich",
		"RandPaul",
		"MarcoRubio",
		"DonaldTrump"), ordered=TRUE,
		labels=c("Jeb Bush",
		"Ben Carson",
		"Chris Christie",
		"Ted Cruz",
		"Carly Fiorina",
		"Mike Huckabee",
		"John Kasich",
		"Rand Paul",
		"Marco Rubio",
		"Donald Trump")),
	mentions_count=plot_all_candidate_mentions_hourly_melted$value
	)

debate_start=as.POSIXct(strptime(c("2015-10-28 17"),"%Y-%m-%d %H"))

plot_all_candidate_mentions_hourly<-ggplot(
	plot_all_candidate_mentions_hourly_melted_df,aes(x=date,y=mentions_count,group=names))+
		geom_line() +
		theme_bw()+
		facet_wrap(~names, nrow=5) +
		theme(axis.text.x=element_text(angle=45,hjust=1))+
		xlab("") +
		scale_x_datetime(
			breaks=date_breaks("1 day"),
			minor_breaks=date_breaks("8 hours"),
			labels=date_format("%m/%e", tz="CET"))+
		geom_vline(xintercept=as.numeric(as.POSIXct(debate_start)),linetype = 2)+
		ylab("Mention Volume, Hourly")

ggsave(file="Candidate mentions, hourly.pdf", width = 6, height = 8)
dev.off()

plot_all_candidate_mentions_hourly_scale_free<-ggplot(
	plot_all_candidate_mentions_hourly_melted_df,aes(x=date,y=mentions_count,group=names))+
		geom_line() +
		theme_bw()+
		facet_wrap(~names, nrow=5, scales = "free_y") +
		theme(axis.text.x=element_text(angle=45,hjust=1))+
		xlab("") +
		scale_x_datetime(
			breaks=date_breaks("1 day"),
			minor_breaks=date_breaks("8 hours"),
			labels=date_format("%m/%e", tz="CET"))+
		geom_vline(xintercept=as.numeric(as.POSIXct(debate_start)),linetype = 2)+
		ylab("Mention Volume, Hourly")

ggsave(file="Candidate mentions, hourly (scale free).pdf", width = 6, height = 8)
dev.off()

##########################################################################################
##########################################################################################