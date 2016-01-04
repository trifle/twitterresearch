# Performing network analysis in R
# Since the network analysis library igraph is available
# both in R and python, all the following analyses
# can be performed in either language. However, installing
# igraph is much easier in R.

# Install and load igraph network analysis package
install.packages("igraph")
library(igraph)

###############
# Retweets
###############

# Load GraphML network file
# If the file is not yet present, generate it using
# the retweet_network() function in network.py
rt.net <- read.graph("retweets.graphml", format="graphml")

# Summary of the network
summary(rt.net)
# Should give the following or similar information:
# IGRAPH D--- 168617 291450 -- 
# + attr: label (v/c), user_id (v/c), id (v/c), id (e/c)
# This represents a directed network with 168617 users and 
# 291450 edges, that is retweet connections

# Edges and nodes (here called vertices) can be accessed like this:
V(rt.net)
# shows a list of vertex IDs
E(rt.net)
# shows a list of edges with source ID-> target ID notation

# Finding Donald Trump
trump <- which(V(rt.net)$label == "realDonaldTrump")
# Trump degree
degree(rt.net, v=trump)
# Trump indegree
degree(rt.net, v=trump, mode="in")

# Let's write the degree as a node attribute:
V(rt.net)$degree <- degree(rt.net)
# The same can be done for in-degree:
V(rt.net)$indegree <- degree(rt.net, mode="in")

# Inspect the degree distribution in a log-log plot
plot(degree.distribution(rt.net, mode="in"), log="xy")

# Sort users by indegree
V(rt.net)[order(-indegree)]

# Find the top 10 users by indegree and print their names
V(rt.net)[order(-indegree)]$label[1:10]

# Analyze a small subset of the graph
# For convenience, we use the top 25 nodes by indegree
# The graph could be defined through any other means

# Get top 25 nodes
top25.nodes <- V(rt.net)[order(-indegree)][1:25]
# Create a small graph by deleting all nodes except top 25
small.rt.net <- delete.vertices(rt.net, which(!V(rt.net) %in% top25.nodes))
# Layout with Fruchterman Rheingold algorithm (works nicely for small graphs)
l <- layout_with_fr(small.rt.net)
# Plot with labels
plot(small.rt.net, l)

# Same for betweenness centrality:
V(rt.net)$betweenness <- betweenness(rt.net)
top25.nodes <- V(rt.net)[order(-betweenness)][1:25]
small.rt.net <- delete.vertices(rt.net, which(!V(rt.net) %in% top25.nodes))
l <- layout_with_fr(small.rt.net)
plot(small.rt.net, l)

# Same for incoming closeness centrality:
V(rt.net)$closeness <- closeness(rt.net, mode="in")
top25.nodes <- V(rt.net)[order(-closeness)][1:25]
small.rt.net <- delete.vertices(rt.net, which(!V(rt.net) %in% top25.nodes))
l <- layout_with_fr(small.rt.net)
plot(small.rt.net, l)


###############
# Replies
###############

# Steps identical to above:

reply.net <- read.graph("replies.graphml")

summary(reply.net)
V(reply.net)$indegree <- degree(reply.net, mode="in")
V(reply.net)[order(-indegree)]$label[1:10]
V(reply.net)$betweenness <- betweenness(reply.net)
V(reply.net)$closeness <- closeness(reply.net, mode="in")
V(reply.net)$closeness.out <- closeness(reply.net, mode="out")

top25.nodes <- V(reply.net)[order(-closeness)][1:25]
small.reply.net <- delete.vertices(reply.net, which(!V(reply.net) %in% top25.nodes))
l <- layout_with_fr(small.reply.net)
plot(small.reply.net, l)
