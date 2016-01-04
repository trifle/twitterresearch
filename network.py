#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Network Extraction
------------------

Data Formats
============
Networks can be expressed in completely different ways such as an adjacency matrix or a list of edges. Not surprisingly, one can choose among many file formats for expressing, storing and transferring network data. The documentation of the open source graphical network analysis tool gephi has a very useful overview over the features and complexity of some of the most common formats. Usually, we prefer to use GraphML which is an XML-based representation that allows storing attributes of both nodes and edges and is well-supported across different software. The excellent igraph python library could easily produce and read GraphML along with many other formats, but can be cumbersome to install.
In order to keep the examples simple and readable, we opted for a plain text format that allows attributes as well: GML. GML files have a simple header, followed by a list of nodes and edges. However, during development we ran into problems with igraph for R not reading the produced GML files, so we needed to write a simple GraphML writer as well. Since GraphML uses XML, we chose python's built-in etree library for handling that part. Here are two quick examples for both formats:

GML (from its wikipedia page):

graph [
    directed 1
    node [
        id 1
        label "node 1"
        thisIsASampleAttribute 42
    ]
    node [
        id 2
        label "node 2"
        thisIsASampleAttribute 43
    ]
    edge [
        source 1
        target 2
        label "Edge from node 1 to node 2"
    ]
]

And the corresponding GraphML file:

<?xml version="1.0" encoding="UTF-8"?>
<graphml xmlns="http://graphml.graphdrawing.org/xmlns">
<key attr.name="label" attr.type="string" for="node" id="label"/>
<key attr.name="Edge Label" attr.type="string" for="edge" id="edgelabel"/>
<key attr.name="Edge Id" attr.type="string" for="edge" id="edgeid"/>
<key attr.name="thisIsASampleAttribute" attr.type="string" for="node" id="thisIsASampleAttribute"/>
<graph edgedefault="directed">
<node id="1">
<data key="label">node 1</data>
<data key="thisIsASampleAttribute">42</data>
</node>
<node id="2">
<data key="label">node 2</data>
<data key="thisIsASampleAttribute">43</data>
</node>
<edge source="1" target="2">
<data key="edgelabel">Edge from node 1 to node 2</data>
</edge>
</graph>
</graphml>


Overview of network file formats from the gephi project: [https://gephi.github.io/users/supported-graph-formats/]
GML specification: [http://www.fim.uni-passau.de/fileadmin/files/lehrstuhl/brandenburg/projekte/gml/gml-technical-report.pdf]
GML on wikipedia: [https://en.wikipedia.org/wiki/Graph_Modelling_Language]

Usage
=====
Since we cannot vouch for the GML format to work, we recommend creating GraphML network files. The functions below should provide

"""

import logging
import database
import peewee
import xml.etree.ElementTree as ET


def write_gml_element(element, name):
    """
    Write GLM element containing key-value pairs surrounded by square brackets
    """
    lines = ["\t{0} [\n".format(name)]
    # write all key value pairs
    for k, v in sorted(element.items()):
        lines.append("\t\t{0} {1}\n".format(k, v))
    # close bracket
    lines.append("\t]\n")
    return lines


def write_gml(nodes, edges, outfile="network.gml"):
    """
    Helper function to write GML-formatted files.
    Requires pre-assembled data in the form of:
    nodes = [{"id": 2, "label": "node 2"},]
    edges = [{"source": 1, "target": 2},]

    This function, along with the helper write_gml_element above,
    is essentially an implementation of the example writer from
    the GML specification.
    """
    with open(outfile, "w") as f:
        f.write("graph [\n")
        # we assume graphs are always directed
        f.write("directed 1\n")
        for node in nodes:
            f.writelines(write_gml_element(node, 'node'))
        for edge in edges:
            f.writelines(write_gml_element(edge, 'edge'))
        f.write("]\n")





def write_graphml_element(graph, attributes, name, data={}):
    """
    Add an element to a graphml graph.
    Elements should be in the format:
    {
    "name": "elementname",
    "element": {"attribute": "value", ...}
    }
    """
    e = ET.SubElement(graph, name)
    for k, v in attributes.items():
        e.set(str(k), str(v))
    for k, v in data.items():
        data = ET.SubElement(e, 'data')
        data.set('key', str(k))
        data.text = str(v)


def write_graphml_file(nodes, edges, filename="network.graphml"):
    """
    Create a GraphML file from network components:
    nodes and edges.

    Note that this is only a tiny writer function and does not implement
    the GraphML format in full! There is no guarantee this will work
    See http://graphml.graphdrawing.org/specification.html
    for the GraphML specification and http://graphml.graphdrawing.org/primer/graphml-primer.html
    for an introduction.
    """
    # Setup the graphml header defining the format
    root = ET.Element("graphml")
    root.set("xmlns", "http://graphml.graphdrawing.org/xmlns")
    root.set("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
    root.set(" xsi:schemaLocation", "http://graphml.graphdrawing.org/xmln;")
    # Set graph attributes.
    # We assume we have IDs for everything (required),
    # labels for nodes and all edges are directed
    label_def = ET.SubElement(root, "key")
    label_def.set("attr.name", "label")
    label_def.set("attr.type", "string")
    label_def.set("for", "node")
    label_def.set("id", "label")

    user_id_def = ET.SubElement(root, "key")
    user_id_def.set("attr.name", "user_id")
    user_id_def.set("attr.type", "string")
    user_id_def.set("for", "node")
    user_id_def.set("id", "user_id")

    graph = ET.SubElement(root, "graph")
    graph.set("id", "G")
    graph.set("edgedefault", "directed")
    # Setup end
    # Write nodes
    node_id_map = {}
    for i, node in enumerate(nodes.items()):
        # Store the new ID given to the node so we can reference it later
        # in the edges
        node_id_map[node[0]] = "n{0}".format(i)
        attributes = {'name': 'node',
                      'id': "n{0}".format(i),
                      }
        data = {
            'user_id': node[0],
            'label': node[1],
        }
        write_graphml_element(
            graph=graph, attributes=attributes, data=data, name="node")
    for j, edge in enumerate(edges):
        attributes = {'name': 'edge',
                      'id': "e{0}".format(j),
                      'source': node_id_map[edge[0]],
                      'target': node_id_map[edge[1]],
                      }
        write_graphml_element(
            graph=graph, attributes=attributes, name="edge")
    # Write file
    tree = ET.ElementTree(root)
    tree.write(filename, encoding='utf-8', xml_declaration=True)


def retweet_links():
    """
    Find all links defined by replies (directed messages starting with an user name: @pascal ....
    From our models, this looks like: User -> Tweet -> is reply to: User
    This function is not maximally efficient but readable.
    """
    nodes = {}
    edges = []
    # The following query finds all reply links, disregarding their frequency.
    # It works like this:
    # Define aliases for the secondary meanings of Tweet and User, namely
    # Tweet as the origintal Tweet of a Retweet and User as the original author
    rt = database.Tweet.alias()
    rtu = database.User.alias()
    # Construct the query by starting with select. We use round brackets to
    # allow the query to span multiple lines with comments
    retweets = (
        database.Tweet.select().
        # Join in the User of the Retweet by linking the ID field
        join(database.User, on=(database.Tweet.user == database.User.id)).
        # Switch the context back to Tweet, since we want to add more joins
        switch(database.Tweet).
        # Join in the Tweet that was retweeted by its ID, using the alias
        join(rt, on=(database.Tweet.retweet == rt.id)).
        # Now switch the context to the original Tweet, so we can add its user
        switch(rt).
        # Join in the User of the original Tweet using the alias
        join(rtu, on=(rt.user == rtu.id)).
        # Switch the context back to our original Tweet
        switch(database.Tweet).
        # Group, ie deduplicate lines that share the same (retweeting user -> original author) pair
        group_by(database.User, rtu)
        )
    for retweet in retweets:
        nodes[retweet.user.id] = retweet.user.username
        nodes[retweet.retweet.user.id] = retweet.retweet.user.username
        edges.append((retweet.user.id, retweet.retweet.user.id))
    write_graphml_file(nodes=nodes, edges=edges, filename="retweets.graphml")


def reply_links():
    """
    Find all links defined by replies (directed messages starting with an user name: @pascal ....
    From our models, this looks like: User -> Tweet -> is reply to: User

    """
    # Set up container variables for writing the network file
    nodes = {}
    edges = []
    # The following query finds all reply links, disregarding their frequency.
    # First, it defines an alias for the user that was replied to in order to distinguish it from the tweet's author.
    # Then, it joins both the author and adressee User objects. Finally, it groups by both users, yielding
    # only one entry per tweet author -> adressee pair.
    reply_user = database.User.alias()
    replies = (
        database.Tweet.select().
        where(database.Tweet.reply_to_user.is_null(False)).
        join(database.User, on=(database.Tweet.user == database.User.id)).
        switch(database.Tweet).
        join(reply_user, on=(database.Tweet.reply_to_user == reply_user.id)).
        switch(database.Tweet).
        group_by(database.User, reply_user)
        )
    print(replies.count())
    for i, reply in enumerate(replies):
        nodes[reply.user.id] = reply.user.username
        nodes[reply.reply_to_user.id] = reply.reply_to_user.username
        edges.append(
            (reply.user.id, reply.reply_to_user.id)
            )
    write_graphml_file(nodes=nodes, edges=edges, filename="replies.graphml")
