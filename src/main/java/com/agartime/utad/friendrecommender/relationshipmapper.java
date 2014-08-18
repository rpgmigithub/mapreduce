package com.agartime.utad.friendrecommender;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;


public class relationshipmapper extends Mapper<Text, Text, Text, friendcount> {
	  private Text word = new Text();

	  @Override
	  public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
	   String line[] = value.toString().split("\\s+");
	   Text fromUser = line[0];
	   List<Text> toUsers = new ArrayList<Text>();

	   if (line.length == 2) {
	    StringTokenizer tokenizer = new StringTokenizer(line[1], ",");
	    while (tokenizer.hasMoreTokens()) {
	     Text toUser = (tokenizer.nextToken().trim());
	     toUsers.add(toUser);
	     context.write(new Text(fromUser), new friendcount(toUser, -1L));
	    }

	    for (int i = 0; i < toUsers.size(); i++) {
	     for (int j = i + 1; j < toUsers.size(); j++) {
	      context.write(new Text(toUsers.get(i)), new friendcount((toUsers.get(j)), fromUser));
	      context.write(new Text(toUsers.get(j)), new friendcount((toUsers.get(i)), fromUser));
	     }
	    }
	   }
	  }
	 }
