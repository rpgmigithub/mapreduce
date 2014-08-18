package com.agartime.utad.friendrecommender;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;


public class relationshipreducer extends Reducer<Text, friendcount, Text, Text> {
	  @Override
	  public void reduce(Text key, Iterable<friendcount> values, Context context)
	    throws IOException, InterruptedException {

	   final java.util.Map<Long, Set<Long>> mutualFriends = new HashMap<Long, Set<Long>>();

	   for (friendcount val : values) {
	    final Boolean isAlreadyFriend = (val.mutualFriend == -1);
	    final Long toUser = val.user;
	    final Long mutualFriend = val.mutualFriend;

	    if (mutualFriends.containsKey(toUser)) {
	     if (isAlreadyFriend) {
	      mutualFriends.put(toUser, null);
	     } else if (mutualFriends.get(toUser) != null) {
	      mutualFriends.get(toUser).add(mutualFriend);
	     }
	    } else {
	     if (!isAlreadyFriend) {
	      mutualFriends.put(toUser, new HashSet<Long>() {
	       {
	        add(mutualFriend);
	       }
	      });
	     } else {
	      mutualFriends.put(toUser, null);
	     }
	    }

}
	   
	   java.util.SortedMap<Long, Set<Long>> sortedMutualFriends = new TreeMap<Long, Set<Long>>(new Comparator<Long>() {
		    @Override
		    public int compare(Long key1, Long key2) {
		     Integer v1 = mutualFriends.get(key1).size();
		     Integer v2 = mutualFriends.get(key2).size();
		     if (v1 > v2) {
		      return -1;
		     } else if (v1.equals(v2) && key1 < key2) {
		      return -1;
		     } else {
		      return 1;
		     }
		    }
		   });

		   for (java.util.Map.Entry<Long, Set<Long>> entry : mutualFriends.entrySet()) {
		    if (entry.getValue() != null) {
		     sortedMutualFriends.put(entry.getKey(), entry.getValue());
		    }
		   }

		   Integer i = 0;
		         String output = "";
		         Set<Long> entrySet = new HashSet<>();
		   for (java.util.Map.Entry<Long, Set<Long>> entry : sortedMutualFriends.entrySet()) {
		    entrySet.add(entry.getKey());
		             entrySet.addAll(entry.getValue());            
		   }
		   Iterator<Long> setItr = entrySet.iterator();
		   while(setItr.hasNext()){
		    if(i==0)
		     output+=setItr.next();
		    else
		     output+="\t"+setItr.next();
		    
		    ++i;
		   }

		  context.write(key, new Text(output));
		 }
