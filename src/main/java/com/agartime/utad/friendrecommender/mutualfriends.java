package com.agartime.utad.friendrecommender;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.SortedMap;

public class mutualfriends extends SortedMap<Text, Set<Text>> sortedMutualFriends = new TreeMap<Text, Set<Text>> {
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

}
