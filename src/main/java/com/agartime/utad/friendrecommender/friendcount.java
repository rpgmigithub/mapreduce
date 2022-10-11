package com.agartime.utad.friendrecommender;

import org.apache.hadoop.io.*;
import java.io.system;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class friendcount implements Writable {
	  public Text user;
	  public Text mutualFriend;

	  public friendcount(Text user, Text mutualFriend) {
	   this.user = user;
	   this.mutualFriend = mutualFriend;
	  }

	  public friendcount() {
	   this(-1L, -1L);
	  }

	  @Override
	  public void write(DataOutput out) throws IOException {
	   out.writeLong(user);
	   out.writeLong(mutualFriend);
	  }

	  @Override
	  public void readFields(DataInput in) throws IOException {
	   user = in.readLong();
	   mutualFriend = in.readLong();
	  }

	  @Override
	  public String toString() {
	   return " toUser: "
	     + Long.toString(user) + " mutualFriend: " + Long.toString(mutualFriend);
	  }

}
