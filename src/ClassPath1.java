
import java.util.ArrayList;
import java.util.Iterator;

public class ClassPath1 {
	private String nodeId;
	private int length;
	private double count;
	private int direction;
	private ArrayList<String> previousNodeIds;
	
	public ClassPath1() {
		
	}
	
	public ClassPath1(ClassPath1 cp1) {
		nodeId = cp1.nodeId;
		length = cp1.length;
		count = cp1.count;
		direction = cp1.direction;
		previousNodeIds = new ArrayList<String>(cp1.previousNodeIds);
	}
	
	public String getNodeId() 		{ return nodeId; 		}
	public int getLength() 		{ return length;		}
	public double getCount()	{ return count;			}
	public int getDirection()	{ return direction;		}
	public ArrayList<String> getPreviousNodeIds()	{ return previousNodeIds;	}
	
	public void setNodeId(String s)	 	{ nodeId = s;			}
	public void setLength(int i)		{ length = i;			}
	public void setCount(double d)		{ count = d;			}
	public void setDirection(int i)	{ direction = i;		}
	public void setPreviousNodeIds(ArrayList<String> al)	{ previousNodeIds = new ArrayList<String>(al);	}
	
	/* 
	 * adds a node identity to the list of the intermediate nodes of a path. 
	 * this method is called at the construction and expansion of a path.
	 */
	public void addPreviousNodeId(String s) {
		previousNodeIds.add(s);
	}
	
	/*
	 * adds to the variable new intermediate nodes of a new path.
	 * It is called when two paths are concatenated to one.
	 */
	public void addPreviousNodeIds(ArrayList<String> al) {
		previousNodeIds.addAll(al);
	}
	
	/*
	 * returns the String representation of the variable. It is called to write
	 * the reducers output to the HDFS file system, if the output format is TextOutputFormat.
	 */
	public String toString() {
		StringBuilder sb = new StringBuilder();
		Iterator<String> iter = previousNodeIds.iterator();
		while(iter.hasNext()) {
			sb.append(iter.next() + ",");
		}
		return nodeId + "\t" + length + "\t" + count + "\t" + direction + "\t" + sb.toString();
	}
	
	public boolean equals(ClassPath1 cp) {
		boolean result = true;
		result = result && this.nodeId.equals(cp.nodeId);
		result = result && (this.length == cp.length);
		result = result && (this.count == cp.count);
		result = result && (this.direction == cp.direction);
		result = result && this.previousNodeIds.containsAll(cp.previousNodeIds) && 
				cp.previousNodeIds.containsAll(this.previousNodeIds);
		
		return result;
	}
}
