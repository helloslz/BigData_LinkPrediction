public class ClassPath2 {
	private int pathLength;
	private double count;
	
	public int getLength() 		{ return pathLength;		}
	public double getCount()	{ return count;			}
	
	public void setLength(int i)		{ pathLength = i;			}
	public void setCount(double d)		{ count = d;			}
	
	/*
	 * returns the String representation of the variable. It is called to write
	 * the reducers output to the HDFS file system, if the output format is TextOutputFormat.
	 */
	public String toString() {
		return pathLength + "\t" + count;
	}
}