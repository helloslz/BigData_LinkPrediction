import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

public class Predict {
	static double threshold = 0.5;
	public static void main(String[] args) throws NumberFormatException, IOException {
		ArrayList<String> prediction = new ArrayList<String>();
		HashSet<String> test = new HashSet<String>();
		
		BufferedReader br = new BufferedReader(new FileReader("w:/part-00000"));
		String line;
		String[] lineInfo;
		// read prediction data into an ArrayList
		while((line = br.readLine()) != null) {
			lineInfo = line.split("\t");
			if(Double.parseDouble(lineInfo[2]) >= threshold) {
				prediction.add(lineInfo[0] + "\t" + lineInfo[1]);
			}
		}
		br.close();
		System.out.println("reading prediciton data finished.");
		
		// read test data into an ArrayList
		br = new BufferedReader(new FileReader("w:/test.txt"));
		while((line = br.readLine()) != null) {
			test.add(line);

		}
		br.close();
		System.out.println("reading test data finished.");
		
		int right = 0;
		// compute prediction and recall
		for(String s : prediction) {
			if(test.contains(s))
				right ++;
		}
		System.out.println("Precision is " + (double)right / prediction.size());
		System.out.println("Recall is " + (double)right / test.size());
	}
}
