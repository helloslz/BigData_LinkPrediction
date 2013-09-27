public class Driver {
	final static double THETA = 0.5;
	public static void main(String[] args) throws Exception {
		String path = args[0];
		int maxLength = Integer.parseInt(args[1]);
		double beta = Double.parseDouble(args[2]);
		int reducerNum = Integer.parseInt(args[3]);
		
		Phase1 p1 = new Phase1();
		p1.run(path, maxLength, beta, reducerNum);

		Phase2 p2 = new Phase2();
		p2.run(path, maxLength, beta, THETA, reducerNum);
	}
}
