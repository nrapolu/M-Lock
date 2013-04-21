package ham.wal.generators;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Random;

public class RandomIdGenerator {	
	static void generateUniformRandIds(long startId, long endId, BufferedWriter bw)	throws IOException {
		Random rand = new Random(0);
	}
	
	static void printUsage() {
		System.out.println("Usage: RandomIdGenerator <Option: 1 -- Uniform, 2 -- NonUniform> " +
				"<StartId> <EndId> <NumOfTrx> <IdsPerTrx> <OutputFile>");
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		if (args.length < 4) {
			printUsage();
			System.exit(-1);
		}

		int option = Integer.parseInt(args[0]);
		long startId = Long.parseLong(args[1]);
		long endId = Long.parseLong(args[2]);
		String outputFile = args[0];
	}
}
