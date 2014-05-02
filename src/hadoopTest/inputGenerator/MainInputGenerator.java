package hadoopTest.inputGenerator;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;
/*
 * Generates random input with tuple => Year, Caller, Callee
 */
public class MainInputGenerator {

	public static void main(String[] args) {
		String fileName = "File";
		Random rand = new Random();

		try {

			for (int fi = 0; fi < 10; fi++) 
			{
				File file = new File(fileName + fi);
				FileOutputStream fop = new FileOutputStream(file);

				if (!file.exists()) {
					file.createNewFile();
				}

				for (int row = 0; row < 1000; row++) {
					String strRow = "";
					strRow += 2010 + rand.nextInt(10) + ",";
					char caller = (char) ('A' + rand.nextInt(10));
					char callee = (char) ('A' + rand.nextInt(10));
					strRow += caller + "," + callee + "\n";

					fop.write(strRow.getBytes());									
				}

				fop.flush();
				fop.close();							
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
