package connector;

import java.io.DataInputStream;
import java.io.IOException;

public class Mongos {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
		    String ls_str;

		    //Process ls_proc = Runtime.getRuntime().exec("D:\\mongodb-win32-i386-2.4.4\\bin\\start_mongos.bat");
		    Process ls_proc = Runtime.getRuntime().exec("D:\\mongodb-win32-i386-2.4.4\\bin\\mongos.exe --port 27017 --logpath D:\\mongodb-win32-i386-2.4.4\\logs\\mongo_mongos.log --configdb 172.22.9.210:30000");

		    // get its output (your input) stream

		    DataInputStream ls_in = new DataInputStream(
	                                          ls_proc.getInputStream());

		    try {
			while ((ls_str = ls_in.readLine()) != null) {
			    System.out.println(ls_str);
			}
		    } catch (IOException e) {
			System.exit(0);
		    }
		} catch (IOException e1) {
		    System.err.println(e1);
		    System.exit(1);
		}

		System.exit(0);
	}

}
