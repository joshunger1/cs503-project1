import java.io.BufferedWriter;
import java.io.File; // Import the File class
import java.io.FileWriter; // Import the FileWriter class
import java.io.IOException; // Import the IOException class to handle errors

public class Main {

	public static void main(String[] args) {

		//create files for both the customer and transaction datasets
		try {
			File customers = new File("./Data/Customers.txt");
			File transactions = new File("./Data/Transactions.txt");
			if (customers.createNewFile() && transactions.createNewFile()) {
				System.out.println("Files created");
			} else {
				System.out.println("File already exists.");
			}
		} catch (IOException e) {
			System.out.println("An error occurred.");
			e.printStackTrace();
		}


		try {
			FileWriter myWriter = new FileWriter("./Data/filename.txt", true);
			BufferedWriter bw = new BufferedWriter(myWriter);
			bw.write("Spain");
			bw.newLine();
			bw.close();
			System.out.println("Successfully wrote to the file.");
		} catch (IOException e) {
			System.out.println("An error occurred.");
			e.printStackTrace();
		}
	}

}