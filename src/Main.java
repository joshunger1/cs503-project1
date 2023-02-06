import java.io.BufferedWriter;
import java.io.File; // Import the File class
import java.io.FileWriter; // Import the FileWriter class
import java.io.IOException; // Import the IOException class to handle errors
import java.util.Random;

public class Main {

	public static void main(String[] args) {

		// create files for both the customer and transaction datasets
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

		for (int j = 0; j < 50000; j++) {
			try {
				FileWriter myWriter = new FileWriter("./Data/Customers.txt", true);
				BufferedWriter bw = new BufferedWriter(myWriter);

				// generate customer name
				int RanNum = new Random().nextInt(20 - 10 + 1) + 10;
				int leftLimit = 48; // numeral '0'
				int rightLimit = 122; // letter 'z'
				Random random = new Random();

				String generatedString = random.ints(leftLimit, rightLimit + 1)
						.filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
						.limit(RanNum)
						.collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
						.toString();

				// generate customer age
				int RanCustomerAge = new Random().nextInt(70 - 10 + 1) + 10;

				// generate customer gender
				String[] genders = { "male", "female" };
				int RanCustomerGender = new Random().nextInt(1 - 0 + 1) + 0;
				String CustomerGender = genders[RanCustomerGender];

				// generate customer country
				int RanCustomerCountry = new Random().nextInt(10 - 1 + 1) + 1;

				// generate customer country
				Float RanCustomerSalary = new Random().nextFloat() * (10000 - 100) + 100;

				String customerString = Integer.toString(j+1) + "," + generatedString + "," + RanCustomerAge + ","
						+ CustomerGender + "," + RanCustomerCountry + "," + Float.toString(RanCustomerSalary);
				bw.write(customerString);
				bw.newLine();
				bw.close();
				System.out.println("Successfully wrote to the file x" + j);
			} catch (IOException e) {
				System.out.println("An error occurred.");
				e.printStackTrace();
			}
		}
		for (int k = 0; k < 5000000; k++) {
			try {
				FileWriter myWriter = new FileWriter("./Data/Transactions.txt", true);
				BufferedWriter bw = new BufferedWriter(myWriter);

				// generate customerID number
				int RanCustomerID = new Random().nextInt(50000 - 1 + 1) + 1;

				// generate transaction total
				Float RanTransactionTotal = new Random().nextFloat() * (1000 - 10) + 10;

				// generate trasaction number of items
				int RanNumItems = new Random().nextInt(10 - 1 + 1) + 1;

				// generate transaction description
				int RanNum = new Random().nextInt(50 - 20 + 1) + 20;
				int leftLimit = 48; // numeral '0'
				int rightLimit = 122; // letter 'z'
				Random random = new Random();

				String description = random.ints(leftLimit, rightLimit + 1)
						.filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
						.limit(RanNum)
						.collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
						.toString();

				String customerString = Integer.toString(k+1) + "," + RanCustomerID + "," + RanTransactionTotal + ","
						+ RanNumItems + "," + description;
				bw.write(customerString);
				bw.newLine();
				bw.close();
				//System.out.println("Successfully wrote to the file x" + k);
			} catch (IOException e) {
				System.out.println("An error occurred.");
				e.printStackTrace();
			}
		}

	}

}