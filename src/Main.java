import java.io.BufferedWriter;
import java.io.File; 
import java.io.FileWriter; 
import java.io.IOException; 
import java.util.Random;

public class Main {

	public static void main(String[] args) {

		// create files for both the customer and transaction datasets
		try {
			File customers = new File("./Data/Customers.txt"); //create the text file for the customer data
			File transactions = new File("./Data/Transactions.txt"); //create the text file for the transaction data 
			
			//check if files are already created
			if (customers.createNewFile() && transactions.createNewFile()) {
				System.out.println("Files created");
			} else {
				System.out.println("File already exists.");
			}
		} catch (IOException e) { //catch any io errors
			System.out.println("An error occurred.");
			e.printStackTrace();
		}

		//generate the customer data
		for (int j = 0; j < 50000; j++) {
			try {
				//create an object to write to the file, allow appending incase method crashing so we dont need to restart the data creation entirely
				FileWriter myWriter = new FileWriter("./Data/Customers.txt", true);
				BufferedWriter bw = new BufferedWriter(myWriter);

				// generate customer name
				int RanNum = new Random().nextInt(20 - 10 + 1) + 10; //the length of the string
				int leftLimit = 48; // numeral '0'
				int rightLimit = 122; // letter 'z'
				Random random = new Random();

				//generate a random alphanumeric string between the range spefified above
				String generatedString = random.ints(leftLimit, rightLimit + 1)
						.filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
						.limit(RanNum)
						.collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
						.toString();

				// generate customer age
				int RanCustomerAge = new Random().nextInt(70 - 10 + 1) + 10;

				// generate customer gender
				String[] genders = { "male", "female" };
				int RanCustomerGender = new Random().nextInt(1 - 0 + 1) + 0; //get a random number between 1 and 0 which will correlate to the index of the gender array
				String CustomerGender = genders[RanCustomerGender]; 

				// generate customer country
				int RanCustomerCountry = new Random().nextInt(10 - 1 + 1) + 1;

				// generate customer country
				Float RanCustomerSalary = new Random().nextFloat() * (10000 - 100) + 100;

				//append all the parts of the customer string together
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
		//generate all the transaction data
		for (int k = 0; k < 5000000; k++) {
			try {
				//generate a filewriter for the transaction dataset
				FileWriter myWriter = new FileWriter("./Data/Transactions.txt", true);
				BufferedWriter bw = new BufferedWriter(myWriter);

				// generate customerID number
				int RanCustomerID = new Random().nextInt(50000 - 1 + 1) + 1;

				// generate transaction total
				Float RanTransactionTotal = new Random().nextFloat() * (1000 - 10) + 10;

				// generate trasaction number of items
				int RanNumItems = new Random().nextInt(10 - 1 + 1) + 1;

				// generate transaction description
				int RanNum = new Random().nextInt(50 - 20 + 1) + 20; //number of characters to generate
				int leftLimit = 48; // numeral '0'
				int rightLimit = 122; // letter 'z'
				Random random = new Random();

				//genereate a alphanumeric string as per the filters above
				String description = random.ints(leftLimit, rightLimit + 1)
						.filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
						.limit(RanNum)
						.collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
						.toString();

				//append all the transacion components into a single string to write to the file
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