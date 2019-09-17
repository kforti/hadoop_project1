package gen_data;

import java.io.File;
import java.io.FileWriter;
import java.util.Random;
import java.util.Scanner;


public class GenerateData {

    public void genData() {
        String transactionsPath = "input/transactions.txt";
        String customersPath = "input/customers.txt";

        Customer customer = new Customer();
        try {
            FileWriter tfw = new FileWriter(customersPath);
            for (int i=0; i < 50000; i++) {
                customer.createCustomer();
                tfw.write(customer.allCustomerData[i]);
                //System.out.println(customer.allCustomerData[i]);
            }

            tfw.close();
        }catch(Exception e){System.out.println(e);}

        Transaction transaction = new Transaction();
        File file =
                new File("customer_transaction_numbers.txt");
        try {
            Scanner sc = new Scanner(file);

            FileWriter tfw = new FileWriter(transactionsPath);
            while (sc.hasNextLine()) {
                String line = sc.nextLine();
                try {
                    String[] data = line.split(",");
                    int custid = Integer.parseInt(data[0]);
                    int num_trans = Integer.parseInt(data[1]);
                    for (int i=0; i < num_trans; i++) {
                        String entry = transaction.createTransaction(custid);
                        tfw.write(entry);
                    }

                        //System.out.println(customer.allCustomerData[i]);


                }catch(Exception e){System.out.println(e);}
            }
            tfw.close();
            sc.close();
        } catch (Exception e) {
            System.out.println(e);
        }
    }
    public String getAlphaNumericString(int len_lower, int len_upper)
    {
        int n = (int) (Math.random() * (len_upper - len_lower +1)) + len_lower;

        // chose a Character random from this String
        String AlphaNumericString = "abcdefghijklmnopqrstuvxyz"; //"ABCDEFGHIJKLMNOPQRSTUVWXYZ" +

        // create StringBuffer size of AlphaNumericString
        StringBuilder sb = new StringBuilder(n);

        for (int i = 0; i < n; i++) {

            // generate a random number between
            // 0 to AlphaNumericString variable length
            int charVal = (int)(AlphaNumericString.length() * Math.random());

            // add Character one by one in end of sb
            sb.append(AlphaNumericString.charAt(charVal));
        }

        return sb.toString();
    }

    public class Customer {
        int id = 1;
        String name;
        int age;
        String gender;
        int gender_value;
        int countrycode;
        double salary;
        Random rand = new Random();

        String[] allCustomerData = new String[50000];

        public void createCustomer() {
            age = rand.nextInt((70 - 10 + 1)) + 10;
            name = getAlphaNumericString(10, 20);
            gender_value = rand.nextInt(2);
            if(gender_value == 0) {
                gender = "male";
            }
            else if(gender_value == 1){
                gender = "female";
            }
            countrycode = rand.nextInt(10) + 1;
            salary = rand.nextInt(10000 - 100 + 1) + 100;

            //System.out.println(String.format("%d,%d,%s, %f", id, age, gender, salary));
            String customerData = String.format("%05d,%s,%d,%s,%d,%.2f\n", id, name, age, gender, countrycode, salary);
            allCustomerData[id-1] = customerData;

            id += 1;
        }

    }

    public class Transaction {
        int transid = 1;
        int custid;
        double trans_total;
        int trans_num_items;
        String trans_desc;
        Random rand = new Random();

        public String createTransaction(int custid) {
            //custid = rand.nextInt(50000) + 1;//(int) (Math.random() * 50000) + 1;
            trans_desc = getAlphaNumericString(20, 50);
            trans_total = rand.nextInt((1000 - 10 + 1)) + 10;
            trans_num_items = rand.nextInt(10) + 1;

            String transactionData = String.format("%09d,%05d,%.2f,%d,%s\n", transid, custid, trans_total, trans_num_items, trans_desc);
            transid += 1;
            return transactionData;
        }
    }
}