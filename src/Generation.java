import java.io.*;
import java.util.regex.Pattern;

public class Generation {
    public static void main(String[] args) throws IOException {
        long start = System.currentTimeMillis();
//        danmu();
        users();
//        videos();
        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }

    public static void danmu() throws IOException {
        File inputFile = new File("src/danmu.csv");
        File outputFile = new File("src/danmu.sql");
        if (outputFile.exists()) {
            if (outputFile.delete()) {
                System.out.println("delete");
            }
            if (outputFile.createNewFile()) {
                System.out.println("new file");
            }
        }
        FileWriter fileWriter = new FileWriter(outputFile);
        try (FileReader fileReader = new FileReader(inputFile);
             BufferedReader bufferedReader = new BufferedReader(fileReader)) {
            bufferedReader.readLine();
            System.out.println("write");
            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                String[] str = line.split(",");
                if (str.length == 4 && !str[3].contains("'")) {
                    fileWriter.write("INSERT INTO danmu (BV, mid, time,content) values ('" + str[0] + "','" + str[1] + "','" + str[2] + "','" + str[3] + "');\n");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            fileWriter.flush();
            fileWriter.close();
        }
    }

    public static void users() throws IOException {
        addUsers();
        File inputFile = new File("src/addUsers.csv");
        File outputFile = new File("src/users.sql");
        if (outputFile.exists()) {
            if (outputFile.delete()) {
                System.out.println("delete");
            }
            if (outputFile.createNewFile()) {
                System.out.println("new file");
            }
        }
        FileWriter fileWriter = new FileWriter(outputFile);


        try (FileReader fileReader = new FileReader(inputFile);
             BufferedReader bufferedReader = new BufferedReader(fileReader)) {
            System.out.println("write");
            String add;
            while ((add = bufferedReader.readLine()) != null) {
                add = add.replace('\'', ' ');
                add = add.replace('[', '{');
                add = add.replace(']', '}');
                add = add.replace('"', ' ');
                int indexOfSex = 0;
                int index1 = 0;
                int index2 = 0;
                String[] arr = add.split(",");
                for (int i = 0; i < arr.length; i++) {
                    if (arr[i].equals("男") || arr[i].equals("女") || arr[i].equals("保密")) {
                        indexOfSex = i;
                    }
                }
                for (int i = 0; i < arr.length; i++) {
                    if (arr[i].contains("{")) {
                        index1 = i;
                    }
                }
                for (int i = 0; i < arr.length; i++) {
                    if (arr[i].contains("}")) {
                        index2 = i;
                    }
                }
                fileWriter.write("INSERT INTO users (Mid,Name,Sex,Birthday,Level,Sign,following,identity) values (");
                //mid
                fileWriter.write(arr[0]);
                fileWriter.write(",");
                //name
                fileWriter.write("'");
                for (int i = 1; i <= indexOfSex - 1; i++) {
                    fileWriter.write(arr[i]);
                }
                fileWriter.write("'");
                fileWriter.write(",");
                //sex
                fileWriter.write("'");
                fileWriter.write(arr[indexOfSex]);
                fileWriter.write("'");
                fileWriter.write(",");
                //birth
                fileWriter.write("'");
                fileWriter.write(arr[indexOfSex + 1]);
                fileWriter.write("'");
                fileWriter.write(",");
                //level
                fileWriter.write(arr[indexOfSex + 2]);
                fileWriter.write(",");
                //sign
                fileWriter.write("'");
                for (int i = indexOfSex + 3; i < index1; i++) {
                    fileWriter.write(arr[i]);
                }
                fileWriter.write("'");
                fileWriter.write(",");
                //following
                fileWriter.write("'");
                for (int i = index1; i < index2; i++) {
                    fileWriter.write(arr[i] + ",");
                }
                fileWriter.write(arr[index2]);
                fileWriter.write("'");
                fileWriter.write(",");
                //identity
                fileWriter.write("'");
                fileWriter.write(arr[arr.length - 1]);
                fileWriter.write("'");
                fileWriter.write(");\n");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            fileWriter.flush();
            fileWriter.close();
        }
    }

    public static void addUsers() throws IOException {
        File inputFile = new File("src/users.csv");
        File outputFile = new File("src/addUsers.csv");
        if (outputFile.exists()) {
            if (outputFile.delete()) {
                System.out.println("delete");
            }
            if (outputFile.createNewFile()) {
                System.out.println("new file");
            }
        }
        FileWriter fileWriter = new FileWriter(outputFile);
        try (FileReader fileReader = new FileReader(inputFile);
             BufferedReader bufferedReader = new BufferedReader(fileReader)) {
            System.out.println("first write");
            bufferedReader.readLine();
            String add = bufferedReader.readLine();
            while (!add.equals("122879,敖厂长,保密,,6,,[],user")) {
                if (add.equals("")) {
                    add = "\\n";
                }
                fileWriter.write(add);
                if (add.endsWith("user")) {
                    fileWriter.write("\n");
                }
                add = bufferedReader.readLine();
            }
            fileWriter.write("122879,敖厂长,保密,,6,,[],user");
            System.out.println("finish");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            fileWriter.flush();
            fileWriter.close();
        }
    }

    public static void videos() throws IOException {
        File inputFile = new File("src/videos.csv");
        File outputFile = new File("src/videos.sql");
        if (outputFile.exists()) {
            if (outputFile.delete()) {
                System.out.println("delete");
            }
            if (outputFile.createNewFile()) {
                System.out.println("new file");
            }
        }
        FileWriter fileWriter = new FileWriter(outputFile);
        try (FileReader fileReader = new FileReader(inputFile);
             BufferedReader bufferedReader = new BufferedReader(fileReader)) {
            bufferedReader.readLine();
            System.out.println("write");
            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                fileWriter.write("INSERT INTO videos (BV,Title,Owner Mid,Owner Name,Commit Time,Review Time,Public Time,Duration,Description,Reviewer,Like,Coin,Favorite,View) values (" + line + ");\n");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            fileWriter.flush();
            fileWriter.close();
        }
    }
}
