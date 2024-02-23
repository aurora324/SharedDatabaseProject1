import java.io.*;

public class FIleManipulation {
    public static void main(String[] args) throws IOException {
//        danmu();
//        users();
        videos();
//        addUsers();
//        danmuAdd();
//        danmu();
//        deleteDanmu();
//        users();
//        insertDanmu();
//        updateDanmu();
//        TOEFL();
    }

    public static void danmu() throws IOException {
        danmuAdd();
        long start = System.currentTimeMillis();
        File inputFile = new File("src/danmuAdd.txt");
        File outputFile = new File("E:/Datagrip/代码/cs307/danmu.sql");
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
            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                line = line.replace('\'', ' ');
                String[] str = line.split(",");
                StringBuilder stringBuilder = new StringBuilder();
                for (int i = 3; i < str.length; i++) {
                    stringBuilder.append(str[i]);
                }
                fileWriter.write("INSERT INTO danmu (BV, user_mid, time,content) values ('" + str[0] + "','" + str[1] + "','" + str[2] + "','" + stringBuilder + "');\n");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            fileWriter.flush();
            fileWriter.close();
        }
        long end = System.currentTimeMillis();
        System.out.println("danmu写出时间为" + (end - start));
    }

    public static void insertDanmu() throws IOException {
        long start = System.currentTimeMillis();
        File inputFile = new File("src/danmuAdd.txt");
        File outputFile = new File("E:/Datagrip/代码/cs307/insertDanmu.sql");
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
            String line = null;
            long counter = 1;
            while ((line = bufferedReader.readLine()) != null && counter <= 1000000) {
                line = line.replace('\'', ' ');
                line = line.replace("\\", "");
                String[] str = line.split(",");
                StringBuilder stringBuilder = new StringBuilder();
                for (int i = 3; i < str.length; i++) {
                    stringBuilder.append(str[i]);
                }
                fileWriter.write("INSERT INTO danmu (danmu_id,BV, user_mid, time,content) values (" + counter + ",'" + str[0] + "','" + str[1] + "','" + str[2] + "','" + stringBuilder + "');\n");
                counter++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            fileWriter.flush();
            fileWriter.close();
        }
        long end = System.currentTimeMillis();
        System.out.println("danmu写出时间为" + (end - start));
    }

    public static void deleteDanmu() throws IOException {
        long start = System.currentTimeMillis();
        File outputFile = new File("E:/Datagrip/代码/cs307/deleteDanmu.sql");
        if (outputFile.exists()) {
            if (outputFile.delete()) {
                System.out.println("delete");
            }
            if (outputFile.createNewFile()) {
                System.out.println("new file");
            }
        }
        FileWriter fileWriter = new FileWriter(outputFile);
        try {
            for (long i = 1; i <= 1000000; i++) {
                fileWriter.write("delete from Danmu where danmu_id=" + i + ";\n");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            fileWriter.flush();
            fileWriter.close();
        }
        long end = System.currentTimeMillis();
        System.out.println("deleteDanmu写出时间为" + (end - start));
    }

    public static void updateDanmu() throws IOException {
        long start = System.currentTimeMillis();
        File outputFile = new File("E:/Datagrip/代码/cs307/updateDanmu.sql");
        if (outputFile.exists()) {
            if (outputFile.delete()) {
                System.out.println("delete");
            }
            if (outputFile.createNewFile()) {
                System.out.println("new file");
            }
        }
        FileWriter fileWriter = new FileWriter(outputFile);
        try {
            fileWriter.write("UPDATE users SET sign = '0' WHERE sign ='';");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            fileWriter.flush();
            fileWriter.close();
        }
        long end = System.currentTimeMillis();
        System.out.println("deleteDanmu写出时间为" + (end - start));
    }


    public static void danmuAdd() throws IOException {
        long start = System.currentTimeMillis();
        File inputFile = new File("src/Danmu.csv");
        File outputFile = new File("src/danmuAdd.txt");
        if (outputFile.exists()) {
            if (outputFile.delete()) {
                System.out.println("delete");
            }
            if (outputFile.createNewFile()) {
                System.out.println("new file");
            }
        }
        FileWriter fileWriter = new FileWriter(outputFile);
        try (FileReader fileReader = new FileReader(inputFile); BufferedReader bufferedReader = new BufferedReader(fileReader)) {
            System.out.println("first write");
            bufferedReader.readLine();
            fileWriter.write(bufferedReader.readLine());
            String add = bufferedReader.readLine();
            while (!(add == null)) {
                if (add.equals("")) {
                    add = " ";
                }
                add = add.replace('"', ' ');
                add = add.trim();
                if (add.contains("BV")) {
                    fileWriter.write("\n");
                }
                fileWriter.write(add);
                add = bufferedReader.readLine();
            }
            System.out.println("finish");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            fileWriter.flush();
            fileWriter.close();
        }
        long end = System.currentTimeMillis();
        System.out.println("danmu文件写入时间" + (end - start));
    }

    public static void users() throws IOException {
        addUsers();
        long start = System.currentTimeMillis();
        File inputFile = new File("src/users.csv");
        File outputFile = new File("E:/Datagrip/代码/cs307/users.sql");
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
                if (!add.endsWith("user")) {
                    add += bufferedReader.readLine();
                }
                add = add.replace('[', '{');
                add = add.replace(']', '}');
                add = add.replace("\"", "\\\"");
                add = add.replace(';', ' ');
                add = add.replace("\\", "\\\\");
                add = add.trim();
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
                fileWriter.write("INSERT INTO users (Mid,Name,Sex,Birthday,Level,Sign,identity) values (");
                //mid
                fileWriter.write("'");
                fileWriter.write(arr[0]);
                fileWriter.write("',");
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
        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }

    public static void addUsers() throws IOException {
        long start = System.currentTimeMillis();
        File inputFile = new File("src/users.csv");
        File outputFile = new File("src/addUsers.txt");
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
        long end = System.currentTimeMillis();
        System.out.println("user写入时间为" + (end - start));
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

        /**
         * 使用bufferedWhiter中不需要关闭
         */
        try (BufferedReader bufferedReader=new BufferedReader(new FileReader(inputFile));
        BufferedWriter bufferedWriter=new BufferedWriter(new FileWriter(outputFile))) {
            bufferedReader.readLine();
            System.out.println("write");
            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                bufferedWriter.write("INSERT INTO videos (BV,Title,Owner Mid,Owner Name,Commit Time,Review Time,Public Time,Duration,Description,Reviewer,Like,Coin,Favorite,View) " +
                        "values (" + line + ");");
                bufferedWriter.newLine();
            }
            bufferedWriter.close();
            bufferedWriter.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void TOEFL() throws IOException {
        File inputFile = new File("src/托福乱序.csv");
        File outputFile = new File("E:/Datagrip/代码/lab3/users.sql");
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
                line = line.replace("'","''");
                line = line.replace("\"","'");
                fileWriter.write("INSERT INTO TOEFL(id,word,phoneticSymbol,explanation) " +
                        "values (" + line + ");\n");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            fileWriter.flush();
            fileWriter.close();
        }
    }
}
