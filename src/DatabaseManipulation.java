import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DatabaseManipulation implements DataManipulation {
    //mid
    static String[] users = new String[37881];
    //name
    static String[] users_name = new String[37881];
    static String[][] follow = new String[37881][];
    static String[] followToString = new String[37881];
    //wbq
//    private String host = "localhost";
//    private String dbname = "数据库";
//    private String user = "postgres";
//    private String pwd = "000000";
//    private String port = "5432";
    //hyf
    private final String host = "localhost";//"192.168.1.3";
//    private final String dbname = "postgres";
        private String dbname = "project 1";
    private final String user = "test";
    private final String pwd = "123456";
    private final String port = "5432";
    private Connection con = null;
    private ResultSet resultSet;

    public static void users() throws IOException {
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
        try (FileReader fileReader = new FileReader(inputFile); BufferedReader bufferedReader = new BufferedReader(fileReader)) {
            System.out.println("first write");
            bufferedReader.readLine();
            String add = bufferedReader.readLine();
            while (!add.equals("122879,敖厂长,保密,,6,,[],user")) {
                if (add.equals("")) {
                    add = " ";
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

    public static int countOccurrences(String str, char ch) {
        int count = 0;
        for (int i = 0; i < str.length(); i++) {
            if (str.charAt(i) == ch) {
                count++;
            }
        }
        return count;
    }

    private void getConnection() {
        try {
            Class.forName("org.postgresql.Driver");
            System.out.println("连接成功");
        } catch (Exception e) {
            System.err.println("Cannot find the PostgreSQL driver. Check CLASSPATH.");
            System.exit(1);
        }
        try {
            String url = "jdbc:postgresql://" + host + ":" + port + "/" + dbname;
            con = DriverManager.getConnection(url, user, pwd);
        } catch (SQLException e) {
            System.err.println("Database connection failed");
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    private void closeConnection() {
        if (con != null) {
            try {
                con.close();
                con = null;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void userFile() throws IOException {
        users();
        int result = 0;
        int count = 0;
        long start = System.currentTimeMillis();
        getConnection();
        File inputFile = new File("src/addUsers.csv");
        File outputFile = new File("src/addFollow.csv");
        if (outputFile.exists()) {
            if (outputFile.delete()) {
                System.out.println("delete");
            }
            if (outputFile.createNewFile()) {
                System.out.println("new file");
            }
        }
        FileWriter filewriter = new FileWriter(outputFile);
        String sql1 = "insert into users (Mid,Name,Sex,Birthday,Level,Sign,identity) values (?,?,?,?,?,?,?);";
        String add = null;
        int counter = 0;
        try (FileReader fileReader = new FileReader(inputFile);
             BufferedReader bufferedReader = new BufferedReader(fileReader)) {
            while ((add = bufferedReader.readLine()) != null) {
                add = add.replace('\'', ' ');
                add = add.replace('[', '{');
                add = add.replace(']', '}');
                add = add.replace('"', ' ');
                add = add.trim();
                int indexOfSex = 0;
                int index1 = 0;
                int index2 = 0;
                String[] arr = add.split(",");
                for (int i = 0; i < arr.length; i++) {
                    if (arr[i].equals("男") || arr[i].equals("女") || arr[i].equals("保密")) {
                        indexOfSex = i;
                        break;
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
                //mid
                String s1 = (arr[0]);
                //name
                StringBuilder s2 = new StringBuilder();
                for (int i = 1; i <= indexOfSex - 1; i++) {
                    s2.append(arr[i]);
                }
                //sex
                StringBuilder s3 = new StringBuilder();
                s3.append(arr[indexOfSex]);
                //birth
                StringBuilder s4 = new StringBuilder();
                s4.append(arr[indexOfSex + 1]);
                //level
                StringBuilder s5 = new StringBuilder();
                s5.append(arr[indexOfSex + 2]);
                //sign
                StringBuilder s6 = new StringBuilder();
                for (int i = indexOfSex + 3; i < index1; i++) {
                    s6.append(arr[i]);
                }
                //following
                String[] copy = new String[index2 - index1];
                for (int i = index1, j = 0; i < index2; i++, j++) {
                    arr[i] = arr[i].replace('{', ' ');
                    arr[i] = arr[i].replace('}', ' ');
                    arr[i] = arr[i].trim();
                    copy[j] = arr[i];
                }

                //identity
                StringBuilder s8 = new StringBuilder().append(arr[arr.length - 1]);
                try {
                    PreparedStatement preparedStatement = con.prepareStatement(sql1);
                    preparedStatement.setString(1, s1);
                    preparedStatement.setString(2, String.valueOf(s2));
                    preparedStatement.setString(3, String.valueOf(s3));
                    preparedStatement.setString(4, String.valueOf(s4));
                    preparedStatement.setInt(5, Integer.parseInt(String.valueOf(s5)));
                    preparedStatement.setString(6, String.valueOf(s6));
                    preparedStatement.setString(7, String.valueOf(s8));
                    result = preparedStatement.executeUpdate();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            closeConnection();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            filewriter.flush();
            filewriter.close();
        }
        long end = System.currentTimeMillis();
        System.out.println("users插入时间为：" + (end - start));
    }

    public void userFile_addBatch() throws IOException {
        users();
        int result = 0;
        int count = 0;
        long start = System.currentTimeMillis();
        getConnection();
        File inputFile = new File("src/addUsers.csv");
        File outputFile = new File("src/addFollow.csv");
        if (outputFile.exists()) {
            if (outputFile.delete()) {
                System.out.println("delete");
            }
            if (outputFile.createNewFile()) {
                System.out.println("new file");
            }
        }
        FileWriter filewriter = new FileWriter(outputFile);
        String sql1 = "insert into users (Mid,Name,Sex,Birthday,Level,Sign,identity) values (?,?,?,?,?,?,?);";
        String add = null;
        int counter = 0;
        try (FileReader fileReader = new FileReader(inputFile);
             BufferedReader bufferedReader = new BufferedReader(fileReader)) {
            PreparedStatement preparedStatement = con.prepareStatement(sql1);
            List<String[]> batchParams = new ArrayList<>();

            while ((add = bufferedReader.readLine()) != null) {
                add = add.replace('\'', ' ');
                add = add.replace('[', '{');
                add = add.replace(']', '}');
                add = add.replace('"', ' ');
                add = add.trim();
                int indexOfSex = 0;
                int index1 = 0;
                int index2 = 0;
                String[] arr = add.split(",");
                for (int i = 0; i < arr.length; i++) {
                    if (arr[i].equals("男") || arr[i].equals("女") || arr[i].equals("保密")) {
                        indexOfSex = i;
                        break;
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
                //mid
                String s1 = (arr[0]);
                //name
                StringBuilder s2 = new StringBuilder();
                for (int i = 1; i <= indexOfSex - 1; i++) {
                    s2.append(arr[i]);
                }
                //sex
                //birth
                //level
                //sign
                StringBuilder s6 = new StringBuilder();
                for (int i = indexOfSex + 3; i < index1; i++) {
                    s6.append(arr[i]);
                }
                //following
                String[] copy = new String[index2 - index1];
                for (int i = index1, j = 0; i < index2; i++, j++) {
                    arr[i] = arr[i].replace('{', ' ');
                    arr[i] = arr[i].replace('}', ' ');
                    arr[i] = arr[i].trim();
                    copy[j] = arr[i];
                }

                // 添加参数到批处理
                preparedStatement.setString(1, s1);
                preparedStatement.setString(2, String.valueOf(s2));
                preparedStatement.setString(3, String.valueOf(arr[indexOfSex]
                        //birth
                        //level
                        //sign
                        //following
                        // 添加参数到批处理
                ));
                preparedStatement.setString(4, String.valueOf(arr[indexOfSex + 1]
                        //level
                        //sign
                        //following
                        // 添加参数到批处理
                ));
                preparedStatement.setInt(5, Integer.parseInt(String.valueOf(arr[indexOfSex + 2]
                        //sign
                        //following
                        // 添加参数到批处理
                )));
                preparedStatement.setString(6, String.valueOf(s6));
                preparedStatement.setString(7, String.valueOf(arr[arr.length - 1]
                        // 添加参数到批处理
                ));
                preparedStatement.addBatch();

                count++;

                if (count % 100 == 0) {
                    // 执行批处理操作
                    preparedStatement.executeBatch();
                    preparedStatement.clearBatch();
                }
            }

            // 执行剩余的批处理操作
            preparedStatement.executeBatch();

            closeConnection();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            filewriter.flush();
            filewriter.close();
        }
        long end = System.currentTimeMillis();
        System.out.println("users插入时间为：" + (end - start));
    }

    public void usersColumn_commonCSV() throws IOException {
        long start = System.currentTimeMillis();
        long count = 0;
        getConnection();

        String sql = "insert into users (Mid,Name,Sex,Birthday,Level,Sign,identity) values (?,?,?,?,?,?,?);";

        try {
            FileInputStream fis = new FileInputStream("src/users.csv");

            // 检查 BOM
            byte[] bom = new byte[3];
            fis.read(bom);
            if (bom[0] == (byte) 0xEF && bom[1] == (byte) 0xBB && bom[2] == (byte) 0xBF) {
                // 文件包含 BOM，跳过前三个字节
//                System.out.println(Arrays.toString(bom));
                fis.skip(3);
            } else {
                // 文件不包含 BOM，将文件指针重置到开头
                fis.reset();
            }

            // 创建 InputStreamReader 对象，并指定字符编码
            InputStreamReader isr = new InputStreamReader(fis, StandardCharsets.UTF_8);
            // 创建 CSVParser 对象
            CSVParser parser = CSVParser.parse(new FileReader("src/users.csv"), CSVFormat.DEFAULT.withHeader());

            // 创建 PreparedStatement 对象
            PreparedStatement statement = con.prepareStatement(sql);

            //如果第一行是表头就用下面的语句跳过去
//            parser.iterator().next();

            // 遍历 CSV 记录并插入数据库
            for (CSVRecord record : parser) {

                String columnA = record.get(0);
                String columnB = record.get(1);
                String columnC = record.get(2);
                String columnD = record.get(3);
                String columnE = record.get(4);
                String columnF = record.get(5);
                String columnG = record.get(7);

                statement.setString(1, columnA);
                statement.setString(2, columnB);
                statement.setString(3, columnC);
                statement.setString(4, columnD);
                statement.setInt(5, Integer.parseInt(columnE));
                statement.setString(6, columnF);
                statement.setString(7, columnG);

//                statement.executeUpdate();

                statement.addBatch();

                count++;

                if (count % 100 == 0) {
                    statement.executeBatch();
                    statement.clearBatch();
                }
            }
            statement.executeBatch();

            // 关闭资源
            statement.close();
            parser.close();
            isr.close();
            fis.close();

            System.out.println("数据插入成功！");
        } catch (Exception e) {
            e.printStackTrace();
        }
        long end = System.currentTimeMillis();
        System.out.println("users插入时间为：" + (end - start));
    }

    public void addFollow() throws IOException {
        int result = 0;
        long start = System.currentTimeMillis();
        getConnection();
        File inputFile = new File("src/addUsers.csv");
//        File outputFile = new File("src/addFollow.csv");
//        if (outputFile.exists()) {
//            if (outputFile.delete()) {
//                System.out.println("delete");
//            }
//            if (outputFile.createNewFile()) {
//                System.out.println("new file");
//            }
//        }
//        FileWriter filewriter = new FileWriter(outputFile);
//        String sql1 = "insert into users (Mid,Name,Sex,Birthday,Level,Sign,identity) values (?,?,?,?,?,?,?);";
        String sql2 = "insert into Follow (follower_mid, following_mid) values (?,?);";
        String add = null;
        int counter = 0;
        try (FileReader fileReader = new FileReader(inputFile);
             BufferedReader bufferedReader = new BufferedReader(fileReader)) {
            while ((add = bufferedReader.readLine()) != null) {
                add = add.replace('\'', ' ');
                add = add.replace('[', '{');
                add = add.replace(']', '}');
                add = add.replace('"', ' ');
                add = add.trim();
                int indexOfSex = 0;
                int index1 = 0;
                int index2 = 0;
                String[] arr = add.split(",");
                for (int i = 0; i < arr.length; i++) {
                    if (arr[i].equals("男") || arr[i].equals("女") || arr[i].equals("保密")) {
                        indexOfSex = i;
                        break;
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
                //mid
                String s1 = (arr[0]);
                //name
                StringBuilder s2 = new StringBuilder();
                for (int i = 1; i <= indexOfSex - 1; i++) {
                    s2.append(arr[i]);
                }
                //sex
                StringBuilder s3 = new StringBuilder();
                s3.append(arr[indexOfSex]);
                //birth
                StringBuilder s4 = new StringBuilder();
                s4.append(arr[indexOfSex + 1]);
                //level
                StringBuilder s5 = new StringBuilder();
                s5.append(arr[indexOfSex + 2]);
                //sign
                StringBuilder s6 = new StringBuilder();
                for (int i = indexOfSex + 3; i < index1; i++) {
                    s6.append(arr[i]);
                }
                //following
                String[] copy = new String[index2 - index1];
                for (int i = index1, j = 0; i < index2; i++, j++) {
                    arr[i] = arr[i].replace('{', ' ');
                    arr[i] = arr[i].replace('}', ' ');
                    arr[i] = arr[i].trim();
                    copy[j] = arr[i];
                }

                //identity
                StringBuilder s8 = new StringBuilder().append(arr[arr.length - 1]);
//                try {
//                    PreparedStatement preparedStatement = con.prepareStatement(sql1);
//                    preparedStatement.setString(1, s1);
//                    preparedStatement.setString(2, String.valueOf(s2));
//                    preparedStatement.setString(3, String.valueOf(s3));
//                    preparedStatement.setString(4, String.valueOf(s4));
//                    preparedStatement.setInt(5, Integer.parseInt(String.valueOf(s5)));
//                    preparedStatement.setString(6, String.valueOf(s6));
////                    Array array = con.createArrayOf("Varchar", copy);
////                    preparedStatement.setArray(7, array);
//                    preparedStatement.setString(7, String.valueOf(s8));
//                    //System.out.println(preparedStatement);
//                    result = preparedStatement.executeUpdate();
//                } catch (SQLException e) {
//                    e.printStackTrace();
//                }

                try {
                    for (int i = 0; i < copy.length; i++) {
                        PreparedStatement preparedStatement = con.prepareStatement(sql2);
                        preparedStatement.setString(1, String.valueOf(s1));
                        preparedStatement.setString(2, copy[i]);
                        result = preparedStatement.executeUpdate();
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }

//                try {
//                    PreparedStatement preparedStatement = con.prepareStatement(sql2);
//                    preparedStatement.setString(1, s1);
//                    preparedStatement.setString(2, String.valueOf(s2));
//                    Array array = con.createArrayOf("Varchar", copy);
//                    preparedStatement.setArray(3, array);
//                    result = preparedStatement.executeUpdate();
//                } catch (SQLException e) {
//                    e.printStackTrace();
//                }


                follow[counter] = copy;
                followToString[counter] = Arrays.toString(copy);
                users[counter] = s1;
                users_name[counter] = String.valueOf(s2);
                counter++;
//                try {
//                    for (String s : copy) {
//                        filewriter.write(s + " ");
//                    }
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
            }
            closeConnection();

//             for (int i = 0; i < 37881; i++) {
//             filewriter.write(users[i] + "\n");
//             for (int j = 0; j < followToString.length; j++) {
//             if (followToString[j].contains(users[i])) {
//             filewriter.write(users[j] + " ");
//             }
//             }
//             filewriter.write("\n");
//             }
        } catch (Exception e) {
            e.printStackTrace();
        }
//         finally {
//            filewriter.flush();
//            filewriter.close();
//        }
        long end = System.currentTimeMillis();
        System.out.println("follow插入时间为：" + (end - start));
    }

    public void addFollower() {
        long start = System.currentTimeMillis();
        getConnection();
        File inputFile = new File("src/addFollow.csv");
        String sql = "insert into Follower (follower_mid, following_mid) values (?,?);";
        try (FileReader fileReader = new FileReader(inputFile); BufferedReader bufferedReader = new BufferedReader(fileReader)) {

            String s = null;
            String s1 = null;
            while ((s = bufferedReader.readLine()) != null) {
                s1 = bufferedReader.readLine();
                String[] follower_mid = s1.split(" ");
                try {
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    for (String value : follower_mid) {
                        preparedStatement.setString(1, s);
                        preparedStatement.setString(2, value);
                        preparedStatement.executeUpdate();
                    }
                } catch (SQLException e) {
                    System.out.println(s);
                    e.printStackTrace();
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeConnection();
        }
        long end = System.currentTimeMillis();
        System.out.println("Follower输入时间为" + (end - start));
    }

    public void follow() throws IOException {
        long start = System.currentTimeMillis();
        long count = 0;
        getConnection();
        String sql = "insert into follow (follower_mid,following_mid) values (?,?);";

        try {
            FileInputStream fis = new FileInputStream("src/users.csv");

            // 检查 BOM
            byte[] bom = new byte[3];
            fis.read(bom);
            if (bom[0] == (byte) 0xEF && bom[1] == (byte) 0xBB && bom[2] == (byte) 0xBF) {
                // 文件包含 BOM，跳过前三个字节
                fis.skip(3);
            } else {
                // 文件不包含 BOM，将文件指针重置到开头
                fis.reset();
            }

            // 创建 InputStreamReader 对象，并指定字符编码
            InputStreamReader isr = new InputStreamReader(fis, StandardCharsets.UTF_8);
            // 创建 CSVParser 对象
            CSVParser parser = CSVParser.parse(new FileReader("src/users.csv"), CSVFormat.DEFAULT.withTrim().withQuote('"').withEscape('\\'));
            // 创建 PreparedStatement 对象
            PreparedStatement statement = con.prepareStatement(sql);
//            System.out.println(parser.getRecordNumber());

            parser.iterator().next();
            // 遍历 CSV 记录并插入数据库
            for (CSVRecord record : parser) {

                String bv = record.get(0);
                String followList = record.get(6);
                followList = followList.replaceAll("\\[|\\]|\\s", "");
                String[] follows = followList.split(",");
                for (String follow : follows) {
                    if (follow.length() == 0) break;
                    if (follow.equals("user") || follow.equals("superuser")) continue;
                    statement.setString(1, bv);
                    statement.setString(2, follow.replaceAll("'", ""));
                    statement.addBatch();

                    count++;

                    if (count % 100 == 0) {
                        statement.executeBatch();
                    }
                }
            }
            statement.executeBatch();
            // 关闭资源
            statement.close();
            parser.close();
            isr.close();
            fis.close();

            System.out.println("数据插入成功！");
        } catch (Exception e) {
            e.printStackTrace();
        }
        long end = System.currentTimeMillis();
        System.out.println("follow插入时间为：" + (end - start));
    }

    public void videosColumn_commonCSV() throws IOException {
        long start = System.currentTimeMillis();
        int count = 0;
        getConnection();
        String sql = "insert into video (BV,title,owner_mid,commit_time,review_time,public_time,duration,description,reviewer_mid) values (?,?,?,?,?,?,?,?,?);";

        try {
            FileInputStream fis = new FileInputStream("src/videos.csv");

            // 检查 BOM
            byte[] bom = new byte[3];
            fis.read(bom);
            if (bom[0] == (byte) 0xEF && bom[1] == (byte) 0xBB && bom[2] == (byte) 0xBF) {
                // 文件包含 BOM，跳过前三个字节
//                System.out.println(Arrays.toString(bom));
                fis.skip(3);
            } else {
                // 文件不包含 BOM，将文件指针重置到开头
                fis.reset();
            }

            // 创建 InputStreamReader 对象，并指定字符编码
            InputStreamReader isr = new InputStreamReader(fis, StandardCharsets.UTF_8);
            // 创建 CSVParser 对象
            CSVParser parser = CSVParser.parse(new FileReader("src/videos.csv"), CSVFormat.DEFAULT.withHeader());

            // 创建 PreparedStatement 对象
            PreparedStatement statement = con.prepareStatement(sql);

            // 遍历 CSV 记录并插入数据库
            for (CSVRecord record : parser) {

                String columnA = record.get(0);
                String columnB = record.get("Title");
                String columnC = record.get("Owner Mid");
                String columnD = record.get("Commit Time");
                String columnE = record.get("Review Time");
                String columnF = record.get("Public Time");
                String columnG = record.get("Duration");
                String columnH = record.get("Description");
                String columnI = record.get("Reviewer");

                statement.setString(1, columnA);
                statement.setString(2, columnB);
                statement.setString(3, columnC);
                statement.setTimestamp(4, Timestamp.valueOf(columnD));
                statement.setTimestamp(5, Timestamp.valueOf(columnE));
                statement.setTimestamp(6, Timestamp.valueOf(columnF));
                statement.setInt(7, Integer.parseInt(columnG));
                statement.setString(8, columnH);
                statement.setString(9, columnI);

//                statement.executeUpdate();
                statement.addBatch();

                count++;

                if (count % 100 == 0) {
                    statement.executeBatch();
                    statement.clearBatch();
                }
            }
            statement.executeBatch();


            // 关闭资源
            statement.close();
            parser.close();
            isr.close();
            fis.close();

            System.out.println("数据插入成功！");
        } catch (Exception e) {
            e.printStackTrace();
        }
        long end = System.currentTimeMillis();
        System.out.println("video插入时间为：" + (end - start));
    }

    //检验有无BOM
    public void BOMDetector() throws IOException {
        try {
            FileInputStream fis = new FileInputStream("src/videos.csv");
            byte[] bom = new byte[3];
            fis.read(bom);
            fis.close();

            if (bom[0] == (byte) 0xEF && bom[1] == (byte) 0xBB && bom[2] == (byte) 0xBF) {
                System.out.println("CSV file contains BOM.");
            } else {
                System.out.println("CSV file does not contain BOM.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void danmuColumn_commonCSV() throws IOException {
        int count = 0;
        long start = System.currentTimeMillis();
        getConnection();
        String sql = "insert into danmu (BV,user_mid,time,content) values (?,?,?,?);";

        try {
            FileInputStream fis = new FileInputStream("src/danmu.csv");

            // 检查 BOM
            byte[] bom = new byte[3];
            fis.read(bom);
            if (bom[0] == (byte) 0xEF && bom[1] == (byte) 0xBB && bom[2] == (byte) 0xBF) {
                // 文件包含 BOM，跳过前三个字节
                fis.skip(3);
            } else {
                // 文件不包含 BOM，将文件指针重置到开头
                fis.reset();
            }

            // 创建 InputStreamReader 对象，并指定字符编码
            InputStreamReader isr = new InputStreamReader(fis, StandardCharsets.UTF_8);
            // 创建 CSVParser 对象
            CSVParser parser = CSVParser.parse(new FileReader("src/danmu.csv"), CSVFormat.DEFAULT.withHeader());

            // 创建 PreparedStatement 对象
            PreparedStatement statement = con.prepareStatement(sql);
//            System.out.println(parser.getRecordNumber());

            // 遍历 CSV 记录并插入数据库
            for (CSVRecord record : parser) {

                String columnA = record.get(0);
                String columnB = record.get(1);
                String columnC = record.get(2);
                String columnD = record.get(3);

                statement.setString(1, columnA);
                statement.setString(2, columnB);
                statement.setString(3, columnC);
                statement.setString(4, columnD);

//                statement.executeUpdate();
                statement.addBatch();
                count++;

                if (count % 100 == 0) {
                    // 执行批处理操作
                    statement.executeBatch();
                }
            }
            statement.executeBatch();
            // 关闭资源
            statement.close();
            parser.close();
            isr.close();
            fis.close();

            System.out.println("数据插入成功！");
        } catch (Exception e) {
            e.printStackTrace();
        }
        long end = System.currentTimeMillis();
        System.out.println("danmu插入时间为：" + (end - start));
    }

    public void danmu() throws IOException {
        int result = 0;
        long start = System.currentTimeMillis();
        getConnection();
        File inputFile = new File("src/danmu.csv");
        File outputFile = new File("src/adddammu.csv");
        if (outputFile.exists()) {
            if (outputFile.delete()) {
                System.out.println("delete");
            }
            if (outputFile.createNewFile()) {
                System.out.println("new file");
            }
        }
        FileWriter filewriter = new FileWriter(outputFile);
        String sql = "insert into danmu (BV,user_mid,time,content) values (?,?,?,?);";
        String add = null;
        try (FileReader fileReader = new FileReader(inputFile);
             BufferedReader bufferedReader = new BufferedReader(fileReader)) {
            while ((add = bufferedReader.readLine()) != null) {
                add = add.replace('\'', ' ');
                add = add.replace('[', '{');
                add = add.replace(']', '}');
                add = add.replace('"', ' ');
                int indexOfSex = 0;
                int index1 = 0;
                int index2 = 0;
                String[] arr = add.split(",");
                if (arr.length == 1 || arr.length == 3) {
                    System.out.println(arr.length);
                    System.out.println(Arrays.toString(arr));
                    continue;
                }
//                for (int i = 0; i < arr.length; i++) {
//                    if (arr[i].equals("男") || arr[i].equals("女") || arr[i].equals("保密")) {
//                        indexOfSex = i;
//                        break;
//                    }
//                }
//                for (int i = 0; i < arr.length; i++) {
//                    if (arr[i].contains("{")) {
//                        index1 = i;
//                    }
//                }
//                for (int i = 0; i < arr.length; i++) {
//                    if (arr[i].contains("}")) {
//                        index2 = i;
//                    }
//                }
                //BV
                String s1 = (arr[0]);
                //mid
                String s2 = (arr[1]);
                //user_mid
                String s3 = (arr[2]);
                //time
                String s4 = (arr[3]);
                //content
                StringBuilder s5 = new StringBuilder();
                for (int i = 4; i < arr.length - 1; i++) {
                    s5.append(arr[i]).append(",");
                }
                s5.append(arr[arr.length - 1]);
//                //following
//                String[] copy = new String[index2 - index1];
//                for (int i = index1, j = 0; i < index2; i++, j++) {
//                    arr[i] = arr[i].replace('{', ' ');
//                    arr[i] = arr[i].replace('}', ' ');
//                    arr[i] = arr[i].trim();
//                    copy[j] = arr[i];
//                }
//                //identity
//                StringBuilder s8 = new StringBuilder().append(arr[arr.length - 1]);

                try {
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    preparedStatement.setString(1, String.valueOf(s1));
                    preparedStatement.setString(2, String.valueOf(s2));
                    preparedStatement.setString(3, String.valueOf(s3));
                    preparedStatement.setString(4, String.valueOf(s4));
//                    preparedStatement.setString(5, String.valueOf(s5));
//                    Array array = con.createArrayOf("Varchar", copy);
//                    preparedStatement.setArray(7, array);
//                    System.out.println(preparedStatement);
                    result = preparedStatement.executeUpdate();
                } catch (SQLException e) {
                    e.printStackTrace();
                }

                try {
                    filewriter.write(s1 + "\n");
//                    for (String s : copy) {
//                        filewriter.write(s + " ");
//                    }
                    filewriter.write(" \n");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            closeConnection();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            filewriter.flush();
            filewriter.close();
        }
        long end = System.currentTimeMillis();
        System.out.println("danmu插入时间为：" + (end - start));
    }

    public void coinColumn_commonCSV() throws IOException {
        int count=0;
        long start = System.currentTimeMillis();
        getConnection();
        String sql = "insert into coin (video_BV,user_mid) values (?,?);";

        try {
            FileInputStream fis = new FileInputStream("src/videos.csv");

            // 检查 BOM
            byte[] bom = new byte[3];
            fis.read(bom);
            if (bom[0] == (byte) 0xEF && bom[1] == (byte) 0xBB && bom[2] == (byte) 0xBF) {
                // 文件包含 BOM，跳过前三个字节
                fis.skip(3);
            } else {
                // 文件不包含 BOM，将文件指针重置到开头
                fis.reset();
            }

            // 创建 InputStreamReader 对象，并指定字符编码
            InputStreamReader isr = new InputStreamReader(fis, StandardCharsets.UTF_8);
            // 创建 CSVParser 对象
            CSVParser parser = CSVParser.parse(new FileReader("src/videos.csv"), CSVFormat.DEFAULT.withTrim().withQuote('"').withEscape('\\'));
            // 创建 PreparedStatement 对象
            PreparedStatement statement = con.prepareStatement(sql);
//            System.out.println(parser.getRecordNumber());

            // 遍历 CSV 记录并插入数据库
            for (CSVRecord record : parser) {

                String bv = record.get(0);
                String coinList = record.get(11);
                coinList = coinList.replaceAll("\\[|\\]|\\s", "");
                String[] coins = coinList.split(",");
                for (String coin : coins) {
                    if (Objects.equals(coin, coins[0])) continue;
                    statement.setString(1, bv);
                    statement.setString(2, coin.replaceAll("'", ""));
//                    statement.executeUpdate();
                    statement.addBatch();
                    count++;

                    if (count % 100 == 0) {
                        // 执行批处理操作
                        statement.executeBatch();
                    }
                }

            }
statement.executeBatch();
            // 关闭资源
            statement.close();
            parser.close();
            isr.close();
            fis.close();

            System.out.println("数据插入成功！");
        } catch (Exception e) {
            e.printStackTrace();
        }
        long end = System.currentTimeMillis();
        System.out.println("coin插入时间为：" + (end - start));
    }

    public void addLike() throws IOException {
        long count=0;
        long start = System.currentTimeMillis();
        getConnection();
        String sql = "insert into thumbs_up (video_BV,user_mid) values (?,?);";

        try {
            FileInputStream fis = new FileInputStream("src/videos.csv");

            // 检查 BOM
            byte[] bom = new byte[3];
            fis.read(bom);
            if (bom[0] == (byte) 0xEF && bom[1] == (byte) 0xBB && bom[2] == (byte) 0xBF) {
                // 文件包含 BOM，跳过前三个字节
                fis.skip(3);
            } else {
                // 文件不包含 BOM，将文件指针重置到开头
                fis.reset();
            }

            // 创建 InputStreamReader 对象，并指定字符编码
            InputStreamReader isr = new InputStreamReader(fis, StandardCharsets.UTF_8);
            // 创建 CSVParser 对象
            CSVParser parser = CSVParser.parse(new FileReader("src/videos.csv"), CSVFormat.DEFAULT.withTrim().withQuote('"').withEscape('\\'));
            // 创建 PreparedStatement 对象
            PreparedStatement statement = con.prepareStatement(sql);
//            System.out.println(parser.getRecordNumber());

            parser.iterator().next();
            // 遍历 CSV 记录并插入数据库
            for (CSVRecord record : parser) {

                String bv = record.get(0);
                String likeList = record.get(10);
                likeList = likeList.replaceAll("\\[|\\]|\\s", "");
                String[] likes = likeList.split(",");
                for (String like : likes) {
                    statement.setString(1, bv);
                    statement.setString(2, like.replaceAll("'", ""));
//                    statement.executeUpdate();
                    statement.addBatch();
                    count++;

                    if (count % 100 == 0) {
                        // 执行批处理操作
                        statement.executeBatch();
                    }
                }

            }
            statement.executeBatch();
            // 关闭资源
            statement.close();
            parser.close();
            isr.close();
            fis.close();

            System.out.println("数据插入成功！");
        } catch (Exception e) {
            e.printStackTrace();
        }
        long end = System.currentTimeMillis();
        System.out.println("thumbs_up插入时间为：" + (end - start));
    }

    public void addFavorite() throws IOException {
        long start = System.currentTimeMillis();
        int count = 0;
        getConnection();
        String sql = "insert into favorite (video_BV,user_mid) values (?,?);";

        try {
            FileInputStream fis = new FileInputStream("src/videos.csv");

            // 检查 BOM
            byte[] bom = new byte[3];
            fis.read(bom);
            if (bom[0] == (byte) 0xEF && bom[1] == (byte) 0xBB && bom[2] == (byte) 0xBF) {
                // 文件包含 BOM，跳过前三个字节
                fis.skip(3);
            } else {
                // 文件不包含 BOM，将文件指针重置到开头
                fis.reset();
            }

            // 创建 InputStreamReader 对象，并指定字符编码
            InputStreamReader isr = new InputStreamReader(fis, StandardCharsets.UTF_8);
            // 创建 CSVParser 对象
            CSVParser parser = CSVParser.parse(new FileReader("src/videos.csv"), CSVFormat.DEFAULT.withTrim().withQuote('"').withEscape('\\'));
            // 创建 PreparedStatement 对象
            PreparedStatement statement = con.prepareStatement(sql);
//            System.out.println(parser.getRecordNumber());

            parser.iterator().next();
            // 遍历 CSV 记录并插入数据库
            for (CSVRecord record : parser) {

                String bv = record.get(0);
                String favoriteList = record.get(12);
                favoriteList = favoriteList.replaceAll("\\[|\\]|\\s", "");
                String[] favorites = favoriteList.split(",");

                for (String favorite : favorites) {
                    statement.setString(1, bv);
                    statement.setString(2, favorite.replaceAll("'", ""));
//                    statement.executeUpdate();
                    statement.addBatch();
                    count++;

                    if (count % 100 == 0) {
                        // 执行批处理操作
                        statement.executeBatch();
                        statement.clearBatch();
                    }
                }

            }
            statement.executeBatch();

            // 关闭资源
            statement.close();
            parser.close();
            isr.close();
            fis.close();

            System.out.println("数据插入成功！");
        } catch (Exception e) {
            e.printStackTrace();
        }
        long end = System.currentTimeMillis();
        System.out.println("favorite插入时间为：" + (end - start));
    }

    public void addView() throws IOException {
        long start = System.currentTimeMillis();
        int count = 0;
        getConnection();
        String sql = "insert into view (video_BV,user_mid,last_watch_time_duration) values (?,?,?);";

        try {
            FileInputStream fis = new FileInputStream("src/videos.csv");

            // 检查 BOM
            byte[] bom = new byte[3];
            fis.read(bom);
            if (bom[0] == (byte) 0xEF && bom[1] == (byte) 0xBB && bom[2] == (byte) 0xBF) {
                // 文件包含 BOM，跳过前三个字节
                fis.skip(3);
            } else {
                // 文件不包含 BOM，将文件指针重置到开头
                fis.reset();
            }

            // 创建 InputStreamReader 对象，并指定字符编码
            InputStreamReader isr = new InputStreamReader(fis, StandardCharsets.UTF_8);
            // 创建 CSVParser 对象
            CSVParser parser = CSVParser.parse(new FileReader("src/videos.csv"), CSVFormat.DEFAULT.withTrim().withQuote('"').withEscape('\\'));
            // 创建 PreparedStatement 对象
            PreparedStatement statement = con.prepareStatement(sql);
//            System.out.println(parser.getRecordNumber());
            //跳过第一行
//            parser.iterator().next();
            // 遍历 CSV 记录并插入数据库
            for (CSVRecord record : parser) {

                String bv = record.get(0);
                String viewData = record.get(13);

                viewData = viewData.replaceAll("\\[|\\]|'", "");
                Pattern pattern = Pattern.compile("\\((\\d+),\\s*(\\d+)\\)");
                Matcher matcher = pattern.matcher(viewData);
                while (matcher.find()) {
                    String userMid = matcher.group(1);
                    String lastWatchTimeDuration = matcher.group(2);

                    statement.setString(1, bv);
                    statement.setString(2, userMid);
                    statement.setInt(3, Integer.parseInt(lastWatchTimeDuration));
//                    statement.executeUpdate();
                    statement.addBatch();
                    count++;

                    if (count % 100 == 0) {
                        // 执行批处理操作
                        statement.executeBatch();
                    }
                }
            }
            statement.executeBatch();
            // 关闭资源
            statement.close();
            parser.close();
            isr.close();
            fis.close();

            System.out.println("数据插入成功！");
        } catch (Exception e) {
            e.printStackTrace();
        }
        long end = System.currentTimeMillis();
        System.out.println("view插入时间为：" + (end - start));
    }

    public String count(String s) {
        getConnection();
        long start = System.currentTimeMillis();
        StringBuilder sb = new StringBuilder();
        String sql = "select count(*) from "+s+";";
        try {
            Statement statement = con.createStatement();
            resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                sb.append(resultSet.getString("count")).append("\n");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeConnection();
        }
        long end = System.currentTimeMillis();
        System.out.println("用时为: " + (end - start)+"ms");
        return sb.toString();
    }
    public String allTableOrderByMid(String s1,String s2,String order) {
        getConnection();
        long start = System.currentTimeMillis();
        StringBuilder sb = new StringBuilder();
        String sql = "select "+s1+" from "+s2+" order by "+order+";";
        try {
            Statement statement = con.createStatement();
            resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                sb.append(resultSet.getString(s1)).append("\n");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeConnection();
        }
        long end = System.currentTimeMillis();
        System.out.println("用时为: " + (end - start)+"ms");
        return sb.toString();
    }
    public String joinSelect() {
        getConnection(); // start connection
        long start = System.currentTimeMillis();
        String sql = "select v.bv, x.mid, x.identity, x.name\n" +
                "from video v\n" +
                "         join (select mid, identity, name\n" +
                "               from danmu d\n" +
                "                        left join users u on u.mid = d.user_mid\n" +
                "                        right join video v on d.bv = v.bv\n" +
                "               ) x on v.reviewer_mid = x.mid;";// string combination
        try {
            Statement statement = con.createStatement();
            resultSet = statement.executeQuery(sql);
            StringBuilder stringBuilder = new StringBuilder(); //combine multi-strings
            while (resultSet.next()) {
                stringBuilder.append(resultSet.getString("bv")).append("\t");
                stringBuilder.append(resultSet.getString("mid")).append("\t");
                stringBuilder.append(resultSet.getString("identity")).append("\t");
                stringBuilder.append(resultSet.getString("name")).append("\n");
            }
            long end = System.currentTimeMillis();
            System.out.println("用时为: " + (end - start)+"ms");
            return stringBuilder.toString();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            closeConnection(); // close connection
        }
        return null;
    }
    public void deleteDataFromTable() {
        getConnection();
        long start = System.currentTimeMillis();
        int deleteCount = 1000000;

        try {
            String deleteQuery = "delete from danmu where danmu_id = ?";
            PreparedStatement statement = con.prepareStatement(deleteQuery);

            for (int i = 0; i < deleteCount; i++) {
                statement.setInt(1, i+1);
                statement.executeUpdate();
            }
            long end = System.currentTimeMillis();
            System.out.println("删除"+deleteCount+"条数据用时为: " + (end - start)+"ms");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public void insertDanmu() throws IOException {
        int count = 1;
        long insertNum=1000000;
        long start = System.currentTimeMillis();
        getConnection();
        String sql = "insert into danmu (danmu_id,BV,user_mid,time,content) values (?,?,?,?,?);";

        try {
            FileInputStream fis = new FileInputStream("src/danmu.csv");

            // 检查 BOM
            byte[] bom = new byte[3];
            fis.read(bom);
            if (bom[0] == (byte) 0xEF && bom[1] == (byte) 0xBB && bom[2] == (byte) 0xBF) {
                // 文件包含 BOM，跳过前三个字节
                fis.skip(3);
            } else {
                // 文件不包含 BOM，将文件指针重置到开头
                fis.reset();
            }

            // 创建 InputStreamReader 对象，并指定字符编码
            InputStreamReader isr = new InputStreamReader(fis, StandardCharsets.UTF_8);
            // 创建 CSVParser 对象
            CSVParser parser = CSVParser.parse(new FileReader("src/danmu.csv"), CSVFormat.DEFAULT.withHeader());

            // 创建 PreparedStatement 对象
            PreparedStatement statement = con.prepareStatement(sql);
//            System.out.println(parser.getRecordNumber());

            // 遍历 CSV 记录并插入数据库
            for (CSVRecord record : parser) {
                if(count>insertNum){
                    break;
                }
                String columnA = record.get(0);
                String columnB = record.get(1);
                String columnC = record.get(2);
                String columnD = record.get(3);

                statement.setInt(1, count);
                statement.setString(2, columnA);
                statement.setString(3, columnB);
                statement.setString(4, columnC);
                statement.setString(5, columnD);

//                statement.executeUpdate();
                statement.addBatch();
                count++;

                if (count % 100 == 0) {
                    // 执行批处理操作
                    statement.executeBatch();
                }
            }
            statement.executeBatch();
            // 关闭资源
            statement.close();
            parser.close();
            isr.close();
            fis.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
        long end = System.currentTimeMillis();
        System.out.println(insertNum+"条数据插入时间为：" + (end - start));
    }
    public void update(){
        getConnection();
        long count=0;
        long start = System.currentTimeMillis();;
        try  {
            DatabaseMetaData metaData = con.getMetaData();
            ResultSet columns = metaData.getColumns(null, null, "users", null);

            while (columns.next()) {
                String updateQuery = "UPDATE users SET sign = '0' WHERE sign ='';";

                try (PreparedStatement statement = con.prepareStatement(updateQuery)) {
                    statement.executeUpdate();
                    count++;
                } catch (SQLException e) {
                    e.printStackTrace();
                }

            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        long end = System.currentTimeMillis();
        System.out.println("update "+count+"条数据时间为：" + (end - start));
    }

    public String allContinentNames() {
        getConnection();
        StringBuilder sb = new StringBuilder();
        String sql = "select continent from countries group by continent";
        try {
            Statement statement = con.createStatement();
            resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                sb.append(resultSet.getString("continent")).append("\n");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeConnection();
        }

        return sb.toString();
    }

    public String continentsWithCountryCount() {
        getConnection();
        StringBuilder sb = new StringBuilder();
        String sql = "select continent, count(*) countryNumber from countries group by continent;";
        try {
            Statement statement = con.createStatement();
            resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                sb.append(resultSet.getString("continent")).append("\t");
                sb.append(resultSet.getString("countryNumber"));
                sb.append(System.lineSeparator());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeConnection();
        }
        return sb.toString();
    }

    public String FullInformationOfMoviesRuntime(int min, int max) {
        getConnection();
        StringBuilder sb = new StringBuilder();
        String sql = "select m.title,c.country_name country,c.continent ,m.runtime " +
                "from movies m " +
                "join countries c on m.country=c.country_code " +
                "where m.runtime between ? and ? order by runtime;";
        try {
            PreparedStatement preparedStatement = con.prepareStatement(sql);
            preparedStatement.setInt(1, min);
            preparedStatement.setInt(2, max);
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                sb.append(resultSet.getString("runtime")).append("\t");
                sb.append(String.format("%-18s", resultSet.getString("country")));
                sb.append(resultSet.getString("continent")).append("\t");
                sb.append(resultSet.getString("title")).append("\t");
                sb.append(System.lineSeparator());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeConnection();
        }
        return sb.toString();
    }

    public String findMovieById(int id) {
        return null;
    }

    public String findMoviesByTitleLimited10(String title) {
        getConnection(); // start connection
        String sql = "select m.title, c.country_name country, m.runtime,m.year_released\n" +
                "from movies m join countries c on m.country = c.country_code\n" +
                "where m.title like '%'||" + title + "||'%'limit 10;";// string combination
        try {
            Statement statement = con.createStatement();
            resultSet = statement.executeQuery(sql);
            StringBuilder stringBuilder = new StringBuilder(); //combine multi-strings
            while (resultSet.next()) {
                stringBuilder.append(String.format("%-20s\t",
                        resultSet.getString("country")));
                stringBuilder.append(resultSet.getInt("year_released")).append("\t");
                stringBuilder.append(resultSet.getInt("runtime")).append("\t");
                stringBuilder.append(resultSet.getString("title")).append("\n");
            }
            return stringBuilder.toString();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            closeConnection(); // close connection
        }
        return null;
    }
}