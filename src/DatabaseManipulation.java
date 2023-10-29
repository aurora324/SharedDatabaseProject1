import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Arrays;

public class DatabaseManipulation implements DataManipulation {
    private Connection con = null;
    private ResultSet resultSet;
    //mid
    static String[] users = new String[37881];
    //name
    static String[] users_name = new String[37881];
    static String[][] follow = new String[37881][];
    static String[] followToString = new String[37881];
    //wbq
    private String host = "localhost";
    private String dbname = "数据库";
    private String user = "postgres";
    private String pwd = "000000";
    private String port = "5432";

    //hyf
//    private String host = "localhost";//"192.168.1.3";
//    private String dbname = "postgres";
//    private String user = "test";
//    private String pwd = "123456";
//    private String port = "5432";

    private void getConnection() {
        try {
            Class.forName("org.postgresql.Driver");
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

    public void addUsers() throws IOException {
        users();
        int result = 0;
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
//                    Array array = con.createArrayOf("Varchar", copy);
//                    preparedStatement.setArray(7, array);
                    preparedStatement.setString(7, String.valueOf(s8));
                    //System.out.println(preparedStatement);
                    result = preparedStatement.executeUpdate();
                } catch (SQLException e) {
                    e.printStackTrace();
                }

//                try {
//                    for (int i = 0; i < copy.length; i++) {
//                        PreparedStatement preparedStatement = con.prepareStatement(sql2);
//                        preparedStatement.setString(1, String.valueOf(s1));
//                        preparedStatement.setString(2, copy[i]);
//                        result = preparedStatement.executeUpdate();
//                    }
//                } catch (SQLException e) {
//                    e.printStackTrace();
//                }

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
            for (int i = 0; i < 37881; i++) {
                filewriter.write(users[i] + "\n");
                for (int j = 0; j < followToString.length; j++) {
                    if (followToString[j].contains(users[i])) {
                        filewriter.write(users[j] + " ");
                    }
                }
                filewriter.write("\n");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            filewriter.flush();
            filewriter.close();
        }
        long end = System.currentTimeMillis();
        System.out.println("users插入时间为：" + (end - start));
    }

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

    public void addVideos() throws IOException {
        int result = 0;
        long start = System.currentTimeMillis();
        getConnection();
//        File inputFile = new File("src/videos.csv");
        File outputFile = new File("src/addVideos.csv");
        if (outputFile.exists()) {
            if (outputFile.delete()) {
                System.out.println("delete");
            }
            if (outputFile.createNewFile()) {
                System.out.println("new file");
            }
        }
        FileWriter filewriter = new FileWriter(outputFile);
        String sql = "insert into video (BV,title,owner_mid,commit_time,review_time,public_time,duration,description,reviewer_mid) values (?,?,?,?,?,?,?,?,?);";

        try {

            FileInputStream fis = new FileInputStream("src/videos.csv");
            FileReader fileReader = new FileReader("src/videos.csv");

            // 检查 BOM
            byte[] bom = new byte[3];
            fis.read(bom);
            if (bom[0] == (byte) 0xEF && bom[1] == (byte) 0xBB && bom[2] == (byte) 0xBF) {
                // 文件包含 BOM，跳过前三个字节
                System.out.println(Arrays.toString(bom));
                fis.skip(3);
            } else {
                // 文件不包含 BOM，将文件指针重置到开头
                fis.reset();
            }
            BufferedReader bufferedReader = new BufferedReader(fileReader);


            // 创建 InputStreamReader 对象，并指定字符编码
            InputStreamReader isr = new InputStreamReader(fis, StandardCharsets.UTF_8);
            // 创建 CSVParser 对象
            CSVParser parser = CSVParser.parse(new FileReader("src/videos.csv"), CSVFormat.DEFAULT.withHeader());

            // 创建 PreparedStatement 对象
            PreparedStatement statement = con.prepareStatement(sql);


            // 遍历 CSV 记录并插入数据库
            for (CSVRecord record : parser) {

//                String add=bufferedReader.readLine();
//                String[] arr = add.split(",");
//                String bv = arr[0];
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

//                System.out.println(1);


                // 设置 PreparedStatement 参数



                // 执行插入语句
                statement.executeUpdate();
            }

            // 关闭资源
            statement.close();
            parser.close();
            isr.close();
            fis.close();

            System.out.println("数据插入成功！");
        } catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            filewriter.flush();
            filewriter.close();
        }
        long end = System.currentTimeMillis();
        System.out.println("users插入时间为：" + (end - start));
    }
    //检验有无BOM
    public void BOMDetector() throws IOException{
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

    public static int countOccurrences(String str, char ch) {
        int count = 0;
        for (int i = 0; i < str.length(); i++) {
            if (str.charAt(i) == ch) {
                count++;
            }
        }
        return count;
    }
}