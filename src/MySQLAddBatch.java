import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Arrays;

public class MySQLAddBatch { // 创建类Conn
    public static Connection con; // 声明Connection对象
    public static String user = "root";
    public static String password = "000000";

    public static void getConnection() {// 建立返回值为Connection的方法
        try { // 加载数据库驱动类
            Class.forName("com.mysql.cj.jdbc.Driver");
            System.out.println("数据库驱动加载成功");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        try { // 通过访问数据库的URL获取数据库连接对象
            con = DriverManager.getConnection("jdbc:mysql://localhost:3306/table?useUnicode=true&characterEncoding=UTF-8", user, password);
            System.out.println("数据库连接成功");
        } catch (SQLException e) {
            e.printStackTrace();
        }


    }

    private static void closeConnection() {
        if (con != null) {
            try {
                con.close();
                con = null;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws IOException, SQLException { // 主方法，测试连接
        MySQLAddBatch c = new MySQLAddBatch(); // 创建本类对象
        insert();
        addFollow();
        follow();
        addDanmu();
    }

    public static void insert() throws IOException {
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

        String add = null;
        int counter = 0;

        try (FileReader fileReader = new FileReader(inputFile);
             BufferedReader bufferedReader = new BufferedReader(fileReader)) {
            try {
                con.setAutoCommit(false);
                PreparedStatement cmd = null;
                cmd = con.prepareStatement("insert into users (Mid,Name,Sex,Birthday,Level,Sign,identity) values (?,?,?,?,?,?,?);");
//                FileInputStream fis = new FileInputStream("src/addUsers.csv");
//                // 检查 BOM
//                byte[] bom = new byte[3];
//                fis.read(bom);
//                if (bom[0] == (byte) 0xEF && bom[1] == (byte) 0xBB && bom[2] == (byte) 0xBF) {
//                    // 文件包含 BOM，跳过前三个字节
//                    System.out.println(Arrays.toString(bom));
//                    fis.skip(3);
//                } else {
//                    // 文件不包含 BOM，将文件指针重置到开头
//                    fis.reset();
//                }
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
                    cmd.setString(1, s1);
                    cmd.setString(2, String.valueOf(s2));
                    cmd.setString(3, String.valueOf(s3));
                    cmd.setString(4, String.valueOf(s4));
                    cmd.setInt(5, Integer.parseInt(String.valueOf(s5)));
                    cmd.setString(6, String.valueOf(s6));
                    cmd.setString(7, String.valueOf(s8));

                    cmd.addBatch();
                    if (counter % 100 == 0) cmd.executeBatch();
                    counter++;
                }
                cmd.executeBatch();
                con.commit();
                cmd.close();
                con.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            filewriter.flush();
            filewriter.close();
        }
        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }

    public static void addFollow() throws IOException, SQLException {
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

        String add = null;
        int counter = 0;

        try (FileReader fileReader = new FileReader(inputFile);
             BufferedReader bufferedReader = new BufferedReader(fileReader)) {
            try {
                con.setAutoCommit(false);
                PreparedStatement cmd = null;
                cmd = con.prepareStatement("insert into Follow (follower_mid, following_mid) values (?,?);");
//                FileInputStream fis = new FileInputStream("src/addUsers.csv");
//                // 检查 BOM
//                byte[] bom = new byte[3];
//                fis.read(bom);
//                if (bom[0] == (byte) 0xEF && bom[1] == (byte) 0xBB && bom[2] == (byte) 0xBF) {
//                    // 文件包含 BOM，跳过前三个字节
//                    System.out.println(Arrays.toString(bom));
//                    fis.skip(3);
//                } else {
//                    // 文件不包含 BOM，将文件指针重置到开头
//                    fis.reset();
//                }
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
                    if (copy.length > 0) {
                        for (int i = 0; i < copy.length; i++) {
                            cmd.setString(1, s1);
                            cmd.setString(2, copy[i]);
                            cmd.addBatch();
                            counter++;
                        }
                    }

//                    else{
//                        cmd.setString(1, s1);
//                        cmd.setString(2," ");
//                        cmd.addBatch();
//                        counter++;
//                    }
                    System.out.println(counter);
                    cmd.executeBatch();
                }
                con.commit();
                cmd.close();
                con.close();
            } catch (SQLException e) {
                System.out.println(add);
                throw new RuntimeException(e);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            filewriter.flush();
            filewriter.close();
        }
        long end = System.currentTimeMillis();
        System.out.println(end - start);
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
    public static void addDanmu() throws IOException {
        long start = System.currentTimeMillis();
        getConnection();
        File inputFile = new File("src/danmuAdd.csv");
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

        String add = null;
        int counter = 0;

        try (FileReader fileReader = new FileReader(inputFile);
             BufferedReader bufferedReader = new BufferedReader(fileReader)) {
            try {
                con.setAutoCommit(false);
                PreparedStatement cmd = null;
                cmd = con.prepareStatement("INSERT INTO danmu(BV, user_mid, time,content) values(?,?,?,?); ");
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
                    String s1=arr[0];
                    String s2=arr[1];
                    String s3=arr[2];
                    StringBuilder s4 = new StringBuilder();
                    for (int i = 3; i < arr.length; i++) {
                        s4.append(arr[i]);
                    }
                    cmd.setString(1, s1);
                    cmd.setString(2, String.valueOf(s2));
                    cmd.setString(3, String.valueOf(s3));
                    cmd.setString(4, String.valueOf(s4));

                    cmd.addBatch();
                    if (counter % 100 == 0) cmd.executeBatch();
                    counter++;
                }
                cmd.executeBatch();
                con.commit();
                cmd.close();
                con.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            filewriter.flush();
            filewriter.close();
        }
        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }

    public static void follow() throws IOException {
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

}

