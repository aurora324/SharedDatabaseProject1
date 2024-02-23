import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Arrays;

public class MySQLColumn {
    private ResultSet resultSet;
    //mid
    static String[] users = new String[37881];
    //name
    static String[] users_name = new String[37881];
    static String[][] follow = new String[37881][];
    static String[] followToString = new String[37881];
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

    public static void main(String[] args) throws IOException { // 主方法，测试连接
        addVideos();
        usersColumn_commonCSV();
        videosColumn_commonCSV();
    }

    public static void videosColumn_commonCSV() throws IOException {
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

    public static void addVideos() throws IOException {
        long start = System.currentTimeMillis();
        int count = 0;
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

            con.setAutoCommit(false);
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
                statement.addBatch();
                count++;
                if (count % 100 == 0) statement.executeBatch();
//                System.out.println(1);


                // 设置 PreparedStatement 参数


                // 执行插入语句
//                statement.executeUpdate();
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
        } finally {
            filewriter.flush();
            filewriter.close();
        }
        long end = System.currentTimeMillis();
        System.out.println("users插入时间为：" + (end - start));
    }

    public static void usersColumn_commonCSV() throws IOException {
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
                statement.executeUpdate();
//                count++;
//                if (count % 100 == 0) {
//                    statement.executeBatch();
//                }
            }
//            statement.executeBatch();
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
}
