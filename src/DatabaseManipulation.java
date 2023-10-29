import java.io.*;
import java.sql.*;

public class DatabaseManipulation implements DataManipulation {
    private Connection con = null;
    private ResultSet resultSet;

    private String host = "localhost";
    private String dbname = "数据库";
    private String user = "postgres";
    private String pwd = "000000";
    private String port = "5432";


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
        String sql = "insert into users (Mid,Name,Sex,Birthday,Level,Sign,identity) values (?,?,?,?,?,?,?);";
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
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
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

                try {
                    filewriter.write(s1 + "\n");
                    for (String s : copy) {
                        filewriter.write(s + " ");
                    }
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

    public void addFollow() {
        getConnection();
        File inputFile = new File("src/addUsers.csv");
        String sql = "insert into users (Mid,Name,Sex,Birthday,Level,Sign,following,identity) values (?,?,?,?,?,?,?,?);";
        String add = null;
    }

    public String allContinentNames() {
        getConnection();
        StringBuilder sb = new StringBuilder();
        String sql = "select continent from countries group by continent";
        try {
            Statement statement = con.createStatement();
            resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                sb.append(resultSet.getString("continent") + "\n");
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
                sb.append(resultSet.getString("continent") + "\t");
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
                sb.append(resultSet.getString("runtime") + "\t");
                sb.append(String.format("%-18s", resultSet.getString("country")));
                sb.append(resultSet.getString("continent") + "\t");
                sb.append(resultSet.getString("title") + "\t");
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
