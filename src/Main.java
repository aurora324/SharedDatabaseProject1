import java.io.IOException;

public class Main {
    public static void main(String[] args) {
        DataManipulation dm = null;
        try {
            dm = new DataFactory().createDataManipulation("database");
            dm.usersColumn_commonCSV();
            dm.videosColumn_commonCSV();
            dm.danmuColumn_commonCSV();
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}