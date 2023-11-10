import java.io.IOException;

public class Main {
    public static void main(String[] args) {
        DataManipulation dm = null;
//        multimporter mi = null;
        try {
            dm = new DataFactory().createDataManipulation("database");
//            mi = new multimporter();
//            dm.addUsers();
//            dm.addFollow();
//            dm.addFollower();
//            dm.videosColumn_commonCSV();
//            dm.danmu();
//            dm.danmuColumn_commonCSV();
//            dm.coinColumn_commonCSV();
//            dm.follow();
//            dm.addLike();
//            dm.addFavorite();
//            dm.addView();
//            dm.videosColumn_commonCSV();
//            dm.usersColumn_commonCSV();
//            dm.userFile_addBatch();

//            mi.addDanmu();

            //count
//            System.out.println(dm.count("danmu"));
//            System.out.println(dm.allTableOrderByMid("owner_mid","video","commit_time"));
//            System.out.println(dm.allTableOrderByMid("name","users","mid"));
//            String s = dm.allTableOrderByMid("time","danmu","user_mid");
//            String s = dm.joinSelect();
//            System.out.println(s);

            // compare
            dm.deleteDataFromTable();
            dm.insertDanmu();
//            dm.update();
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}