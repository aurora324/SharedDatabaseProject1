import java.io.IOException;

public interface DataManipulation {
    public void userFile() throws IOException;
    public void addFollow()throws IOException;
    public void addFollower()throws IOException;
    public void videosColumn_commonCSV()throws  IOException;
    public void usersColumn_commonCSV() throws  IOException;
    public void userFile_addBatch() throws  IOException;
    public void danmu() throws  IOException;
    public void danmuColumn_commonCSV() throws  IOException;
    public void coinColumn_commonCSV() throws  IOException;
    public void follow() throws  IOException;
    public void addLike() throws  IOException;
    public void addFavorite() throws  IOException;
    public void addView() throws  IOException;
    public String allContinentNames();
    public String count(String s);
    public String joinSelect();
    public String allTableOrderByMid(String s1,String s2,String order);
    public String continentsWithCountryCount();
    public String FullInformationOfMoviesRuntime(int min, int max);
    public String findMovieById(int id);
    public String findMoviesByTitleLimited10(String title);
    public void deleteDataFromTable() throws IOException;
    public void insertDanmu() throws IOException;
    public void update() throws IOException;

}
