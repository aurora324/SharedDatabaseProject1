import java.io.IOException;

public interface DataManipulation {


    public void addUsers() throws IOException;
    public void addFollow();
    public String allContinentNames();
    public String continentsWithCountryCount();
    public String FullInformationOfMoviesRuntime(int min, int max);
    public String findMovieById(int id);
    public String findMoviesByTitleLimited10(String title);
}
