import java.sql.*;
import java.util.Locale;
import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.JobStatistics.QueryStatistics;


public class Main {
    public static void main(String args[]) {
        String input = args[0];
        Connection c = null;
        Statement stmt = null;

        BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId("york-cdf-start")
                .build().getService();
        try {
            Class.forName("org.postgresql.Driver");
            c = DriverManager
                    .getConnection("deleted",
                            "deleted", "deleted");

            System.out.println("Opened database successfully");

            stmt = c.createStatement();
            ResultSet rs = stmt.executeQuery(
                    "SELECT f.film_id, f.title, f.description, f.release_year, f.language_id, f.rental_duration, f.rental_rate, f.length,\n" +
                            "f.replacement_cost, f.rating, f.last_update, f.special_features, f.fulltext, c.name AS category_name, a.first_name, a.last_name\n" +
                            "FROM film f\n" +
                            "JOIN film_actor fa ON f.film_id = fa.film_id\n" +
                            "JOIN actor a on fa.actor_id = a.actor_id\n" +
                            "JOIN film_category fc on fc.film_id = f.film_id\n" +
                            "JOIN category c on fc.category_id = c.category_id\n" +
                            "WHERE (first_name LIKE '" + input + "' OR last_name LIKE '" + input + "')\n" +
                            "OR (first_name LIKE '" + input + "' AND last_name LIKE '" + input + "')\n" +
                            "LIMIT 250"
            );

            //String[] catarr = new String[250];
            //String[] newcatarr = new String[250];
            //int i = 0;
            //String catname;

            while (rs.next()) {
                int film_id = rs.getInt("film_id");
                String title = rs.getString("title").toUpperCase(Locale.ROOT);
                String description = rs.getString("description");
                int release_year = rs.getInt("release_year");
                int language_id = rs.getInt("language_id");
                int rental_duration = rs.getInt("rental_duration");
                float rental_rate = rs.getFloat("rental_rate");
                int length = rs.getInt("length");
                float replacement_cost = rs.getFloat("replacement_cost");
                String rating = rs.getString("rating");
                String last_update = rs.getString("last_update");
                String special_features = rs.getString("special_features");
                String fulltext = rs.getString("fulltext");
//                catname = rs.getString("category_name");
//                catarr[i] = title;
//                if (catname.charAt(0)=='D') {
//                    newcatarr[i] = catname.toUpperCase(Locale.ROOT);
//                }
//                else if (catname.charAt(0)=='N') {
//                    newcatarr[i] = catname.toLowerCase(Locale.ROOT);
//                }
//                else {
//                    newcatarr[i] = catname;
//                }
                String catname = "";
                if (rs.getString("category_name").substring(0,1).equalsIgnoreCase("D")) {
                    catname = rs.getString("category_name").toUpperCase();
                } else if (rs.getString("category_name").substring(0,1).equalsIgnoreCase("N")) {
                    catname = rs.getString("category_name").toLowerCase();
                } else {
                    catname = rs.getString("category_name");
                }
                String first_name = rs.getString("first_name");
                String last_name = rs.getString("last_name");
                System.out.println(film_id);
                System.out.println(title);
                System.out.println(description);
                System.out.println(release_year);
                System.out.println(language_id);
                System.out.println(rental_duration);
                System.out.println(rental_rate);
                System.out.println(length);
                System.out.println(replacement_cost);
                System.out.println(rating);
                System.out.println(last_update);
                System.out.println(special_features);
                System.out.println(fulltext);
                System.out.println(catname);
                System.out.println(first_name);
                System.out.println(last_name);

                final String INSERT_FILM =
                        "INSERT INTO `york-cdf-start.final_james_thomson.final-java` (film_id, title, description, " +
                                "release_year, language_id, rental_duration, rental_rate, length, replacement_cost," +
                                "rating, last_update, special_features, fulltext, category_name, first_name, last_name)" +
                                "VALUES (" + film_id + ", '" + title + "', '" + description + "', " + release_year +
                                ", " + language_id + ", " + rental_duration + ", " + rental_rate + ", " + length +
                                ", " + replacement_cost + ", '" + rating + "', '" + last_update +
                                "', '" + special_features + "', \"" + fulltext + "\", '" + catname + "', '" + first_name +
                                "', '" + last_name + "');";
                QueryJobConfiguration queryConfig =
                        QueryJobConfiguration.newBuilder(INSERT_FILM).build();

                Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).build());
                queryJob = queryJob.waitFor();
                if (queryJob == null) {
                    throw new Exception("job no longer exists");
                }

                if (queryJob.getStatus().getError() != null) {
                    throw new Exception(queryJob.getStatus().getError().toString());
                }

                //QueryStatistics stats = queryJob.getStatistics();
                //Long rowsInserted = stats.getDmlStats().getInsertedRowCount();
                //System.out.printf("%d rows inserted\n", rowsInserted);
            }
            rs.close();
            stmt.close();
            c.close();
        } catch (Exception e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        System.out.println("Operation done successfully");
    }
}

