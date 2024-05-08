package edu.canisius.cyb600.lab4.database;

import edu.canisius.cyb600.lab4.dataobjects.Film;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Posgres Implementation of the db adapter.
 */
public class PostgresDBAdapter extends AbstractDBAdapter {

    public PostgresDBAdapter(Connection conn) {
        super(conn);
    }

    @Override
    public List<String> getAllDistinctCategoryNames() {
        try (Statement statement = conn.createStatement()) {
            //This statement is easy
            //Select * from actor is saying "Return all Fields for all rows in films". Because there
            //is no "where clause", all rows are returned
            ResultSet results = statement.executeQuery("Select name from category");
            //Initialize an empty List to hold the return set of films.
            List<String> Names = new ArrayList<>();
            //Loop through all the results and create a new Film object to hold all its information
            while (results.next()) {
                Names.add(results.getString("NAME"));
            }
            //Return all the films.
            return Names;
        } catch (SQLException exception) {
            exception.printStackTrace();
        }
        return new ArrayList<>();
    }

    @Override
    public List<Film> getAllFilmsWithALengthLongerThanX(int length){
        try (Statement statement = conn.createStatement()) {
            //This statement is easy
            //Select * from actor is saying "Return all Fields for all rows in films". Because there
            //is no "where clause", all rows are returned
            ResultSet results = statement.executeQuery("Select name from category");
            //Initialize an empty List to hold the return set of films.
            List<Film> films = new ArrayList<>();
            //Loop through all the results and create a new Film object to hold all its information
            while (results.next()) {

            }
            //Return all the films.
            return films;
        } catch (SQLException exception) {
            exception.printStackTrace();
        }
        return new ArrayList<>();
    }

}
}


