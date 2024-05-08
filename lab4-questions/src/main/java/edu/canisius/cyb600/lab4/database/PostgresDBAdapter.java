package edu.canisius.cyb600.lab4.database;

import edu.canisius.cyb600.lab4.dataobjects.Actor;
import edu.canisius.cyb600.lab4.dataobjects.Film;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Postgres Implementation of the db adapter.
 */
public class PostgresDBAdapter extends AbstractDBAdapter {

    public PostgresDBAdapter(Connection conn) {
        super(conn);
    }

    @Override
    public List<String> getAllDistinctCategoryNames() {
        try (Statement statement = conn.createStatement()) {
            //Execute Query
            ResultSet results = statement.executeQuery("Select name from category");
            List<String> Names = new ArrayList<>();

            //Loop to add names into return list
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

        //preparing statement with parameter
        String sql = "Select * from film where length > ?";

        //establish connection
        try (PreparedStatement statement = conn.prepareStatement(sql)) {

            //add parameter into statement
            statement.setInt(1, length);
            ResultSet results = statement.executeQuery();

            List<Film> films = new ArrayList<>();

            //grab all films since we already filtered shorter ones out with query
            while (results.next()) {
                Film film = new Film();
                film.setFilmId(results.getInt("FILM_ID"));
                film.setTitle(results.getString("TITLE"));
                film.setDescription(results.getString("DESCRIPTION"));
                film.setReleaseYear(results.getString("RELEASE_YEAR"));
                film.setLanguageId(results.getInt("LANGUAGE_ID"));
                film.setRentalDuration(results.getInt("RENTAL_DURATION"));
                film.setRentalRate(results.getDouble("RENTAL_RATE"));
                film.setLength(results.getInt("LENGTH"));
                film.setReplacementCost(results.getDouble("REPLACEMENT_COST"));
                film.setRating(results.getString("RATING"));
                film.setSpecialFeatures(results.getString("SPECIAL_FEATURES"));
                film.setLastUpdate(results.getDate("LAST_UPDATE"));
                //Add film to the array
                films.add(film);
            }
            //Return all the films.
            return films;
        } catch (SQLException exception) {
            exception.printStackTrace();
        }
        return new ArrayList<>();
    }

    @Override
    public List<Actor> getActorsFirstNameStartingWithX(char firstLetter){
        try (Statement statement = conn.createStatement()) {
            ResultSet results = statement.executeQuery("Select * from actor");
            List<Actor> actors = new ArrayList<>();

            while (results.next()) {
                String firstName = results.getString("FIRST_NAME");
                //decided to get all actors and use this if statement to filter based on the first letter
                if (Character.toUpperCase(firstLetter) ==  Character.toUpperCase(firstName.charAt(0))) {
                    Actor actor = new Actor();
                    actor.setActorId(results.getInt("ACTOR_ID"));
                    actor.setFirstName(results.getString("FIRST_NAME"));
                    actor.setLastName(results.getString("LAST_NAME"));
                    actor.setLastUpdate(results.getDate("LAST_UPDATE"));

                    actors.add(actor);
                }
            }
            //Return all the films.
            return actors;
        } catch (SQLException exception) {
            exception.printStackTrace();
        }
        return new ArrayList<>();
    }

    @Override
    public List<Actor> insertAllActorsWithAnOddNumberLastName(List<Actor> actors){
        List<Actor> added = new ArrayList<>();

        for (Actor actor : actors) {
            int nameLength = actor.getLastName().length();
            if ( nameLength % 2 == 1) {
                //code to add an actor
                String sql = "INSERT INTO ACTOR (first_name, last_name) VALUES (? , ? ) returning ACTOR_ID, LAST_UPDATE";
                try (PreparedStatement statement = conn.prepareStatement(sql)) {
                    int i = 1;
                    statement.setString(i++, actor.getFirstName().toUpperCase());
                    statement.setString(i++, actor.getLastName().toUpperCase());
                    ResultSet results = statement.executeQuery();
                    if (results.next()) {
                        actor.setActorId(results.getInt("ACTOR_ID"));
                        actor.setLastUpdate(results.getDate("LAST_UPDATE"));
                    }
                    added.add(actor); //add the actor to the list of "added"
                } catch (SQLException exception) {
                    exception.printStackTrace();
                }
            }
        }
        return added;
    }
}



