package example;

import java.nio.file.Paths;

/**
 *
 *
 * @author Beat Hoermann
 */
public final class Main {

	public static void main(String[] args) {
		try (PersonDB db = new PersonDB(Paths.get(
									"pathToPersonDatabaseLayoutFile"), -1, false, 0)) {
		}
	}

}
