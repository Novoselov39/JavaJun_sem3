package ru.gb.lesson3.hw;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class Homework {

  /**
   * С помощью JDBC, выполнить следующие пункты:
   * 1. Создать таблицу Person (скопировать код с семниара)
   * 2. Создать таблицу Department (id bigint primary key, name varchar(128) not null)
   * 3. Добавить в таблицу Person поле department_id типа bigint (внешний ключ)
   * 4. Написать метод, который загружает Имя department по Идентификатору person
   * 5. * Написать метод, который загружает Map<String, String>, в которой маппинг person.name -> department.name
   *   Пример: [{"person #1", "department #1"}, {"person #2", "department #3}]
   * 6. ** Написать метод, который загружает Map<String, List<String>>, в которой маппинг department.name -> <person.name>
   *   Пример:
   *   [
   *     {"department #1", ["person #1", "person #2"]},
   *     {"department #2", ["person #3", "person #4"]}
   *   ]
   *
   *  7. *** Создать классы-обертки над таблицами, и в пунктах 4, 5, 6 возвращать объекты.
   */
public static void main(String[] args) {
  String sqlPerson = """
        create table person (
          id bigint,
          name varchar(256),
          age integer,
          department_id bigint REFERENCES department(id),
          active boolean
        )
        """;
  String sqlDeprtmen = """
        create table department (
          id bigint primary key,
          name varchar(128) not null
        )
        """;
  try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:test")) {

    createTable(connection,sqlDeprtmen);
    createTable(connection,sqlPerson);
    insertDataDepartment(connection,4);
    insertDataPerson(connection, 10);
    selectDataPerson(connection);
    System.out.println("--------------");
    selectDataDepartment(connection);
    System.out.println("--------------");
    System.out.println("Person 5 из " + getPersonDepartmentName(connection,5));

  } catch (SQLException e) {
    System.err.println("Во время подключения произошла ошибка: " + e.getMessage());
  }
}

private static void createTable(Connection connection, String sql) throws SQLException {
  try (Statement statement = connection.createStatement()) {
    statement.execute(sql);
  } catch (SQLException e) {
    System.err.println("Во время создания таблицы произошла ошибка: " + e.getMessage());
    throw e;
  }
}

private static void insertDataDepartment(Connection connection, int count) throws SQLException {
  try (Statement statement = connection.createStatement()) {
    StringBuilder insertQuery = new StringBuilder("insert into department(id, name) values\n");
    for (int i = 1; i <= count; i++) {
      int age = ThreadLocalRandom.current().nextInt(20, 60);
      boolean active = ThreadLocalRandom.current().nextBoolean();
      insertQuery.append(String.format("(%s, '%s')", i, "Department #" + i));

      if (i !=count) {
        insertQuery.append(",\n");
      }
    }

    int insertCount = statement.executeUpdate(insertQuery.toString());
    System.out.println("Вставлено строк: " + insertCount);
  }
}

  private static void insertDataPerson(Connection connection, int count) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      StringBuilder insertQuery = new StringBuilder("insert into person(id, name, age, department_id, active) values\n");
      for (int i = 1; i <= count; i++) {
        int age = ThreadLocalRandom.current().nextInt(20, 60);
        int departmentId = ThreadLocalRandom.current().nextInt(1, 5);
        boolean active = ThreadLocalRandom.current().nextBoolean();
        insertQuery.append(String.format("(%s, '%s', %s, %s,%s)", i, "Person #" + i, age, departmentId, active));

        if (i != count) {
          insertQuery.append(",\n");
        }
      }

      int insertCount = statement.executeUpdate(insertQuery.toString());
      System.out.println("Вставлено строк: " + insertCount);
    }
  }

private static void updateData(Connection connection) throws SQLException {
  try (Statement statement = connection.createStatement()) {
    int updateCount = statement.executeUpdate("update person set active = true where id > 5");
    System.out.println("Обновлено строк: " + updateCount);
  }
}

// static Optional<String> selectNameById(long id) {
//   ...
// }

private static List<String> selectNamesByAge(Connection connection, String age) throws SQLException {
//    try (Statement statement = connection.createStatement()) {
//      statement.executeQuery("select name from person where age = " + age);
//      // where age = 1 or 1=1
//    }

  try (PreparedStatement statement =
               connection.prepareStatement("select name from person where age = ?")) {
    statement.setInt(1, Integer.parseInt(age));
    ResultSet resultSet = statement.executeQuery();

    List<String> names = new ArrayList<>();
    while (resultSet.next()) {
      names.add(resultSet.getString("name"));
    }
    return names;
  }
}

private static void selectDataPerson(Connection connection) throws SQLException {
  try (Statement statement = connection.createStatement()) {
    ResultSet resultSet = statement.executeQuery("""
        select id, name, age, department_id
        from person""");
//        where active is true""");

    while (resultSet.next()) {
      long id = resultSet.getLong("id");
      String name = resultSet.getString("name");
      int age = resultSet.getInt("age");
      long department = resultSet.getLong("department_id");
      // persons.add(new Person(id, name, age))
      System.out.println("Найдена строка: [id = " + id + ", name = " + name + ", age = " + age + ", depaertment = " + department + "]");
    }
  }
}
  private static void selectDataDepartment(Connection connection) throws SQLException {
//    System.out.println("go");
    try (Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery("""
        select id, name
        from department """);

      while (resultSet.next()) {
        long id = resultSet.getLong("id");
        String name = resultSet.getString("name");
        // persons.add(new Person(id, name, age))
        System.out.println("Найдена строка: [id = " + id + ", name = " + name + "]");
      }
    }
  }

  /**
   * Пункт 4
   */
  private static String getPersonDepartmentName(Connection connection, long personId) throws SQLException {
    // FIXME: Ваш код тут
    StringBuilder sql = new StringBuilder("select department_id from person where id =");
    sql.append(personId);
    long department = 1;
    String nameDepartment="";

    try (Statement statement = connection.createStatement()) {
      ResultSet setPerson = statement.executeQuery(sql.toString());


      while (setPerson.next()) {
        department = setPerson.getLong("department_id");
      }
      ResultSet setDepartment = statement.executeQuery("select id, name from department");
      while (setDepartment.next()) {
        if (setDepartment.getLong("id") == department){
          nameDepartment=setDepartment.getString("name");
          break;
        }
      }
      }



    return nameDepartment;
  }

  /**
   * Пункт 5
   */
  private static Map<String, String> getPersonDepartments(Connection connection) throws SQLException {
    // FIXME: Ваш код тут
    throw new UnsupportedOperationException();
  }

  /**
   * Пункт 6
   */
  private static Map<String, List<String>> getDepartmentPersons(Connection connection) throws SQLException {
    // FIXME: Ваш код тут
    throw new UnsupportedOperationException();
  }

}
