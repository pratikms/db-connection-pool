package main

import (
	"database/sql"
	"log"
	"sync"

	"github.com/pratikms/dbconnectionpool/pool"
)

func addConnectionBackToPool(db *sql.DB) {

}

func main() {
	pool, err := pool.NewConnectionPool("sqlite3", "test.db", 3)
	if err != nil {
		log.Println(err)
	}
	defer pool.Close()
	log.Printf("%+v\n", pool)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Println("Attempting to get connection for CREATE TABLE")
		db, err := pool.Get()
		if err != nil {
			log.Println(err)
		}
		defer db.Close()
		log.Printf("DB: %+v\n", db)

		_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS users (
		    id INTEGER PRIMARY KEY,
		    name TEXT,
		    email TEXT
		)`)
		if err != nil {
			log.Fatal(err)
		}
		log.Println("created successfully")

		_, err = db.Exec("DELETE FROM users")
		if err != nil {
			log.Fatal(err)
		}

	}()

	wg.Wait()

	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Println("Attempting to get connection for INSERT entry for John")
		db2, err := pool.Get()
		if err != nil {
			log.Println(err)
		}
		defer pool.Add(db2)
		log.Printf("DB2: %+v\n", db2)

		// Insert a row into the users table
		result, err := db2.Exec("INSERT INTO users(name, email) VALUES (?, ?)", "John Doe", "john@example.com")
		if err != nil {
			log.Fatal(err)
		}

		// Print the number of rows affected by the insert statement
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("%d row(s) inserted.\n", rowsAffected)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Println("Attempting to get connection for INSRET entry for Bob")
		db3, err := pool.Get()
		if err != nil {
			log.Println(err)
		}
		defer pool.Add(db3)
		log.Printf("BEFORE DB3: %+v\n", db3)

		// Insert a row into the users table
		result, err := db3.Exec("INSERT INTO users(name, email) VALUES (?, ?)", "Bob Johnson", "bob@example.com")
		if err != nil {
			log.Fatal(err)
		}

		// Print the number of rows affected by the insert statement
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("%d row(s) inserted.\n", rowsAffected)

		// time.Sleep(3 * time.Second)

		// err = pool.Add(db3)
		// if err != nil {
		// 	log.Println(err)
		// }
		// log.Printf("AFTER DB3: %+v\n", db3)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Println("Attempting to get connection for INSERT entry for Carol")
		db4, err := pool.Get()
		if err != nil {
			log.Println(err)
		}
		defer pool.Add(db4)
		log.Printf("DB4: %+v\n", db4)

		// Insert a row into the users table
		result, err := db4.Exec("INSERT INTO users(name, email) VALUES (?, ?)", "Carol Davis", "carol@example.com")
		if err != nil {
			log.Fatal(err)
		}

		// Print the number of rows affected by the insert statement
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("%d row(s) inserted.\n", rowsAffected)

	}()

	wg.Wait()

	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Println("Attempting to get connection for PRINTING all users")
		db5, err := pool.Get()
		if err != nil {
			log.Println(err)
		}
		defer pool.Add(db5)
		log.Printf("DB5: %+v\n", db5)

		// Execute a SELECT statement to get all rows from the users table
		rows, err := db5.Query("SELECT * FROM users")
		if err != nil {
			log.Fatal(err)
		}
		defer rows.Close()

		// Iterate over the rows and print their data
		for rows.Next() {
			var id int
			var name string
			var email string
			err := rows.Scan(&id, &name, &email)
			if err != nil {
				log.Fatal(err)
			}
			log.Printf("id=%d, name=%s, email=%s\n", id, name, email)
		}

		// Check for errors during iteration
		err = rows.Err()
		if err != nil {
			log.Fatal(err)
		}

	}()

	wg.Wait()
}
