package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
)

func printParameters(params map[string]string) {
	for key, value := range params {
		if key == "password" {
			value = "****" // Mask the password
		}
		if value != "" { // Print only if the value is not empty
			fmt.Printf("%s: %s\n", key, value)
		}
	}
}

func printActions() {
	fmt.Println("Actions")

	fmt.Println("\nMigrate")

	fmt.Println("\nExecutes SQL migration scripts from the specified folder against the target database.")
	fmt.Println("./dataprox.exe -database=Database_type -user=root -password=Pass123 -host=127.0.0.1 -port=3306 -folderpath=/path/to/scripts -action=migrate -dbname=mydb -exportfile=result.log -tag=dev")
	fmt.Println("Description:")
	fmt.Println("Executes .sql files in the folder by filtering them using the specified tag (if provided). The system tracks the already executed files using a hash to avoid re-running the same script.")

	fmt.Println("\nClear")

	fmt.Println("\nRemoves data from a specific table in the database.")
	fmt.Println("./dataprox.exe -database=Database_type -user=root -password=Pass123 -host=127.0.0.1 -port=3306 -folderpath=/path/to/scripts -action=clear -dbname=mydb -exportfile=result.log")
	fmt.Println("Description:")
	fmt.Println("Drops the tbl_version table that tracks migrations. This is useful for resetting migration tracking.")

	fmt.Println("\nClearall")

	fmt.Println("\nRemoves all tables from the database.")
	fmt.Println("./dataprox.exe -database=Database_type -user=root -password=Pass123 -host=127.0.0.1 -port=3306 -folderpath=/path/to/scripts -action=clearall -dbname=mydb -exportfile=result.log")
	fmt.Println("Description:")
	fmt.Println("Drops all tables in the target database, effectively clearing the entire database.")

	fmt.Println("\nCheck")

	fmt.Println("\nChecks the status of SQL scripts to determine which scripts have been executed or skipped.")
	fmt.Println("./dataprox.exe -database=Database_type -user=root -password=Pass123 -host=127.0.0.1 -port=3306 -folderpath=/path/to/scripts -action=check -dbname=mydb -exportfile=result.log")
	fmt.Println("Description:")
	fmt.Println("Compares the executed scripts in the tbl_version with the scripts in the provided folder and identifies unexecuted or missing scripts.")

	fmt.Println("\nSnapshot")

	fmt.Println("\nGenerates a SQL snapshot of the current database structure.")
	fmt.Println("./dataprox.exe -database=Database_type -user=root -password=Pass123 -host=127.0.0.1 -port=3306 -folderpath=/path/to/scripts -action=snapshot -dbname=mydb -statefile=before_migration.sql")
	fmt.Println("Description:")
	fmt.Println("Generates SQL create table scripts for all tables in the database and saves them to the specified statefile.")

	fmt.Println("\nDelta")

	fmt.Println("\nCompares two SQL scripts and generates a diff.")
	fmt.Println("./dataprox.exe -database=Database_type -user=root -password=Pass123 -host=127.0.0.1 -folderpath=/path/to/scripts -action=delta -prescript=before.sql -postscript=after.sql -diffscript=diff.sql")
	fmt.Println("Description:")
	fmt.Println("Compares the pre-script and post-script and generates a diff script that contains the differences between the two. Useful for comparing database changes before and after migrations.")
}

func printHelp(dbType string) {
	switch dbType {
	case "db2":
		fmt.Println("EX: dataprox.exe -database=db2 -action=migrate -user=db2admin -port=25000 -password=12345 -host=your_host -folderpath=C:\\path\\to\\scripts -dbname=sample -exportfile=output.txt -tag=")
	case "mssql":
		fmt.Println("Ex: dataprox.exe -database=mssql -action=migrate -user=username -password=12345 -host=localhost -folderpath=C:\\path\\to\\scripts -dbname=sample -exportfile=output.txt -tag= -port=5432")
	case "mysql":
		fmt.Println("Ex: dataprox.exe -database=mysql -action=migrate -user=root -port=3305 -password=12345 -host=localhost -folderpath=C:\\path\\to\\scripts -dbname=sample -exportfile=output.txt -tag=")
	case "postgres":
		fmt.Println("Ex: dataprox.exe -database=postgres -action=migrate -user=postgres -port=5432 -password=12345 -host=localhost -folderpath=C:\\path\\to\\scripts -dbname=migration -exportfile=output.txt -tag=")
	case "oracle":
		fmt.Println("EX: dataprox.exe -database=oracle -action=migrate -user=system -port=1522 -password=12345 -host=oracle_hostname -folderpath=C:\\path\\to\\scripts -dbname=mydatabase -exportfile=outputfile.txt -service=XE -walletLocation= -tag=")
	case "sqlite":
		fmt.Println("Ex: dataprox.exe -database=sqlite -action=migrate -folderpath=C:\\path\\to\\scripts -dbpath=path\\to\\database -exportfile=output.txt -tag=")
	case "singlestore":
		fmt.Println("Ex: dataprox.exe -database=singlestore -action=migrate -user=root -port=3306 -password=12345 -host=127.0.0.1(prefer) -folderpath=C:\\path\\to\\scripts -dbname=sample -exportfile=output.txt -tag=")
	case "cassandra":
		fmt.Println("Ex: dataprox.exe -database=cassandra -action=migrate -keyspace=your_keyspace -host=127.0.0.1(prefer) -folderpath=C:\\path\\to\\scripts -exportfile=output.txt -tag=")
	case "mariadb":
		fmt.Println("Ex: dataprox.exe -database=mariadb -action=migrate -user=root -port=3305 -password=12345 -host=localhost -folderpath=C:\\path\\to\\scripts -dbname=sample -exportfile=output.txt -tag=")
	default:
		fmt.Println("Unsupported database type. Please use db2, mssql, mysql, sqlite, singlestore, cassandra or postgres.")
	}
	fmt.Println("Description:")
	fmt.Println("Executes .sql files in the folder by filtering them using the specified tag (if provided). The system tracks the already executed files using a hash to avoid re-running the same script.")
}

func main() {
	// Check if there are enough arguments
	if len(os.Args) < 2 {
		fmt.Println("Usage: <exe> -help")
		return
	}

	// Get the action and db type from command line arguments
	action := os.Args[1]

	// Check if help is requested
	if action == "-help" {
		printActions()
		return
	}

	if len(os.Args) < 3 {
		fmt.Println("Usage: <exe> migrate <db_type> -help")
		return
	}

	dbTypeArg := os.Args[2]

	// Check if help is requested for a specific action
	if len(os.Args) > 3 && os.Args[3] == "-help" {
		if action == "migrate" {
			printHelp(dbTypeArg)
			return
		}
	}

	// Flag definitions
	dbType := flag.String("database", "", "Type of database (postgres/mssql/mysql/mariadb/oracle/db2)")
	host := flag.String("host", "", "Server name")
	port := flag.String("port", "", "Database port")
	database := flag.String("dbname", "", "Database name")
	username := flag.String("user", "", "Database username")
	password := flag.String("password", "", "Database password")
	folderPath := flag.String("folderpath", "", "Main folder path")
	actionFlag := flag.String("action", "", "Action to perform (e.g., migrate)")
	tag := flag.String("tag", "", "Tag to filter SQL scripts (optional)")
	exportFile := flag.String("exportfile", "", "File to export the output")
	statefile := flag.String("statefile", "", "Output SQL file name")
	service := flag.String("service", "", "Database service name")
	walletLocation := flag.String("walletLocation", "", "Database wallet location")
	keyspace := flag.String("keyspace", "", "Keyspace name")
	prescript := flag.String("prescript", "", "First SQL script file name")
	postscript := flag.String("postscript", "", "Second SQL script file name")
	difference := flag.String("diffscript", "", "Difference file name")
	dbPath := flag.String("dbpath", "", "Database path")

	// Parse command-line flags
	flag.Parse()

	// Set dbType from the positional argument if not set via flags
	if *dbType == "" {
		*dbType = dbTypeArg
	}

	// Collect parameters in a map
	params := map[string]string{
		"database":       *dbType,
		"host":           *host,
		"port":           *port,
		"dbname":         *database,
		"user":           *username,
		"password":       *password,
		"folderpath":     *folderPath,
		"action":         *actionFlag,
		"tag":            *tag,
		"exportfile":     *exportFile,
		"prestatefile":   *statefile,
		"service":        *service,
		"walletLocation": *walletLocation,
		"keyspace":       *keyspace,
		"script1":        *prescript,
		"script2":        *postscript,
		"difference":     *difference,
		"dbpath":         *dbPath,
	}

	// Print the parameters
	printParameters(params)

	// Create log file
	logFile, err := os.Create(*exportFile)
	if err != nil {
		fmt.Println("Error creating export file:", err)
		return
	}
	defer logFile.Close()

	logger := log.New(io.MultiWriter(os.Stdout, logFile), "", log.LstdFlags)

	// Switch based on dbType
	switch *dbType {
	case "postgres":
		postgres(*host, *port, *database, *username, *password, *folderPath, *actionFlag, *statefile, *prescript, *postscript, *difference, *tag, logger)
	case "mssql":
		logFile, err := os.Create(*exportFile)
		if err != nil {
			fmt.Println("Error creating export file:", err)
			return
		}
		defer logFile.Close()

		logger := log.New(io.MultiWriter(os.Stdout, logFile), "", log.LstdFlags)

		mssql(*host, *port, *database, *username, *password, *folderPath, *actionFlag, *statefile, *prescript, *postscript, *difference, *tag, logger)
	case "oracle":
		oracle(*host, *port, *database, *username, *password, *folderPath, *actionFlag, *statefile, *walletLocation, *service, *prescript, *postscript, *difference, *tag, logger)
	case "db2":
		db2(*host, *port, *database, *username, *password, *folderPath, *actionFlag, *statefile, *prescript, *postscript, *difference, *tag, logger)
	case "mysql":
		mysql(*host, *port, *database, *username, *password, *folderPath, *actionFlag, *statefile, *prescript, *postscript, *difference, *tag, logger)
	case "mariadb":
		mariadb(*host, *port, *database, *username, *password, *folderPath, *actionFlag, *statefile, *prescript, *postscript, *difference, *tag, logger)
	case "cassandra":
		if *host == "" || *folderPath == "" || *actionFlag == "" || *keyspace == "" || *exportFile == "" {
			fmt.Println("Missing required arguments for Cassandra")
			flag.PrintDefaults()
			return
		}
		logFile, err := os.Create(*exportFile)
		if err != nil {
			fmt.Println("Error creating export file:", err)
			return
		}
		defer logFile.Close()

		logger := log.New(io.MultiWriter(os.Stdout, logFile), "", log.LstdFlags)
		if *actionFlag == "delta" && (*prescript == "" || *postscript == "") {
			fmt.Println("Please provide all required flags for 'delta' action: -script1 and -script2")
			flag.Usage()
			return
		}

		if *actionFlag == "delta" {
			if err := cassandraProcessFiles(*folderPath, *prescript, *postscript, *difference); err != nil {
				fmt.Println("Error processing SQL scripts:", err)
				return
			}
		}

		cassandra([]string{*host}, *keyspace, *folderPath, *actionFlag, *statefile, *prescript, *postscript, *difference, *tag, logger)
	case "singlestore":
		singlestore(*host, *port, *database, *username, *password, *folderPath, *actionFlag, *statefile, *prescript, *postscript, *difference, *tag, logger)
	case "sqlite":
		sqlite(*dbPath, *folderPath, *actionFlag, *tag, *prescript, *postscript, *difference, logger, *statefile)
	default:
		fmt.Println("Invalid database type. Please enter a valid option.")
	}
}
