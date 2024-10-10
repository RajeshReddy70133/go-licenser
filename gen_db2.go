package main

import (
	"crypto/md5"
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/ibmdb/go_ibm_db" // Import the IBM Db2 driver package
	"github.com/jedib0t/go-pretty/table"
)

func db2(host, port, database, username, password, folderPath, action, statefile, prescript, postscript, difference, tag string, logger *log.Logger) {
	switch action {
	case "migrate":
		prosesing(host, port, database, username, password, folderPath, tag, logger)
	case "clear":
		db2ClearDB(host, port, database, username, password, logger)
	case "clearall":
		db2_clearall(host, port, database, username, password, logger)
	case "check":
		db2_checkScripts(host, port, database, username, password, folderPath, logger)
	case "snapshot":
		db2GenerateMigrationScript(host, port, database, username, password, folderPath, statefile, logger)
	case "delta":
		db2_handleProcessing(folderPath, prescript, postscript, difference)
	default:
		logger.Println("Unsupported action:", action)
	}
}
func prosesing(host, port, database, username, password, folderPath, tag string, logger *log.Logger) {
	connStr := fmt.Sprintf("HOSTNAME=%s;PORT=%s;DATABASE=%s;UID=%s;PWD=%s;", host, port, database, username, password)
	db, err := sql.Open("go_ibm_db", connStr)
	if err != nil {
		logger.Fatalf("Error connecting to IBM Db2 database: %v", err)
	}
	defer db.Close()

	// Create a table if it doesn't exist
	createTableQuery := `
		CREATE TABLE IF NOT EXISTS tbl_version (
			Id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
			Major VARCHAR(10),
			Minor VARCHAR(10),
			Change VARCHAR(10),
			Status VARCHAR(10),
			HashCode VARCHAR(50),
			ExecutionTime TIMESTAMP
		);
	`
	_, err = db.Exec(createTableQuery)
	if err != nil {
		logger.Fatalf("Error creating table: %v", err)
	}

	var scriptIDCounter int

	// Retrieve hash codes from the database table
	hashCodes := db2RetrieveHashCodes(db)
	fmt.Println("Connected to IBM Db2 database")

	// Iterate over the files in the main folder
	var skippedFiles []string // Slice to store skipped file names
	tableWriter := table.NewWriter()
	tableWriter.SetOutputMirror(logger.Writer())
	tableWriter.AppendHeader(table.Row{"Id", "Major", "Minor", "Change", "Status", "Hash Code", "Execution Time"})

	// Reset the scriptID counter to 0 at the start of each run.
	scriptIDCounter = 0

	err = filepath.Walk(folderPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Printf("Error accessing file %s: %v", path, err)
			return nil
		}

		if info.IsDir() || !strings.HasSuffix(info.Name(), ".sql") {
			return nil
		}

		versions, fileName, hashKeys, scripts, tags := db2GetVersion(path)
		majorValue := strings.Split(filepath.Base(filepath.Dir(path)), "_")[0]

		for i, version := range versions {
			// Skip scripts tagged with `tag=dev` if no tag is provided
			if tag == "" && tags[i] == "dev" {
				continue
			}

			// Skip scripts if a tag is provided and it doesn't match the script's tag
			if tag != "" && tags[i] != tag {
				continue
			}

			hk := hashKeys[i]
			script := scripts[i]
			status := ""

			if db2Contains(hashCodes, hk) && !strings.HasPrefix(fileName, "c") {
				status = "skipped"
				skippedFiles = append(skippedFiles, fileName)
				logger.Printf("Execution skipped due to matching hash code: %s, change: %s\n", filepath.Base(fileName), version)
			} else {
				sqlScript := db2RemoveComments(script)
				statements := strings.Split(sqlScript, ";")
				success := true

				for _, statement := range statements {
					if strings.TrimSpace(statement) != "" {
						_, err := db.Exec(statement)
						if err != nil {
							log.Printf("Error executing script %s: %v", fileName, err)
							success = false
							break
						}
					}
				}

				if success {
					status = "success"
					logger.Printf("Execution success for script: %s, change: %s\n", filepath.Base(fileName), version)
				} else {
					status = "failed"
					logger.Printf("Execution failed for script: %s, change: %s\n", filepath.Base(fileName), version)
				}
			}

			minorValue := strings.Split(strings.Split(fileName, "_")[0], "_")[0]

			scriptIDCounter++ // Increment the script ID counter.

			// Insert/update the record only if the script is not tagged with "dev" or if it matches the provided tag
			if (tag == "" && tags[i] != "dev") || (tag != "" && tags[i] == tag) {
				existingRecordID := db2CheckExistingRecord(db, majorValue, minorValue, version)
				executionTime := time.Now().Format("2006-01-02 15:04:05")

				if status != "failed" {
					if existingRecordID != -1 {
						_, err = db.Exec(
							"UPDATE tbl_version SET Status=?, HashCode=?, ExecutionTime=? WHERE Id=?",
							status, hk, executionTime, existingRecordID)
					} else {
						_, err = db.Exec(
							"INSERT INTO tbl_version (Major, Minor, Change, Status, HashCode, ExecutionTime) VALUES (?, ?, ?, ?, ?, ?)",
							majorValue, minorValue, version, status, hk, executionTime)
					}

					if err != nil {
						logger.Fatalf("Error updating/inserting record: %v", err)
					}
				}

				tableWriter.AppendRow([]interface{}{scriptIDCounter, majorValue, minorValue, version, status, hk, executionTime})
				hashCodes = append(hashCodes, hk)
			}
		}
		return nil
	})

	if err != nil {
		logger.Fatalf("Error iterating over files: %v", err)
	}

	tableWriter.Render()
}

func db2RetrieveHashCodes(db *sql.DB) []string {
	var hashCodes []string
	rows, err := db.Query("SELECT HashCode FROM tbl_version")
	if err != nil {
		log.Fatalf("Error retrieving hash codes: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var hashCode string
		if err := rows.Scan(&hashCode); err != nil {
			log.Fatalf("Error scanning hash code: %v", err)
		}
		hashCodes = append(hashCodes, hashCode)
	}

	return hashCodes
}

func db2RemoveComments(sqlScript string) string {
	lines := strings.Split(sqlScript, "\n")
	var cleanedLines []string
	for _, line := range lines {
		if !strings.HasPrefix(line, "--") {
			cleanedLines = append(cleanedLines, line)
		}
	}
	return strings.Join(cleanedLines, "\n")
}

func db2GetVersion(filePath string) ([]string, string, []string, []string, []string) {
	content, err := ioutil.ReadFile(filePath)
	if err != nil {
		log.Fatalf("Error reading file %s: %v", filePath, err)
	}

	lines := strings.Split(string(content), "\n")
	var versions []string
	var hashKeys []string
	var scripts []string
	var tags []string
	var script string

	for _, line := range lines {
		if strings.HasPrefix(line, "--start version:") {
			parts := strings.SplitN(strings.TrimSpace(strings.TrimPrefix(line, "--start version:")), ";tag=", 2)
			version := parts[0]
			tag := ""
			if len(parts) == 2 {
				tag = parts[1]
			}
			versions = append(versions, version)
			tags = append(tags, tag)
		} else if strings.HasPrefix(line, "--end version:") {
			hashKey := fmt.Sprintf("%x", md5.Sum([]byte(script)))
			hashKeys = append(hashKeys, hashKey)
			scripts = append(scripts, script)
			script = ""
		} else {
			script += line + "\n"
		}
	}

	fileName := filepath.Base(filePath)
	return versions, fileName, hashKeys, scripts, tags
}

func db2Contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

func db2CheckExistingRecord(db *sql.DB, majorValue, minorValue, version string) int {
	var id int
	err := db.QueryRow("SELECT Id FROM tbl_version WHERE Major=? AND Minor=? AND Change=?", majorValue, minorValue, version).Scan(&id)
	if err != nil {
		return -1
	}
	return id
}

// Function to clear the database (drop the specified table).
func db2ClearDB(host, port, database, username, password string, logger *log.Logger) {
	// Establish a database connection
	connStr := fmt.Sprintf("HOSTNAME=%s;PORT=%s;DATABASE=%s;UID=%s;PWD=%s;", host, port, database, username, password)
	db, err := sql.Open("go_ibm_db", connStr)
	if err != nil {
		logger.Fatalf("Error connecting to DB2 database: %v", err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		logger.Fatal("Error pinging database: ", err)
	}

	tableName := "tbl_version"

	// Drop the specified table
	dropQuery := fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
	_, err = db.Exec(dropQuery)
	if err != nil {
		logger.Fatal("Error dropping table ", tableName, ": ", err)
	}

	logger.Println("Dropped table: ", tableName)
}

func db2_clearall(host, port, database, username, password string, logger *log.Logger) {

	// Establish a database connection (using the appropriate driver)
	// You'll need to install the DB2 driver for Go
	// https://www.ibm.com/docs/en/db2/administration/administration-and-development-guide-go?topic=functions-installing-the-ibm-db2-driver-for-go
	connStr := fmt.Sprintf("HOSTNAME=%s;PORT=%s;DATABASE=%s;UID=%s;PWD=%s;", host, port, database, username, password)
	db, err := sql.Open("go_ibm_db", connStr)
	if err != nil {
		logger.Fatalf("Error connecting to DB2 database: %v", err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		logger.Fatal("Error pinging database: ", err)
	}

	// Query to get all table names in the database schema 'public' (assuming)
	query := `
		SELECT NAME 
		FROM SYSIBM.SYSTABLES
		WHERE CREATOR = 'db2admin' AND TYPE = 'T';
	`

	rows, err := db.Query(query)
	if err != nil {
		log.Fatal("Error fetching table names: ", err)
	}
	defer rows.Close()

	// Iterate through each table and drop it
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			log.Fatal("Error scanning table name: ", err)
		}

		dropQuery := fmt.Sprintf("DROP TABLE %s RESTRICT", tableName) // Use RESTRICT instead of CASCADE in DB2
		_, err := db.Exec(dropQuery)
		if err != nil {
			log.Println("Error dropping table ", tableName, ": ", err)
		} else {
			fmt.Println("Dropped table: ", tableName)
		}
	}

	if err := rows.Err(); err != nil {
		log.Fatal("Error iterating table rows: ", err)
	}
}

func db2_checkScripts(host, port, database, username, password, folderPath string, logger *log.Logger) {
	// Establish a database connection
	db, err := sql.Open("go_ibm_db", fmt.Sprintf("HOSTNAME=%s;PORT=%s;DATABASE=%s;UID=%s;PWD=%s;", host, port, database, username, password))
	if err != nil {
		logger.Fatalf("Error connecting to IBM Db2 database: %v", err)
	}
	defer db.Close()

	// Retrieve executed scripts with their version tags and hash codes from the database
	executedScripts := db2_getExecutedScriptsWithHashes(db)

	// Parse scripts from the specified folder with their version tags and hash codes
	folderScripts := make(map[string]map[string]string) // Map of script names to maps of version to hash code
	err = filepath.Walk(folderPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Printf("Error accessing path %q: %v\n", path, err)
			return err
		}
		if !info.IsDir() && strings.HasSuffix(info.Name(), ".sql") {
			versions, fileName, hashCodes := db2_getVersionsAndHashes(path)
			folderScripts[fileName] = make(map[string]string)
			for i, version := range versions {
				folderScripts[fileName][version] = hashCodes[i]
			}
		}
		return nil
	})
	if err != nil {
		logger.Fatalf("Error walking through folder: %v", err)
	}

	// Compare executed scripts and folder scripts
	db2_findAndPrintMissingScripts(executedScripts, folderScripts, logger)
}

func db2_getExecutedScriptsWithHashes(db *sql.DB) map[string]map[string]string {
	// Retrieve executed scripts with their version and hash code from the database
	executedScripts := make(map[string]map[string]string)
	rows, err := db.Query("SELECT Change, HashCode FROM tbl_version")
	if err != nil {
		log.Fatalf("Error retrieving executed scripts and hashes: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var change, hashCode string
		if err := rows.Scan(&change, &hashCode); err != nil {
			log.Fatalf("Error scanning change and hash code: %v", err)
		}
		if _, exists := executedScripts[change]; !exists {
			executedScripts[change] = make(map[string]string)
		}
		executedScripts[change]["HashCode"] = hashCode
	}
	return executedScripts
}

func db2_getVersionsAndHashes(filePath string) ([]string, string, []string) {
	content, err := ioutil.ReadFile(filePath)
	if err != nil {
		log.Fatalf("Error reading file %s: %v", filePath, err)
	}

	lines := strings.Split(string(content), "\n")
	var versions []string
	var hashCodes []string
	var script string
	var tags []string

	for _, line := range lines {
		if strings.HasPrefix(line, "--start version:") {
			parts := strings.SplitN(strings.TrimSpace(strings.TrimPrefix(line, "--start version:")), ";tag=", 2)
			version := parts[0]
			tag := ""
			if len(parts) == 2 {
				tag = parts[1]
			}
			versions = append(versions, version)
			tags = append(tags, tag)
		} else if strings.HasPrefix(line, "--end version:") {
			// Calculate hash
			// Calculate hash code for the script between start and end version markers
			hashKey := fmt.Sprintf("%x", md5.Sum([]byte(script)))
			hashCodes = append(hashCodes, hashKey)
			script = "" // Reset script for next version
		} else {
			script += line + "\n"
		}
	}

	fileName := filepath.Base(filePath)
	return versions, fileName, hashCodes
}

func db2_findAndPrintMissingScripts(executedScripts map[string]map[string]string, folderScripts map[string]map[string]string, logger *log.Logger) {
	for fileName, versions := range folderScripts {
		for version, hashCode := range versions {
			if executedVersion, exists := executedScripts[version]; !exists || executedVersion["HashCode"] != hashCode {
				logger.Printf("Change %s (version %s) with hash code %s has not been executed in tbl_version\n", fileName, version, hashCode)
			}
		}
	}
}

// Function to get table names from the database
func db2GetTableNames(db *sql.DB) ([]string, error) {
	query := "SELECT TABNAME FROM SYSCAT.TABLES WHERE TABSCHEMA = 'YOUR_SCHEMA'"
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tableNames []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, err
		}
		tableNames = append(tableNames, tableName)
	}
	return tableNames, nil
}

// Function to generate SQL script for creating a table
func db2GenerateScriptForTable(db *sql.DB, tableName string) (string, error) {
	query := fmt.Sprintf("SELECT COLNAME, TYPENAME, LENGTH FROM SYSCAT.COLUMNS WHERE TABNAME = '%s' AND TABSCHEMA = 'YOUR_SCHEMA'", tableName)
	rows, err := db.Query(query)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	script := fmt.Sprintf("CREATE TABLE %s (\n", tableName)
	for rows.Next() {
		var columnName, typeName string
		var length int
		if err := rows.Scan(&columnName, &typeName, &length); err != nil {
			return "", err
		}

		script += fmt.Sprintf("    %s %s", columnName, typeName)
		if typeName == "VARCHAR" {
			script += fmt.Sprintf("(%d)", length)
		}
		script += ",\n"
	}
	script = script[:len(script)-2] + "\n);\n\n" // Remove the last comma and add closing bracket
	return script, nil
}

// Function to handle database connection and generate script
func db2GenerateMigrationScript(host, port, database, username, password, folderPath, statefile string, logger *log.Logger) {
	connString := fmt.Sprintf("HOSTNAME=%s;PORT=%s;DATABASE=%s;UID=%s;PWD=%s;", host, port, database, username, password)

	db, err := sql.Open("go_ibm_db", connString)
	if err != nil {
		logger.Println("Error creating connection pool:", err)
		return
	}
	defer db.Close()

	// Check if the database connection is established
	if err = db.Ping(); err != nil {
		logger.Println("Error connecting to the database:", err)
		return
	}

	tableNames, err := db2GetTableNames(db)
	if err != nil {
		logger.Println("Error getting table names:", err)
		return
	}

	var combinedScript string
	for _, tableName := range tableNames {
		script, err := db2GenerateScriptForTable(db, tableName)
		if err != nil {
			logger.Println("Error generating script for table:", err)
			return
		}
		combinedScript += script
	}

	combinedSQLFilePath := filepath.Join(folderPath, statefile)
	file, err := os.Create(combinedSQLFilePath)
	if err != nil {
		logger.Println("Error creating file:", err)
		return
	}
	defer file.Close()

	_, err = file.WriteString(combinedScript)
	if err != nil {
		logger.Println("Error writing to file:", err)
		return
	}

	logger.Println("Script has been generated and saved to the specified file.")
}

// handleProcessing handles the processing of files and prints the result.
func db2_handleProcessing(folderPath, prescript, postscript, difference string) {
	err := db2_processFiles(folderPath, prescript, postscript, difference)
	if err != nil {
		log.Fatalf("Error processing files: %v", err)
	}

	fmt.Println("Differences have been saved to", difference)
}

// processFiles handles the core logic of reading the files, finding differences, and saving the output.
func db2_processFiles(folderPath, prescript, postscript, difference string) error {
	// Construct file paths
	file1Path := filepath.Join(folderPath, prescript)
	file2Path := filepath.Join(folderPath, postscript)
	outputPath := filepath.Join(folderPath, difference)

	// Read the SQL files
	file1Content, err := ioutil.ReadFile(file1Path)
	if err != nil {
		return fmt.Errorf("error reading file1: %w", err)
	}

	file2Content, err := ioutil.ReadFile(file2Path)
	if err != nil {
		return fmt.Errorf("error reading file2: %w", err)
	}

	// Split the contents into individual statements
	statementsFile1 := db2_splitSQLStatements(string(file1Content))
	statementsFile2 := db2_splitSQLStatements(string(file2Content))

	// Find differences
	differences := db2_findDifferences(statementsFile1, statementsFile2)

	// Save the differences to a new SQL file
	return db2_saveDifferencesToFile(outputPath, differences)
}

// splitSQLStatements splits the SQL script into individual statements.
func db2_splitSQLStatements(content string) []string {
	statements := strings.Split(content, ";")
	var trimmedStatements []string
	for _, stmt := range statements {
		trimmedStmt := strings.TrimSpace(stmt)
		if trimmedStmt != "" {
			trimmedStatements = append(trimmedStatements, trimmedStmt+";")
		}
	}
	return trimmedStatements
}

// findDifferences returns the statements that are in file2 but not in file1.
func db2_findDifferences(file1Statements, file2Statements []string) []string {
	file1Set := make(map[string]struct{})
	for _, stmt := range file1Statements {
		file1Set[stmt] = struct{}{}
	}

	var differences []string
	for _, stmt := range file2Statements {
		if _, found := file1Set[stmt]; !found {
			differences = append(differences, stmt)
		}
	}
	return differences
}

// saveDifferencesToFile saves the differences to a file.
func db2_saveDifferencesToFile(filename string, differences []string) error {
	content := strings.Join(differences, "\n\n")
	return ioutil.WriteFile(filename, []byte(content), 0644)
}
