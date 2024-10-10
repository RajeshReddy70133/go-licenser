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

	_ "github.com/go-sql-driver/mysql"
	"github.com/jedib0t/go-pretty/table"
)

func mariadb(host, port, database, username, password, folderPath, action, statefile, prescript, postscript, difference, tag string, logger *log.Logger) {
	dbName = database
	switch action {
	case "migrate":
		mariadb_calling(host, port, database, username, password, folderPath, tag, logger)
	case "clear":
		mariadbcleardata(host, port, database, username, password, logger)
	case "clearall":
		mariadbclearAllData(host, port, database, username, password, logger)
	case "check":
		mariadbcheckScripts(host, port, database, username, password, folderPath, logger)
	case "snapshot":
		mariadbGenerateMigrationScript(host, port, database, username, password, folderPath, statefile, logger)
	case "delta":
		mariadb_processFiles(folderPath, prescript, postscript, difference)
	default:
		logger.Println("Unsupported action:", action)
	}
}

func mariadb_calling(host, port, database, username, password, folderPath, tag string, logger *log.Logger) {
	// Establish a database connection
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", username, password, host, port, database))
	if err != nil {
		logger.Fatalf("Error connecting to mariadb database: %v", err)
	}
	defer db.Close()

	// Create a table if it doesn't exist
	createTableQuery := `
		CREATE TABLE IF NOT EXISTS tbl_version (
			Id INT AUTO_INCREMENT PRIMARY KEY,
			Major VARCHAR(10),
			Minor VARCHAR(10),
			ChangeColumn VARCHAR(10),
			Status VARCHAR(10),
			HashCode VARCHAR(50),
			ExecutionTime DATETIME
		);
	`
	_, err = db.Exec(createTableQuery)
	if err != nil {
		logger.Fatalf("Error creating table: %v", err)
	}

	// Retrieve hash codes from the database table
	hashCodes := mariadbRetrieveHashCodes(db)
	logger.Println("Connected to mariadb database")

	// Iterate over the files in the main folder
	var skippedFiles []string // Slice to store skipped file names
	tableWriter := table.NewWriter()
	tableWriter.SetOutputMirror(logger.Writer())
	tableWriter.AppendHeader(table.Row{"Id", "Major", "Minor", "Change", "Status", "Hash Code", "Execution Time"})

	err = filepath.Walk(folderPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			logger.Printf("Error accessing file %s: %v", path, err)
			return nil
		}

		if info.IsDir() || !strings.HasSuffix(info.Name(), ".sql") {
			return nil
		}

		versions, fileName, hashKeys, scripts, tags := mariadbGetVersion(path)
		majorValue := strings.Split(filepath.Base(filepath.Dir(path)), "_")[0]

		for i, version := range versions {
			hk := hashKeys[i]
			script := scripts[i]
			scriptTag := tags[i]
			status := ""

			// Check if the script should be executed based on the provided tag
			if tag != "" {
				if scriptTag != tag {
					continue // Skip scripts that do not match the provided tag
				}
			} else if scriptTag != "" {
				continue // Skip scripts that have a tag if no tag is provided
			}

			if mariadbContains(hashCodes, hk) && !strings.HasPrefix(fileName, "c") {
				status = "skipped"
				skippedFiles = append(skippedFiles, fileName)
				logger.Printf("Execution skipped due to matching hash code: %s, change: %s\n", filepath.Base(fileName), version)
			} else {
				sqlScript := mariadbRemoveComments(script)
				statements := strings.Split(sqlScript, ";")
				success := true

				for _, statement := range statements {
					if strings.TrimSpace(statement) != "" {
						_, err := db.Exec(statement)
						if err != nil {
							logger.Printf("Error executing script %s: %v", fileName, err)
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

			parts := strings.SplitN(fileName, "_", 2)
			minorValue := parts[0]

			existingRecordID := mariadbCheckExistingRecord(db, majorValue, minorValue, version)
			executionTime := time.Now().Format("2006-01-02 15:04:05")

			if status != "failed" {
				if existingRecordID != -1 {
					_, err = db.Exec(
						"UPDATE tbl_version SET Status=?, HashCode=?, ExecutionTime=? WHERE Id=?",
						status, hk, executionTime, existingRecordID)
				} else {
					res, err := db.Exec(
						"INSERT INTO tbl_version (Major, Minor, ChangeColumn, Status, HashCode, ExecutionTime) VALUES (?, ?, ?, ?, ?, ?)",
						majorValue, minorValue, version, status, hk, executionTime)
					if err != nil {
						logger.Fatalf("Error inserting record: %v", err)
					}
					newID, _ := res.LastInsertId()
					existingRecordID = int(newID)
				}

				if err != nil {
					logger.Fatalf("Error updating record: %v", err)
				}
			}

			tableWriter.AppendRow([]interface{}{existingRecordID, majorValue, minorValue, version, status, hk, executionTime})
			hashCodes = append(hashCodes, hk)
		}
		return nil
	})

	if err != nil {
		logger.Fatalf("Error iterating over files: %v", err)
	}

	tableWriter.Render()
}
func mariadbGetTableNames(db *sql.DB) ([]string, error) {
	query := "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE'"
	rows, err := db.Query(query, dbName)
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
func mariadbGenerateScriptForTable(db *sql.DB, tableName string) (string, error) {
	query := "SELECT column_name, column_type FROM information_schema.columns WHERE table_schema = ? AND table_name = ?"
	rows, err := db.Query(query, dbName, tableName)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	script := fmt.Sprintf("CREATE TABLE %s (\n", tableName)
	for rows.Next() {
		var columnName, columnType string
		if err := rows.Scan(&columnName, &columnType); err != nil {
			return "", err
		}

		script += fmt.Sprintf("    %s %s,\n", columnName, columnType)
	}
	script = script[:len(script)-2] + "\n);\n\n" // Remove the last comma and add closing bracket
	return script, nil
}

// Function to handle database connection and generate script
func mariadbGenerateMigrationScript(host, port, database, username, password, folderPath, statefile string, logger *log.Logger) {
	connString := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", username, password, host, port, database)

	db, err := sql.Open("mysql", connString)
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

	tableNames, err := mariadbGetTableNames(db)
	if err != nil {
		logger.Println("Error getting table names:", err)
		return
	}

	var combinedScript string
	for _, tableName := range tableNames {
		script, err := mariadbGenerateScriptForTable(db, tableName)
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

func mariadbRetrieveHashCodes(db *sql.DB) []string {
	var hashCodes []string
	rows, err := db.Query("SELECT HashCode FROM tbl_version")
	if err != nil {
		log.Fatalf("Error retrieving hash codes from the database: %v", err)
		return nil
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

func mariadbRemoveComments(sqlScript string) string {
	lines := strings.Split(sqlScript, "\n")
	var cleanedLines []string
	for _, line := range lines {
		if !strings.HasPrefix(line, "--") {
			cleanedLines = append(cleanedLines, line)
		}
	}
	return strings.Join(cleanedLines, "\n")
}

func mariadbGetVersion(filePath string) ([]string, string, []string, []string, []string) {
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
		if strings.HasPrefix(line, "#start version:") {
			parts := strings.Split(strings.TrimSpace(strings.TrimPrefix(line, "#start version:")), ";")
			version := strings.TrimSpace(parts[0])
			tag := ""
			if len(parts) > 1 {
				tag = strings.TrimSpace(strings.Split(parts[1], "=")[1])
			}
			versions = append(versions, version)
			tags = append(tags, tag)
		} else if strings.HasPrefix(line, "#end version:") {
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

func mariadbContains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

func mariadbCheckExistingRecord(db *sql.DB, majorValue, minorValue, version string) int {
	var id int
	err := db.QueryRow("SELECT Id FROM tbl_version WHERE Major=? AND Minor=? AND ChangeColumn=?", majorValue, minorValue, version).Scan(&id)
	if err != nil {
		return -1
	}
	return id
}

func mariadbcleardata(host, port, database, username, password string, logger *log.Logger) {
	// Establish a database connection
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", username, password, host, port, database))
	if err != nil {
		logger.Fatalf("Error connecting to mariadb database: %v", err)
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

func mariadbclearAllData(host, port, database, username, password string, logger *log.Logger) {
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", username, password, host, port, database))
	if err != nil {
		logger.Fatalf("Error connecting to mariadb database: %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		logger.Fatalf("Error retrieving tables: %v", err)
	}
	defer rows.Close()

	var tableName string
	for rows.Next() {
		err := rows.Scan(&tableName)
		if err != nil {
			logger.Fatalf("Error scanning table name: %v", err)
		}

		_, err = db.Exec("DROP TABLE IF EXISTS " + tableName)
		if err != nil {
			logger.Fatalf("Error dropping table %s: %v", tableName, err)
		}

		logger.Println("Dropped table:", tableName)
	}
}

func mariadbcheckScripts(host, port, database, username, password, folderPath string, logger *log.Logger) {
	// Establish a database connection
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", username, password, host, port, database))
	if err != nil {
		logger.Fatalf("Error connecting to mariadb database: %v", err)
	}
	defer db.Close()

	// Retrieve executed scripts with their version tags and hash codes from the database
	executedScripts := mariadbexecutedScriptsWithHashes(db)

	// Parse scripts from the specified folder with their version tags and hash codes
	folderScripts := make(map[string]map[string]string) // Map of script names to maps of version to hash code
	err = filepath.Walk(folderPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			logger.Printf("Error accessing path %q: %v\n", path, err)
			return err
		}
		if !info.IsDir() && strings.HasSuffix(info.Name(), ".sql") {
			versions, fileName, hashCodes, _, _ := mariadbGetVersion(path)
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
	mariadbprintMissingScripts(executedScripts, folderScripts, logger)
}

func mariadbexecutedScriptsWithHashes(db *sql.DB) map[string]map[string]string {
	// Retrieve executed scripts with their version and hash code from the database
	executedScripts := make(map[string]map[string]string)
	rows, err := db.Query("SELECT ChangeColumn, HashCode FROM tbl_version")
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

func mariadbprintMissingScripts(executedScripts map[string]map[string]string, folderScripts map[string]map[string]string, logger *log.Logger) {
	for fileName, versions := range folderScripts {
		for version, hashCode := range versions {
			if executedVersion, exists := executedScripts[version]; !exists || executedVersion["HashCode"] != hashCode {
				logger.Printf("Change %s (version %s) with hash code %s has not been executed in tbl_version\n", fileName, version, hashCode)
			}
		}
	}
}

// processFiles handles the core logic of reading the files, finding differences, and saving the output.
func mariadb_processFiles(folderPath, prescript, postscript, difference string) error {
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
	statementsFile1 := mariadb_splitSQLStatements(string(file1Content))
	statementsFile2 := mariadb_splitSQLStatements(string(file2Content))

	// Find differences
	differences := mariadb_findDifferences(statementsFile1, statementsFile2)
	fmt.Println("Differences have been saved to", difference)
	// Save the differences to a new SQL file
	return mariadb_saveDifferencesToFile(outputPath, differences)
}

// splitSQLStatements splits the SQL script into individual statements.
func mariadb_splitSQLStatements(content string) []string {
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
func mariadb_findDifferences(file1Statements, file2Statements []string) []string {
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
func mariadb_saveDifferencesToFile(filename string, differences []string) error {
	content := strings.Join(differences, "\n\n")
	return ioutil.WriteFile(filename, []byte(content), 0644)
}
