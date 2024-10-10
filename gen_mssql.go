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

	_ "github.com/denisenkom/go-mssqldb" // Import the MSSQL driver package
	"github.com/jedib0t/go-pretty/table"
)

func mssql(host, port, database, username, password, folderPath, action, statefile, prescript, postscript, difference, tag string, logger *log.Logger) {
	switch action {
	case "snapshot":
		mssql_generateMigrationScript(host, port, database, username, password, folderPath, statefile)
	case "migrate":
		processMSSQL(host, port, database, username, password, folderPath, tag, logger)
	case "clear":
		mssqldelete(host, username, password, database, logger)
	case "clearall":
		deleteall(host, username, password, database, logger)
	case "check":
		mssqlcheckScripts1(host, database, username, password, folderPath, logger)
	case "delta":
		mssql_handleProcessing(folderPath, prescript, postscript, difference)
	default:
		logger.Println("Unsupported action:", action)
	}
}

func processMSSQL(host, port, database, username, password, folderPath, tag string, logger *log.Logger) {
	connStr := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%s;database=%s;", host, username, password, port, database)
	db, err := sql.Open("sqlserver", connStr)
	if err != nil {
		logger.Fatalf("Error connecting to MSSQL database: %v", err)
	}
	defer db.Close()

	createTableQuery := `
	IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='tbl_version' and xtype='U')
		CREATE TABLE tbl_version (
			ID INT IDENTITY(1,1) PRIMARY KEY,
			Major VARCHAR(10),
			Minor VARCHAR(10),
			Change VARCHAR(10),
			Status VARCHAR(10),
			HashCode VARCHAR(32),
			ExecutionTime DATETIME 
		)
	`
	_, err = db.Exec(createTableQuery)
	if err != nil {
		logger.Fatalf("Error creating table: %v", err)
	}

	var scriptIDCounter int
	hashCodes := retrieveHashCodesMSSQL(db)
	fmt.Println("Connected to MSSQL database")

	var skippedFiles []string
	tableWriter := table.NewWriter()
	tableWriter.SetOutputMirror(logger.Writer())
	tableWriter.AppendHeader(table.Row{"Id", "Major", "Minor", "Change", "Status", "Hash Code", "Execution Time"})
	scriptIDCounter = 0

	err = filepath.Walk(folderPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Printf("Error accessing file %s: %v", path, err)
			return nil
		}

		if info.IsDir() || !strings.HasSuffix(info.Name(), ".sql") {
			return nil
		}

		versions, fileName, hashKeys, scripts, tags := getVersionMSSQL(path)
		majorValue := strings.Split(filepath.Base(filepath.Dir(path)), "_")[0]

		for i, version := range versions {
			if tag == "" && tags[i] == "dev" {
				continue
			}

			if tag != "" && tags[i] != tag {
				continue
			}

			hk := hashKeys[i]
			script := scripts[i]
			status := ""

			if containsMSSQL(hashCodes, hk) && !strings.HasPrefix(fileName, "c") {
				status = "skipped"
				skippedFiles = append(skippedFiles, fileName)
				logger.Printf("Execution skipped due to matching hash code: %s, change: %s\n", filepath.Base(fileName), version)
			} else {
				sqlScript := removeCommentsMSSQL(script)
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

			scriptIDCounter++

			if (tag == "" && tags[i] != "dev") || (tag != "" && tags[i] == tag) {
				existingRecordID := checkExistingRecordMSSQL(db, majorValue, minorValue, version)
				executionTime := time.Now().Format("2006-01-02 15:04:05")

				if status != "failed" {
					if existingRecordID != -1 {
						_, err = db.Exec(
							"UPDATE tbl_version SET Status=@p1, HashCode=@p2, ExecutionTime=@p3 WHERE Id=@p4",
							status, hk, executionTime, existingRecordID)
					} else {
						_, err = db.Exec(
							"INSERT INTO tbl_version (Major, Minor, Change, Status, HashCode, ExecutionTime) VALUES (@p1, @p2, @p3, @p4, @p5, @p6)",
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

func retrieveHashCodesMSSQL(db *sql.DB) []string {
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

func removeCommentsMSSQL(sqlScript string) string {
	lines := strings.Split(sqlScript, "\n")
	var cleanedLines []string
	for _, line := range lines {
		if !strings.HasPrefix(line, "--") {
			cleanedLines = append(cleanedLines, line)
		}
	}
	return strings.Join(cleanedLines, "\n")
}

func getVersionMSSQL(filePath string) ([]string, string, []string, []string, []string) {
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

func containsMSSQL(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

func checkExistingRecordMSSQL(db *sql.DB, majorValue, minorValue, version string) int {
	var id int
	err := db.QueryRow("SELECT Id FROM tbl_version WHERE Major=@p1 AND Minor=@p2 AND Change=@p3", majorValue, minorValue, version).Scan(&id)
	if err != nil {
		return -1
	}
	return id
}

func mssqldelete(host string, username string, password string, database string, logger *log.Logger) {
	// Connection string
	connString := fmt.Sprintf("server=%s;user id=%s;password=%s;database=%s", host, username, password, database)
	// Create a connection to the database
	db, err := sql.Open("sqlserver", connString)
	if err != nil {
		logger.Fatal("Error connecting to database: ", err)
	}
	defer db.Close()

	// Attempt to ping the database
	if err = db.Ping(); err != nil {
		logger.Fatal("Error pinging database: ", err)
	}

	tableName := "tbl_version"

	// Drop the specified table
	dropQuery := fmt.Sprintf("DROP TABLE %s", tableName)
	_, err = db.Exec(dropQuery)
	if err != nil {
		logger.Fatal("Error dropping table ", tableName, ": ", err)
	}

	logger.Println("Dropped table: ", tableName)
}

func deleteall(host string, username string, password string, database string, logger *log.Logger) {
	// Connection string
	connString := fmt.Sprintf("server=%s;user id=%s;password=%s;database=%s", host, username, password, database)
	// Create a connection to the database
	conn, err := sql.Open("sqlserver", connString)
	if err != nil {
		logger.Fatal("Error connecting to database: ", err.Error())
	}
	defer conn.Close()

	err = conn.Ping()
	if err != nil {
		logger.Fatal("Error pinging database: ", err.Error())
	}

	// Query to get all table names in the database
	query := "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"

	rows, err := conn.Query(query)
	if err != nil {
		logger.Fatal("Error fetching table names: ", err.Error())
	}
	defer rows.Close()

	// Iterate through each table and drop it
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			logger.Fatal("Error scanning table name: ", err.Error())
		}

		dropQuery := fmt.Sprintf("DROP TABLE %s", tableName)
		_, err := conn.Exec(dropQuery)
		if err != nil {
			logger.Println("Error dropping table ", tableName, ": ", err.Error())
		} else {
			logger.Println("Dropped table: ", tableName)
		}
	}

	if err := rows.Err(); err != nil {
		logger.Fatal("Error iterating table rows: ", err.Error())
	}
}

func mssqlcheckScripts1(host, database, username, password, folderPath string, logger *log.Logger) {
	// Establish a database connection
	connString := fmt.Sprintf("server=%s;user id=%s;password=%s;database=%s", host, username, password, database)
	db, err := sql.Open("sqlserver", connString)
	if err != nil {
		logger.Fatalf("Error connecting to MSSQL database: %v", err)
	}
	defer db.Close()

	// Retrieve executed scripts with their version tags and hash codes from the database
	executedScripts := mssqlGetExecutedScriptsWithHashes(db)

	// Parse scripts from the specified folder with their version tags and hash codes
	folderScripts := make(map[string]map[string]string) // Map of script names to maps of version to hash code
	err = filepath.Walk(folderPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			logger.Printf("Error accessing path %q: %v\n", path, err)
			return err
		}
		if !info.IsDir() && strings.HasSuffix(info.Name(), ".sql") {
			versions, fileName, hashCodes := mssqlGetVersionsAndHashes(path)
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
	mssql_findAndPrintMissingScripts(executedScripts, folderScripts)
}

func mssqlGetExecutedScriptsWithHashes(db *sql.DB) map[string]map[string]string {
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

func mssqlGetVersionsAndHashes(filePath string) ([]string, string, []string) {
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

func mssql_findAndPrintMissingScripts(executedScripts map[string]map[string]string, folderScripts map[string]map[string]string) {
	for fileName, versions := range folderScripts {
		for version, hashCode := range versions {
			if executedVersion, exists := executedScripts[version]; !exists || executedVersion["HashCode"] != hashCode {
				fmt.Printf("Change %s (version %s) with hash code %s has not been executed in tbl_version\n", fileName, version, hashCode)
			}
		}
	}
}

func mssql_getTableNames(db *sql.DB) ([]string, error) {
	query := "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"
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
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return tableNames, nil
}

func mssql_generateScriptForTable(db *sql.DB, tableName string) (string, error) {
	query := fmt.Sprintf("SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s'", tableName)
	rows, err := db.Query(query)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	script := fmt.Sprintf("CREATE TABLE %s (\n", tableName)
	for rows.Next() {
		var columnName, dataType string
		var maxLength sql.NullInt64
		if err := rows.Scan(&columnName, &dataType, &maxLength); err != nil {
			return "", err
		}

		script += fmt.Sprintf("    %s %s", columnName, dataType)
		if maxLength.Valid {
			script += fmt.Sprintf("(%d)", maxLength.Int64)
		}
		script += ",\n"
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	script = script[:len(script)-2] + "\n);\n\n"
	return script, nil
}

func mssql_generateMigrationScript(host, port, database, username, password, folderPath, statefile string) {
	connString := fmt.Sprintf("server=%s;port=%s;user id=%s;password=%s;database=%s", host, port, username, password, database)

	db, err := sql.Open("mssql", connString)
	if err != nil {
		log.Fatal("Error creating connection pool: ", err)
	}
	defer db.Close()

	if err = db.Ping(); err != nil {
		log.Fatal("Error connecting to the database: ", err)
	}

	tableNames, err := mssql_getTableNames(db)
	if err != nil {
		log.Fatal("Error getting table names: ", err)
	}

	var combinedScript string
	for _, tableName := range tableNames {
		script, err := mssql_generateScriptForTable(db, tableName)
		if err != nil {
			log.Fatal("Error generating script for table: ", err)
		}
		combinedScript += script
	}

	combinedSQLFilePath := filepath.Join(folderPath, statefile)
	file, err := os.Create(combinedSQLFilePath)
	if err != nil {
		log.Fatal("Error creating file: ", err)
	}
	defer file.Close()

	_, err = file.WriteString(combinedScript)
	if err != nil {
		log.Fatal("Error writing to file: ", err)
	}

	fmt.Println("Script has been generated and saved to the specified file.")
}

// handleProcessing handles the processing of files and prints the result.
func mssql_handleProcessing(folderPath, prescript, postscript, difference string) {
	err := mssql_processFiles(folderPath, prescript, postscript, difference)
	if err != nil {
		log.Fatalf("Error processing files: %v", err)
	}

	fmt.Println("Differences have been saved to", difference)
}

// processFiles handles the core logic of reading the files, finding differences, and saving the output.
func mssql_processFiles(folderPath, prescript, postscript, difference string) error {
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
	statementsFile1 := mssql_splitSQLStatements(string(file1Content))
	statementsFile2 := mssql_splitSQLStatements(string(file2Content))

	// Find differences
	differences := mssql_findDifferences(statementsFile1, statementsFile2)

	// Save the differences to a new SQL file
	return mssql_saveDifferencesToFile(outputPath, differences)
}

// splitSQLStatements splits the SQL script into individual statements.
func mssql_splitSQLStatements(content string) []string {
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
func mssql_findDifferences(file1Statements, file2Statements []string) []string {
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
func mssql_saveDifferencesToFile(filename string, differences []string) error {
	content := strings.Join(differences, "\n\n")
	return ioutil.WriteFile(filename, []byte(content), 0644)
}
