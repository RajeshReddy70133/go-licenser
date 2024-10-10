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

	_ "github.com/go-sql-driver/mysql" // MySQL driver
	"github.com/jedib0t/go-pretty/table"
)

func singlestore(host, port, database, username, password, folderPath, action, statefile, prescript, postscript, diffscript, tag string, logger *log.Logger) {
	switch action {
	case "migrate":
		executeMigrations(host, port, database, username, password, folderPath, tag, logger)
	case "clear":
		clearData(host, port, database, username, password, logger)
	case "clearall":
		clearAllData(host, port, database, username, password, logger)
	case "check":
		checkScripts(host, port, database, username, password, folderPath, logger)
	case "snapshot":
		memsqlGenerateMigrationScript(host, port, database, username, password, folderPath, statefile, logger)
	case "delta":
		single_processFiles(folderPath, prescript, postscript, diffscript)
	default:
		logger.Println("Unsupported action:", action)
	}
}

func executeMigrations(host, port, database, username, password, folderPath, tag string, logger *log.Logger) {
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", username, password, host, port, database))
	if err != nil {
		logger.Fatalf("Error connecting to SingleStore database: %v", err)
	}
	defer db.Close()

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

	hashCodes := retrieveHashCodes(db)
	logger.Println("Connected to SingleStore database")

	var skippedFiles []string
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

		versions, fileName, hashKeys, scripts, tags := parseSQLFile(path)
		majorValue := strings.Split(filepath.Base(filepath.Dir(path)), "_")[0]

		for i, version := range versions {
			hk := hashKeys[i]
			script := scripts[i]
			scriptTag := tags[i]
			status := ""

			if tag != "" {
				if scriptTag != tag {
					continue
				}
			} else if scriptTag != "" {
				continue
			}

			if contains(hashCodes, hk) && !strings.HasPrefix(fileName, "c") {
				status = "skipped"
				skippedFiles = append(skippedFiles, fileName)
				logger.Printf("Execution skipped due to matching hash code: %s, change: %s\n", filepath.Base(fileName), version)
			} else {
				success := executeSQLScript(db, script, logger, fileName, version)
				if success {
					status = "success"
				} else {
					status = "failed"
				}
			}

			parts := strings.SplitN(fileName, "_", 2)
			minorValue := parts[0]

			existingRecordID := checkExistingRecord(db, majorValue, minorValue, version)
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

			tableWriter.AppendRow([]interface{}{existingRecordID, majorValue, minorValue, version, status, hk, time.Now()})
			hashCodes = append(hashCodes, hk)
		}
		return nil
	})

	if err != nil {
		logger.Fatalf("Error iterating over files: %v", err)
	}

	tableWriter.Render()
}

func retrieveHashCodes(db *sql.DB) []string {
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

func executeSQLScript(db *sql.DB, script string, logger *log.Logger, fileName, version string) bool {
	sqlScript := removeComments(script)
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
		logger.Printf("Execution success for script: %s, change: %s\n", filepath.Base(fileName), version)
	} else {
		logger.Printf("Execution failed for script: %s, change: %s\n", filepath.Base(fileName), version)
	}

	return success
}

func removeComments(sqlScript string) string {
	lines := strings.Split(sqlScript, "\n")
	var cleanedLines []string
	for _, line := range lines {
		if !strings.HasPrefix(line, "--") {
			cleanedLines = append(cleanedLines, line)
		}
	}
	return strings.Join(cleanedLines, "\n")
}

func parseSQLFile(filePath string) ([]string, string, []string, []string, []string) {
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

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

func checkExistingRecord(db *sql.DB, majorValue, minorValue, version string) int {
	var id int
	err := db.QueryRow("SELECT Id FROM tbl_version WHERE Major=? AND Minor=? AND ChangeColumn=?", majorValue, minorValue, version).Scan(&id)
	if err != nil {
		return -1
	}
	return id
}

func clearData(host, port, database, username, password string, logger *log.Logger) {
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", username, password, host, port, database))
	if err != nil {
		logger.Fatalf("Error connecting to SingleStore database: %v", err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		logger.Fatal("Error pinging database: ", err)
	}

	tableName := "tbl_version"

	dropQuery := fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
	_, err = db.Exec(dropQuery)
	if err != nil {
		logger.Fatal("Error dropping table ", tableName, ": ", err)
	}

	logger.Println("Dropped table: ", tableName)
}

func clearAllData(host, port, database, username, password string, logger *log.Logger) {
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", username, password, host, port, database))
	if err != nil {
		logger.Fatalf("Error connecting to SingleStore database: %v", err)
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

func checkScripts(host, port, database, username, password, folderPath string, logger *log.Logger) {
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", username, password, host, port, database))
	if err != nil {
		logger.Fatalf("Error connecting to SingleStore database: %v", err)
	}
	defer db.Close()

	executedScripts := retrieveExecutedScriptsWithHashes(db)
	folderScripts := make(map[string]map[string]string)
	err = filepath.Walk(folderPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			logger.Printf("Error accessing path %q: %v\n", path, err)
			return err
		}
		if !info.IsDir() && strings.HasSuffix(info.Name(), ".sql") {
			versions, fileName, hashCodes, _, _ := parseSQLFile(path)
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

	printMissingScripts(executedScripts, folderScripts, logger)
}

func retrieveExecutedScriptsWithHashes(db *sql.DB) map[string]map[string]string {
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

func printMissingScripts(executedScripts map[string]map[string]string, folderScripts map[string]map[string]string, logger *log.Logger) {
	for fileName, versions := range folderScripts {
		for version, hashCode := range versions {
			if executedVersion, exists := executedScripts[version]; !exists || executedVersion["HashCode"] != hashCode {
				logger.Printf("Change %s (version %s) with hash code %s has not been executed in tbl_version\n", fileName, version, hashCode)
			}
		}
	}
}

// Function to get table names from the database
func memsqlGetTableNames(db *sql.DB) ([]string, error) {
	query := "SHOW TABLES"
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
func memsqlGenerateScriptForTable(db *sql.DB, tableName string) (string, error) {
	query := fmt.Sprintf("SHOW CREATE TABLE %s", tableName)
	rows, err := db.Query(query)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	var tableNameFromDB, tableCreateStatement string
	for rows.Next() {
		if err := rows.Scan(&tableNameFromDB, &tableCreateStatement); err != nil {
			return "", err
		}
	}

	return tableCreateStatement, nil
}

// Function to handle database connection and generate script
func memsqlGenerateMigrationScript(host, port, database, username, password, folderPath, statefile string, logger *log.Logger) {
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", username, password, host, port, database))
	if err != nil {
		logger.Fatalf("Error connecting to SingleStore database: %v", err)
	}
	defer db.Close()

	// Check if the database connection is established
	if err = db.Ping(); err != nil {
		logger.Println("Error connecting to the database:", err)
		return
	}

	tableNames, err := memsqlGetTableNames(db)
	if err != nil {
		logger.Println("Error getting table names:", err)
		return
	}

	var combinedScript string
	for _, tableName := range tableNames {
		script, err := memsqlGenerateScriptForTable(db, tableName)
		if err != nil {
			logger.Println("Error generating script for table:", err)
			return
		}
		combinedScript += script + ";\n\n"
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

// processFiles handles the core logic of reading the files, finding differences, and saving the output.
func single_processFiles(folderPath, prescript, postscript, diffscript string) error {
	file1Path := filepath.Join(folderPath, prescript)
	file2Path := filepath.Join(folderPath, postscript)
	outputPath := filepath.Join(folderPath, diffscript)

	// Check if files exist
	if _, err := os.Stat(file1Path); os.IsNotExist(err) {
		return fmt.Errorf("file does not exist: %s", file1Path)
	}
	if _, err := os.Stat(file2Path); os.IsNotExist(err) {
		return fmt.Errorf("file does not exist: %s", file2Path)
	}

	// Read the SQL files
	file1Content, err := ioutil.ReadFile(file1Path)
	if err != nil {
		return fmt.Errorf("error reading file1 (%s): %v", file1Path, err)
	}

	file2Content, err := ioutil.ReadFile(file2Path)
	if err != nil {
		return fmt.Errorf("error reading file2 (%s): %v", file2Path, err)
	}

	// Split the contents into individual statements
	statementsFile1 := singleSQLStatements(string(file1Content))
	statementsFile2 := singleSQLStatements(string(file2Content))

	// Find differences
	differences := findDifferencesInSingleStore(statementsFile1, statementsFile2)

	// Save the differences to a new SQL file
	err = saveSingleStoreDifferencesToFile(outputPath, differences)
	if err != nil {
		return fmt.Errorf("error saving differences to %s: %v", outputPath, err)
	}

	fmt.Println("Differences have been saved to", outputPath)

	// Return nil when no error occurs
	return nil
}

// singleSQLStatements splits the SQL script into individual statements.
func singleSQLStatements(content string) []string {
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

// findDifferencesInSingleStore returns the statements that are in file2 but not in file1.
func findDifferencesInSingleStore(file1Statements, file2Statements []string) []string {
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

// saveSingleStoreDifferencesToFile saves the differences to a file.
func saveSingleStoreDifferencesToFile(filename string, differences []string) error {
	content := strings.Join(differences, "\n\n")
	return os.WriteFile(filename, []byte(content), 0644)
}
