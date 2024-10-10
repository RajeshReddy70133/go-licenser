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

	"github.com/jedib0t/go-pretty/table"
	_ "github.com/mattn/go-sqlite3"
)

func sqlite(dbPath, folderPath, action, tag, prescript, postscript, diffscript string, logger *log.Logger, statefile string) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		log.Fatalf("Error connecting to SQLite database: %v", err)
	}
	defer db.Close()
	switch action {
	case "migrate":
		sqlitecallingfucn(dbPath, folderPath, tag, logger)
	case "clear":
		sqlitecleardata(dbPath, logger)
	case "check":
		sqlitecheckScripts1(dbPath, folderPath, logger)
	case "clearall":
		sqliteclearalltables(dbPath, logger)
	case "snapshot":
		sqliteGenerateMigrationScript(dbPath, folderPath, statefile, logger)
	case "delta":
		sqlite_processFiles(folderPath, prescript, postscript, diffscript)
	default:
		logger.Println("Unsupported action:", action)
	}
}
func sqlitecallingfucn(dbPath, folderPath string, tag string, logger *log.Logger) {
	db, err := sql.Open("sqlite3", dbPath+"?_timeout=30000") // Increase timeout
	if err != nil {
		logger.Fatalf("Error connecting to sqlite database: %v", err)
	}
	defer db.Close()

	// Set busy timeout
	_, err = db.Exec("PRAGMA busy_timeout = 30000")
	if err != nil {
		logger.Fatalf("Error setting busy timeout: %v", err)
	}

	createTableQuery := `
        CREATE TABLE IF NOT EXISTS tbl_version (
            Id INTEGER PRIMARY KEY,
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

	var scriptIDCounter int

	// Retrieve hash codes from the database table
	hashCodes := sqliteretrieveHashCodes(db)
	logger.Println("Connected to sqlite database")

	// Iterate over the files in the main folder
	var skippedFiles []string // Slice to store skipped file names
	tableWriter := table.NewWriter()
	tableWriter.SetOutputMirror(logger.Writer())
	tableWriter.AppendHeader(table.Row{"Id", "Major", "Minor", "Change", "Status", "Hash Code", "Execution Time"})

	// Reset the scriptID counter to 0 at the start of each run.
	scriptIDCounter = 0

	err = filepath.Walk(folderPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			logger.Printf("Error accessing file %s: %v", path, err)
			return nil
		}

		if info.IsDir() || !strings.HasSuffix(info.Name(), ".sql") {
			return nil
		}

		versions, fileName, hashKeys, scripts, tags := sqlitegetVersion(path)
		majorValue := strings.Split(filepath.Base(filepath.Dir(path)), ".")[0]

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

			if sqlitecontains(hashCodes, hk) && !strings.HasPrefix(fileName, "c") {
				status = "skipped"
				skippedFiles = append(skippedFiles, fileName)
				logger.Printf("execution skipped due to matching hash code: %s, change: %s\n", filepath.Base(fileName), version)
			} else {
				sqlScript := sqliteremoveComments(script)
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
					logger.Printf("execution success for script : %s, change: %s\n", filepath.Base(fileName), version)

				} else {
					status = "failed"
					logger.Printf("execution failed for script: %s, change: %s\n", filepath.Base(fileName), version)
				}
			}

			parts := strings.SplitN(fileName, "_", 2)
			minorValue := parts[0]

			scriptIDCounter++ // Increment the script ID counter.

			existingRecordID, err := sqlitecheckExistingRecord(db, majorValue, minorValue, version)
			if err != nil {
				logger.Fatalf("Error checking existing record: %v", err)
			}

			executionTime := time.Now().Format("2006-01-02 15:04:05")

			if status != "failed" {
				if existingRecordID != -1 {
					_, err = db.Exec(
						"UPDATE tbl_version SET Status=?, HashCode=?, ExecutionTime=? WHERE Id=?",
						status, hk, executionTime, scriptIDCounter)
				} else {
					_, err = db.Exec(
						"INSERT OR REPLACE INTO tbl_version (Id, Major, Minor, ChangeColumn, Status, HashCode, ExecutionTime) VALUES (?, ?, ?, ?, ?, ?, ?)",
						scriptIDCounter, majorValue, minorValue, version, status, hk, executionTime)
				}

				if err != nil {
					logger.Fatalf("Error updating/inserting record: %v", err)
				}
			}

			tableWriter.AppendRow([]interface{}{scriptIDCounter, majorValue, minorValue, version, status, hk, executionTime})
			hashCodes = append(hashCodes, hk)
		}
		return nil
	})
	if err != nil {
		logger.Fatalf("Error iterating over files: %v", err)
	}

	tableWriter.Render()
}

func sqliteretrieveHashCodes(db *sql.DB) []string {
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

func sqliteremoveComments(sqlScript string) string {
	lines := strings.Split(sqlScript, "\n")
	var cleanedLines []string
	for _, line := range lines {
		if !strings.HasPrefix(line, "--") {
			cleanedLines = append(cleanedLines, line)
		}
	}
	return strings.Join(cleanedLines, "\n")
}

func sqlitegetVersion(filePath string) ([]string, string, []string, []string, []string) {
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
			parts := strings.Split(strings.TrimSpace(strings.TrimPrefix(line, "--start version:")), ";")
			version := strings.TrimSpace(parts[0])
			tag := ""
			if len(parts) > 1 {
				tag = strings.TrimSpace(strings.Split(parts[1], "=")[1])
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

func sqlitecontains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

func sqlitecheckExistingRecord(db *sql.DB, majorValue, minorValue, version string) (int, error) {
	var id int
	err := db.QueryRow("SELECT Id FROM tbl_version WHERE Major=? AND Minor=? AND ChangeColumn=?", majorValue, minorValue, version).Scan(&id)
	if err != nil {
		if err == sql.ErrNoRows {
			return -1, nil // No record found
		}
		return -1, err // An actual error occurred
	}
	return id, nil
}

func sqlitecleardata(dbPath string, logger *log.Logger) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		log.Fatalf("Error connecting to sqlite database: %v", err)
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

func sqlitecheckScripts1(dbPath, folderPath string, logger *log.Logger) {
	// Establish a database connection
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		log.Fatalf("Error connecting to sqlite database: %v", err)
	}
	defer db.Close()

	// Retrieve executed scripts with their version tags and hash codes from the database
	executedScripts := executedScriptsWithHashes(db)

	// Parse scripts from the specified folder with their version tags and hash codes
	folderScripts := make(map[string]map[string]string) // Map of script names to maps of version to hash code
	err = filepath.Walk(folderPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			logger.Printf("Error accessing path %q: %v\n", path, err)
			return err
		}
		if !info.IsDir() && strings.HasSuffix(info.Name(), ".sql") {
			versions, fileName, hashCodes, _, _ := sqlitegetVersion(path)
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
	sqlite_printMissingScripts(executedScripts, folderScripts, logger)
}

func executedScriptsWithHashes(db *sql.DB) map[string]map[string]string {
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

func sqlite_printMissingScripts(executedScripts map[string]map[string]string, folderScripts map[string]map[string]string, logger *log.Logger) {
	for fileName, versions := range folderScripts {
		for version, hashCode := range versions {
			if executedVersion, exists := executedScripts[version]; !exists || executedVersion["HashCode"] != hashCode {
				logger.Printf("Change %s (version %s) with hash code %s has not been executed in tbl_version\n", fileName, version, hashCode)
			}
		}
	}
}

func findMissingScripts(executedScripts map[string]string, folderScripts map[string][]string) map[string][]string {
	missingScripts := make(map[string][]string)

	for folderName, scriptVersions := range folderScripts {
		for _, version := range scriptVersions {
			if _, exists := executedScripts[version]; !exists {
				if _, ok := missingScripts[folderName]; !ok {
					missingScripts[folderName] = []string{}
				}
				missingScripts[folderName] = append(missingScripts[folderName], version)
			}
		}
	}

	return missingScripts
}

func sqliteclearalltables(dbPath string, logger *log.Logger) {
	db, err := sql.Open("sqlite3", dbPath+"?_timeout=30000") // Increase timeout
	if err != nil {
		logger.Fatalf("Error connecting to sqlite database: %v", err)
	}
	defer db.Close()

	// Set busy timeout
	_, err = db.Exec("PRAGMA busy_timeout = 30000")
	if err != nil {
		logger.Fatalf("Error setting busy timeout: %v", err)
	}

	tables, err := db.Query("SELECT name FROM sqlite_master WHERE type='table'")
	if err != nil {
		logger.Fatalf("Error retrieving tables: %v", err)
	}
	defer tables.Close()

	var tableName string
	for tables.Next() {
		err := tables.Scan(&tableName)
		if err != nil {
			logger.Fatalf("Error scanning table name: %v", err)
		}

		if tableName != "sqlite_sequence" {
			_, err = db.Exec(fmt.Sprintf("DROP TABLE %s", tableName))
			if err != nil {
				logger.Printf("Error dropping table %s: %v", tableName, err)
			}
		}
	}

	logger.Println("All tables cleared successfully")
}

// Function to handle database connection and generate script
func sqliteGenerateMigrationScript(database, folderPath, statefile string, logger *log.Logger) {
	connString := fmt.Sprintf("file:%s?cache=shared&mode=rwc", database)

	db, err := sql.Open("sqlite3", connString)
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

	tableNames, err := sqliteGetTableNames(db)
	if err != nil {
		logger.Println("Error getting table names:", err)
		return
	}

	var combinedScript string
	for _, tableName := range tableNames {
		script, err := sqliteGenerateScriptForTable(db, tableName)
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

func sqliteGetTableNames(db *sql.DB) ([]string, error) {
	query := "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';"
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

func sqliteGenerateScriptForTable(db *sql.DB, tableName string) (string, error) {
	query := fmt.Sprintf("SELECT sql FROM sqlite_master WHERE type='table' AND name='%s';", tableName)
	row := db.QueryRow(query)

	var createTableSQL string
	if err := row.Scan(&createTableSQL); err != nil {
		return "", err
	}

	return createTableSQL + ";\n", nil
}

// processFiles handles the core logic of reading the files, finding differences, and saving the output.
func sqlite_processFiles(folderPath, prescript, postscript, diffscript string) error {
	// Construct file paths
	file1Path := filepath.Join(folderPath, prescript)
	file2Path := filepath.Join(folderPath, postscript)
	outputPath := filepath.Join(folderPath, diffscript)

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
	statementsFile1 := sqlite_splitSQLStatements(string(file1Content))
	statementsFile2 := sqlite_splitSQLStatements(string(file2Content))

	// Find differences
	differences := sqlite_findDifferences(statementsFile1, statementsFile2)
	fmt.Println("Differences have been saved to", diffscript)
	// Save the differences to a new SQL file
	return sqlite_saveDifferencesToFile(outputPath, differences)
}

// splitSQLStatements splits the SQL script into individual statements.
func sqlite_splitSQLStatements(content string) []string {
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
func sqlite_findDifferences(file1Statements, file2Statements []string) []string {
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
func sqlite_saveDifferencesToFile(filename string, differences []string) error {
	content := strings.Join(differences, "\n\n")
	return ioutil.WriteFile(filename, []byte(content), 0644)
}

func connectToDatabase(dbPath string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("error connecting to SQLite database: %w", err)
	}
	return db, nil
}

func executeStatements(db *sql.DB, statements []string) (bool, error) {
	success := true
	for _, statement := range statements {
		if strings.TrimSpace(statement) != "" {
			_, err := db.Exec(statement)
			if err != nil {
				return false, fmt.Errorf("error executing statement: %w", err)
			}
		}
	}
	return success, nil
}
