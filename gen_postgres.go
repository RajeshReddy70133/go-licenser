package main

import (
	"bufio"
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
	_ "github.com/lib/pq"
	"github.com/sergi/go-diff/diffmatchpatch"
)

func postgres(host, port, database, username, password, folderPath, action, statefile, prescript, postscript, difference, tag string, logger *log.Logger) {
	switch action {
	case "migrate":
		postgres_prosesing(host, port, database, username, password, folderPath, tag, logger)
	case "clear":
		postgresclearda(host, port, database, username, password, logger)
	case "clearall":
		postgres_clearall(host, port, database, username, password, logger)
	case "check":
		postgrescheckScripts(host, port, database, username, password, folderPath, logger)
	case "snapshot":
		postgres_generateMigrationScript(host, port, database, username, password, folderPath, statefile)
	case "delta":
		postgres_processSQLScripts(folderPath, prescript, postscript, difference)
	default:
		fmt.Println("Unsupported action:", action)
	}
}

func postgres_prosesing(host, port, database, username, password, folderPath, tag string, logger *log.Logger) {
	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", host, port, username, password, database)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		logger.Fatalf("Error connecting to PostgreSQL database: %v", err)
	}
	defer db.Close()

	// Create a table if it doesn't exist
	createTableQuery := `
		CREATE TABLE IF NOT EXISTS tbl_version (
			Id SERIAL PRIMARY KEY,
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
	hashCodes := postgresRetrieveHashCodes(db)
	fmt.Println("Connected to PostgreSQL database")

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

		versions, fileName, hashKeys, scripts, tags := postgresGetVersion(path)
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

			if postgresContains(hashCodes, hk) && !strings.HasPrefix(fileName, "c") {
				status = "skipped"
				skippedFiles = append(skippedFiles, fileName)
				logger.Printf("Execution skipped due to matching hash code: %s, change: %s\n", filepath.Base(fileName), version)
			} else {
				sqlScript := postgresRemoveComments(script)
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
				existingRecordID := postgresCheckExistingRecord(db, majorValue, minorValue, version)
				executionTime := time.Now().Format("2006-01-02 15:04:05")

				if status != "failed" {
					if existingRecordID != -1 {
						_, err = db.Exec(
							"UPDATE tbl_version SET Status=$1, HashCode=$2, ExecutionTime=$3 WHERE Id=$4",
							status, hk, executionTime, existingRecordID)
					} else {
						_, err = db.Exec(
							"INSERT INTO tbl_version (Major, Minor, Change, Status, HashCode, ExecutionTime) VALUES ($1, $2, $3, $4, $5, $6)",
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

func postgresRetrieveHashCodes(db *sql.DB) []string {
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

func postgresRemoveComments(sqlScript string) string {
	lines := strings.Split(sqlScript, "\n")
	var cleanedLines []string
	for _, line := range lines {
		if !strings.HasPrefix(line, "--") {
			cleanedLines = append(cleanedLines, line)
		}
	}
	return strings.Join(cleanedLines, "\n")
}

func postgresGetVersion(filePath string) ([]string, string, []string, []string, []string) {
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

func postgresContains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

func postgresCheckExistingRecord(db *sql.DB, majorValue, minorValue, version string) int {
	var id int
	err := db.QueryRow("SELECT Id FROM tbl_version WHERE Major=$1 AND Minor=$2 AND Change=$3", majorValue, minorValue, version).Scan(&id)
	if err != nil {
		return -1
	}
	return id
}

func postgresclearda(host, port, database, username, password string, logger *log.Logger) {
	// Establish a database connection
	db, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%s user=%s dbname=%s password=%s sslmode=disable", host, port, username, database, password))
	if err != nil {
		logger.Fatalf("Error connecting to PostgreSQL database: %v", err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Fatal("Error pinging database: ", err)
	}

	tableName := "tbl_version"

	// Drop the specified table
	dropQuery := fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", tableName)
	_, err = db.Exec(dropQuery)
	if err != nil {
		log.Fatal("Error dropping table ", tableName, ": ", err)
	}

	fmt.Println("Dropped table: ", tableName)
}

func postgres_clearall(host, port, database, username, password string, logger *log.Logger) {
	// Establish a database connection
	db, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%s user=%s dbname=%s password=%s sslmode=disable", host, port, username, database, password))
	if err != nil {
		logger.Fatalf("Error connecting to PostgreSQL database: %v", err)
	}
	defer db.Close()
	err = db.Ping()
	if err != nil {
		logger.Println("Error pinging database: ", err)
	}

	// Query to get all table names in the database
	query := `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = 'public' AND table_type = 'BASE TABLE';
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

		dropQuery := fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", tableName)
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

func postgrescheckScripts(host, port, database, username, password, folderPath string, logger *log.Logger) {
	// Establish a database connection
	db, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%s user=%s dbname=%s password=%s sslmode=disable", host, port, username, database, password))
	if err != nil {
		logger.Fatalf("Error connecting to PostgreSQL database: %v", err)
	}
	defer db.Close()

	// Retrieve executed scripts with their version tags and hash codes from the database
	executedScripts := postgresgetExecutedScriptsWithHashes(db)

	// Parse scripts from the specified folder with their version tags and hash codes
	folderScripts := make(map[string]map[string]string) // Map of script names to maps of version to hash code
	err = filepath.Walk(folderPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Printf("Error accessing path %q: %v\n", path, err)
			return err
		}
		if !info.IsDir() && strings.HasSuffix(info.Name(), ".sql") {
			versions, fileName, hashCodes := postgresgetVersionsAndHashes(path)
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
	findAndPrintMissingScripts(executedScripts, folderScripts, logger)
}

func postgresgetExecutedScriptsWithHashes(db *sql.DB) map[string]map[string]string {
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

func postgresgetVersionsAndHashes(filePath string) ([]string, string, []string) {
	content, err := ioutil.ReadFile(filePath)
	if err != nil {
		log.Fatalf("Error reading file %s: %v", filePath, err)
	}

	lines := strings.Split(string(content), "\n")
	var versions []string
	var hashCodes []string
	var script string

	for _, line := range lines {
		if strings.HasPrefix(line, "--start version:") {
			parts := strings.SplitN(strings.TrimSpace(strings.TrimPrefix(line, "--start version:")), ";tag=", 2)
			version := parts[0]
			versions = append(versions, version)
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

func findAndPrintMissingScripts(executedScripts map[string]map[string]string, folderScripts map[string]map[string]string, logger *log.Logger) {
	for fileName, versions := range folderScripts {
		for version, hashCode := range versions {
			if executedVersion, exists := executedScripts[version]; !exists || executedVersion["HashCode"] != hashCode {
				logger.Printf("Change %s (version %s) with hash code %s has not been executed in tbl_version\n", fileName, version, hashCode)
			}
		}
	}
}

// Function to get table names from the database
func postgres_getTableNames(db *sql.DB) ([]string, error) {
	query := "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE'"
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
func postgres_generateScriptForTable(db *sql.DB, tableName string) (string, error) {
	query := fmt.Sprintf("SELECT column_name, data_type, character_maximum_length FROM information_schema.columns WHERE table_name = '%s'", tableName)
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
		if maxLength.Valid && dataType == "character varying" {
			script += fmt.Sprintf("(%d)", maxLength.Int64)
		}
		script += ",\n"
	}
	script = script[:len(script)-2] + "\n);\n\n"
	return script, nil
}

// Function to handle database connection and generate script
func postgres_generateMigrationScript(host, port, database, username, password, folderPath, statefile string) {
	connString := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", host, port, username, password, database)

	db, err := sql.Open("postgres", connString)
	if err != nil {
		log.Fatal("Error creating connection pool: ", err)
	}
	defer db.Close()

	// Check if the database connection is established
	if err = db.Ping(); err != nil {
		log.Fatal("Error connecting to the database: ", err)
	}

	tableNames, err := postgres_getTableNames(db)
	if err != nil {
		log.Fatal("Error getting table names: ", err)
	}

	var combinedScript string
	for _, tableName := range tableNames {
		script, err := postgres_generateScriptForTable(db, tableName)
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

// Function to read the contents of a SQL script file
func postgres_readSQLScript(filePath string) ([]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return lines, nil
}

// Function to compare two SQL script files and extract differences
func postgres_compareSQLScripts(script1Lines, script2Lines []string) []string {
	dmp := diffmatchpatch.New()
	diffs := dmp.DiffMain(strings.Join(script1Lines, "\n"), strings.Join(script2Lines, "\n"), false)

	var diffLines []string
	for _, diff := range diffs {
		if diff.Type != diffmatchpatch.DiffEqual {
			for _, line := range strings.Split(diff.Text, "\n") {
				if line != "" {
					diffLines = append(diffLines, line)
				}
			}
		}
	}
	return diffLines
}

// Function to save differences to a new SQL script file
func postgres_saveDiffToSQLFile(diffLines []string, outputFilePath string) error {
	file, err := os.Create(outputFilePath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	for _, line := range diffLines {
		_, err := writer.WriteString(line + "\n")
		if err != nil {
			return err
		}
	}

	return writer.Flush()
}

// Function to process SQL scripts: read, compare, and save differences
func postgres_processSQLScripts(folderPath, prescript, postscript, diffFileName string) error {
	script1Path := filepath.Join(folderPath, prescript)
	script2Path := filepath.Join(folderPath, postscript)

	// Read the contents of the input SQL script files
	script1Lines, err := postgres_readSQLScript(script1Path)
	if err != nil {
		return fmt.Errorf("error reading script 1: %w", err)
	}

	script2Lines, err := postgres_readSQLScript(script2Path)
	if err != nil {
		return fmt.Errorf("error reading script 2: %w", err)
	}

	// Compare the scripts and extract differences
	diffOutput := postgres_compareSQLScripts(script1Lines, script2Lines)

	outputFilePath := filepath.Join(folderPath, diffFileName)

	// Save the differences to a new migration script file
	if err = postgres_saveDiffToSQLFile(diffOutput, outputFilePath); err != nil {
		return fmt.Errorf("error saving diff file: %w", err)
	}

	// Print a message indicating the successful save
	fmt.Printf("Differences have been saved to '%s'\n", outputFilePath)
	return nil
}
